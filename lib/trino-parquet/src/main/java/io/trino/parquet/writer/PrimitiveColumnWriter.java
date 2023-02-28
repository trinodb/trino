/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.writer.repdef.DefLevelWriterProvider;
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.parquet.writer.repdef.RepLevelIterable;
import io.trino.parquet.writer.repdef.RepLevelIterables;
import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.parquet.writer.ParquetCompressor.getCompressor;
import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.DefinitionLevelWriter;
import static io.trino.parquet.writer.repdef.DefLevelWriterProvider.getRootDefinitionLevelWriter;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PrimitiveColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(PrimitiveColumnWriter.class);

    private final ColumnDescriptor columnDescriptor;
    private final CompressionCodec compressionCodec;

    private final PrimitiveValueWriter primitiveValueWriter;
    private final ValuesWriter definitionLevelWriter;
    private final ValuesWriter repetitionLevelWriter;

    private final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    private boolean closed;
    private boolean getDataStreamsCalled;

    // current page stats
    private int valueCount;
    private int currentPageNullCounts;

    // column meta data stats
    private final Set<Encoding> encodings = new HashSet<>();
    private final Map<org.apache.parquet.format.Encoding, Integer> dataPagesWithEncoding = new HashMap<>();
    private final Map<org.apache.parquet.format.Encoding, Integer> dictionaryPagesWithEncoding = new HashMap<>();
    private long totalCompressedSize;
    private long totalUnCompressedSize;
    private long totalValues;
    private Statistics<?> columnStatistics;

    private final int maxDefinitionLevel;

    private final List<ParquetDataOutput> pageBuffer = new ArrayList<>();

    @Nullable
    private final ParquetCompressor compressor;

    private final int pageSizeThreshold;

    private long bufferedBytes;
    private long pageBufferedBytes;

    public PrimitiveColumnWriter(ColumnDescriptor columnDescriptor, PrimitiveValueWriter primitiveValueWriter, ValuesWriter definitionLevelWriter, ValuesWriter repetitionLevelWriter, CompressionCodec compressionCodec, int pageSizeThreshold)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
        this.maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();
        this.definitionLevelWriter = requireNonNull(definitionLevelWriter, "definitionLevelWriter is null");
        this.repetitionLevelWriter = requireNonNull(repetitionLevelWriter, "repetitionLevelWriter is null");
        this.primitiveValueWriter = requireNonNull(primitiveValueWriter, "primitiveValueWriter is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.compressor = getCompressor(compressionCodec);
        this.pageSizeThreshold = pageSizeThreshold;
        this.columnStatistics = Statistics.createStats(columnDescriptor.getPrimitiveType());
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        checkState(!closed);
        // write values
        primitiveValueWriter.write(columnChunk.getBlock());

        List<DefLevelWriterProvider> defLevelWriterProviders = ImmutableList.<DefLevelWriterProvider>builder()
                .addAll(columnChunk.getDefLevelWriterProviders())
                .add(DefLevelWriterProviders.of(columnChunk.getBlock(), maxDefinitionLevel))
                .build();
        DefinitionLevelWriter rootDefinitionLevelWriter = getRootDefinitionLevelWriter(defLevelWriterProviders, definitionLevelWriter);

        DefLevelWriterProvider.ValuesCount valuesCount = rootDefinitionLevelWriter.writeDefinitionLevels();
        currentPageNullCounts += valuesCount.totalValuesCount() - valuesCount.maxDefinitionLevelValuesCount();
        valueCount += valuesCount.totalValuesCount();

        if (columnDescriptor.getMaxRepetitionLevel() > 0) {
            // write repetition levels for nested types
            Iterator<Integer> repIterator = RepLevelIterables.getIterator(ImmutableList.<RepLevelIterable>builder()
                    .addAll(columnChunk.getRepLevelIterables())
                    .add(RepLevelIterables.of(columnChunk.getBlock()))
                    .build());
            while (repIterator.hasNext()) {
                int next = repIterator.next();
                repetitionLevelWriter.writeInteger(next);
            }
        }

        updateBufferedBytes();
        if (bufferedBytes >= pageSizeThreshold) {
            flushCurrentPageToBuffer();
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        checkState(closed);
        return ImmutableList.of(new BufferData(getDataStreams(), getColumnMetaData()));
    }

    // Returns ColumnMetaData that offset is invalid
    private ColumnMetaData getColumnMetaData()
    {
        checkState(getDataStreamsCalled);

        ColumnMetaData columnMetaData = new ColumnMetaData(
                ParquetTypeConverter.getType(columnDescriptor.getPrimitiveType().getPrimitiveTypeName()),
                encodings.stream().map(parquetMetadataConverter::getEncoding).collect(toImmutableList()),
                ImmutableList.copyOf(columnDescriptor.getPath()),
                compressionCodec,
                totalValues,
                totalUnCompressedSize,
                totalCompressedSize,
                -1);
        columnMetaData.setStatistics(ParquetMetadataConverter.toParquetStatistics(columnStatistics));
        ImmutableList.Builder<PageEncodingStats> pageEncodingStats = ImmutableList.builder();
        dataPagesWithEncoding.entrySet().stream()
                .map(encodingAndCount -> new PageEncodingStats(PageType.DATA_PAGE, encodingAndCount.getKey(), encodingAndCount.getValue()))
                .forEach(pageEncodingStats::add);
        dictionaryPagesWithEncoding.entrySet().stream()
                .map(encodingAndCount -> new PageEncodingStats(PageType.DICTIONARY_PAGE, encodingAndCount.getKey(), encodingAndCount.getValue()))
                .forEach(pageEncodingStats::add);
        columnMetaData.setEncoding_stats(pageEncodingStats.build());
        return columnMetaData;
    }

    // page header
    // repetition levels
    // definition levels
    // data
    private void flushCurrentPageToBuffer()
            throws IOException
    {
        byte[] pageDataBytes = BytesInput.concat(
                repetitionLevelWriter.getBytes(),
                definitionLevelWriter.getBytes(),
                primitiveValueWriter.getBytes())
                .toByteArray();
        long uncompressedSize = pageDataBytes.length;
        ParquetDataOutput pageData = (compressor != null)
                ? compressor.compress(pageDataBytes)
                : createDataOutput(Slices.wrappedBuffer(pageDataBytes));
        long compressedSize = pageData.size();

        Statistics<?> statistics = primitiveValueWriter.getStatistics();
        statistics.incrementNumNulls(currentPageNullCounts);
        columnStatistics.mergeStatistics(statistics);

        ByteArrayOutputStream pageHeaderOutputStream = new ByteArrayOutputStream();
        parquetMetadataConverter.writeDataPageV1Header(toIntExact(uncompressedSize),
                toIntExact(compressedSize),
                valueCount,
                repetitionLevelWriter.getEncoding(),
                definitionLevelWriter.getEncoding(),
                primitiveValueWriter.getEncoding(),
                pageHeaderOutputStream);
        ParquetDataOutput pageHeader = createDataOutput(BytesInput.from(pageHeaderOutputStream));

        dataPagesWithEncoding.merge(parquetMetadataConverter.getEncoding(primitiveValueWriter.getEncoding()), 1, Integer::sum);

        // update total stats
        totalUnCompressedSize += pageHeader.size() + uncompressedSize;
        long pageCompressedSize = pageHeader.size() + compressedSize;
        totalCompressedSize += pageCompressedSize;
        totalValues += valueCount;

        pageBuffer.add(pageHeader);
        pageBuffer.add(pageData);
        pageBufferedBytes += pageCompressedSize;

        // Add encoding should be called after ValuesWriter#getBytes() and before ValuesWriter#reset()
        encodings.add(repetitionLevelWriter.getEncoding());
        encodings.add(definitionLevelWriter.getEncoding());
        encodings.add(primitiveValueWriter.getEncoding());

        // reset page stats
        valueCount = 0;
        currentPageNullCounts = 0;

        repetitionLevelWriter.reset();
        definitionLevelWriter.reset();
        primitiveValueWriter.reset();
        updateBufferedBytes();
    }

    private List<ParquetDataOutput> getDataStreams()
            throws IOException
    {
        List<ParquetDataOutput> dictPage = new ArrayList<>();
        if (valueCount > 0) {
            flushCurrentPageToBuffer();
        }
        // write dict page if possible
        DictionaryPage dictionaryPage = primitiveValueWriter.toDictPageAndClose();
        if (dictionaryPage != null) {
            long uncompressedSize = dictionaryPage.getUncompressedSize();
            byte[] pageBytes = dictionaryPage.getBytes().toByteArray();
            ParquetDataOutput pageData = compressor != null
                    ? compressor.compress(pageBytes)
                    : createDataOutput(Slices.wrappedBuffer(pageBytes));
            long compressedSize = pageData.size();

            ByteArrayOutputStream dictStream = new ByteArrayOutputStream();
            parquetMetadataConverter.writeDictionaryPageHeader(
                    toIntExact(uncompressedSize),
                    toIntExact(compressedSize),
                    dictionaryPage.getDictionarySize(),
                    dictionaryPage.getEncoding(),
                    dictStream);
            ParquetDataOutput pageHeader = createDataOutput(BytesInput.from(dictStream));
            dictPage.add(pageHeader);
            dictPage.add(pageData);
            totalCompressedSize += pageHeader.size() + compressedSize;
            totalUnCompressedSize += pageHeader.size() + uncompressedSize;
            dictionaryPagesWithEncoding.merge(new ParquetMetadataConverter().getEncoding(dictionaryPage.getEncoding()), 1, Integer::sum);

            primitiveValueWriter.resetDictionary();
            updateBufferedBytes();
        }
        getDataStreamsCalled = true;

        return ImmutableList.<ParquetDataOutput>builder()
                .addAll(dictPage)
                .addAll(pageBuffer)
                .build();
    }

    @Override
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                primitiveValueWriter.getAllocatedSize() +
                definitionLevelWriter.getAllocatedSize() +
                repetitionLevelWriter.getAllocatedSize();
    }

    private void updateBufferedBytes()
    {
        bufferedBytes = pageBufferedBytes +
                definitionLevelWriter.getBufferedSize() +
                repetitionLevelWriter.getBufferedSize() +
                primitiveValueWriter.getBufferedSize();
    }
}
