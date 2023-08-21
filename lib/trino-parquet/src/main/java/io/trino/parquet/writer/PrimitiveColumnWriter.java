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
import io.trino.parquet.writer.repdef.RepLevelWriterProvider;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.trino.plugin.base.io.ChunkedSliceOutput;
import jakarta.annotation.Nullable;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.RepetitionLevelWriter;
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.getRootRepetitionLevelWriter;
import static java.util.Objects.requireNonNull;

public class PrimitiveColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(PrimitiveColumnWriter.class);
    private static final int MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE = 8 * 1024;
    private static final int MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE = 2 * 1024 * 1024;

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

    private final ChunkedSliceOutput compressedOutputStream;

    @Nullable
    private final ParquetCompressor compressor;

    private final int pageSizeThreshold;

    // Total size of compressed parquet pages and the current uncompressed page buffered in memory
    // Used by ParquetWriter to decide when a row group is big enough to flush
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
        this.compressedOutputStream = new ChunkedSliceOutput(MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE, MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE);
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
            List<RepLevelWriterProvider> repLevelWriterProviders = ImmutableList.<RepLevelWriterProvider>builder()
                    .addAll(columnChunk.getRepLevelWriterProviders())
                    .add(RepLevelWriterProviders.of(columnChunk.getBlock()))
                    .build();
            RepetitionLevelWriter rootRepetitionLevelWriter = getRootRepetitionLevelWriter(repLevelWriterProviders, repetitionLevelWriter);
            rootRepetitionLevelWriter.writeRepetitionLevels(0);
        }

        long currentPageBufferedBytes = getCurrentPageBufferedBytes();
        if (currentPageBufferedBytes >= pageSizeThreshold) {
            flushCurrentPageToBuffer();
        }
        else {
            updateBufferedBytes(currentPageBufferedBytes);
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
        int uncompressedSize = pageDataBytes.length;
        ParquetDataOutput pageData = (compressor != null)
                ? compressor.compress(pageDataBytes)
                : createDataOutput(Slices.wrappedBuffer(pageDataBytes));
        int compressedSize = pageData.size();

        Statistics<?> statistics = primitiveValueWriter.getStatistics();
        statistics.incrementNumNulls(currentPageNullCounts);
        columnStatistics.mergeStatistics(statistics);

        int writtenBytesSoFar = compressedOutputStream.size();
        parquetMetadataConverter.writeDataPageV1Header(uncompressedSize,
                compressedSize,
                valueCount,
                repetitionLevelWriter.getEncoding(),
                definitionLevelWriter.getEncoding(),
                primitiveValueWriter.getEncoding(),
                compressedOutputStream);
        int pageHeaderSize = compressedOutputStream.size() - writtenBytesSoFar;

        dataPagesWithEncoding.merge(parquetMetadataConverter.getEncoding(primitiveValueWriter.getEncoding()), 1, Integer::sum);

        // update total stats
        totalUnCompressedSize += pageHeaderSize + uncompressedSize;
        int pageCompressedSize = pageHeaderSize + compressedSize;
        totalCompressedSize += pageCompressedSize;
        totalValues += valueCount;

        pageData.writeData(compressedOutputStream);
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
        updateBufferedBytes(getCurrentPageBufferedBytes());
    }

    private List<ParquetDataOutput> getDataStreams()
            throws IOException
    {
        ImmutableList.Builder<ParquetDataOutput> outputs = ImmutableList.builder();
        if (valueCount > 0) {
            flushCurrentPageToBuffer();
        }
        // write dict page if possible
        DictionaryPage dictionaryPage = primitiveValueWriter.toDictPageAndClose();
        if (dictionaryPage != null) {
            int uncompressedSize = dictionaryPage.getUncompressedSize();
            byte[] pageBytes = dictionaryPage.getBytes().toByteArray();
            ParquetDataOutput pageData = compressor != null
                    ? compressor.compress(pageBytes)
                    : createDataOutput(Slices.wrappedBuffer(pageBytes));
            int compressedSize = pageData.size();

            ByteArrayOutputStream dictStream = new ByteArrayOutputStream();
            parquetMetadataConverter.writeDictionaryPageHeader(
                    uncompressedSize,
                    compressedSize,
                    dictionaryPage.getDictionarySize(),
                    dictionaryPage.getEncoding(),
                    dictStream);
            ParquetDataOutput pageHeader = createDataOutput(dictStream);
            outputs.add(pageHeader);
            outputs.add(pageData);
            totalCompressedSize += pageHeader.size() + compressedSize;
            totalUnCompressedSize += pageHeader.size() + uncompressedSize;
            dictionaryPagesWithEncoding.merge(new ParquetMetadataConverter().getEncoding(dictionaryPage.getEncoding()), 1, Integer::sum);

            primitiveValueWriter.resetDictionary();
        }
        getDataStreamsCalled = true;

        outputs.add(createDataOutput(compressedOutputStream));
        return outputs.build();
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
                compressedOutputStream.getRetainedSize() +
                primitiveValueWriter.getAllocatedSize() +
                definitionLevelWriter.getAllocatedSize() +
                repetitionLevelWriter.getAllocatedSize();
    }

    private void updateBufferedBytes(long currentPageBufferedBytes)
    {
        bufferedBytes = pageBufferedBytes + currentPageBufferedBytes;
    }

    private long getCurrentPageBufferedBytes()
    {
        return definitionLevelWriter.getBufferedSize() +
                repetitionLevelWriter.getBufferedSize() +
                primitiveValueWriter.getBufferedSize();
    }
}
