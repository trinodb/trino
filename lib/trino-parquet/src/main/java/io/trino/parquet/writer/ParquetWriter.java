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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.writer.ColumnWriter.BufferData;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;

public class ParquetWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ParquetWriter.class).instanceSize();

    private static final int CHUNK_MAX_BYTES = toIntExact(DataSize.of(128, MEGABYTE).toBytes());

    private final OutputStreamSliceOutput outputStream;
    private final ParquetWriterOptions writerOption;
    private final MessageType messageType;
    private final String createdBy;
    private final int chunkMaxLogicalBytes;
    private final Map<List<String>, Type> primitiveTypes;
    private final CompressionCodecName compressionCodecName;

    private final ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

    private List<ColumnWriter> columnWriters;
    private int rows;
    private long bufferedBytes;
    private boolean closed;
    private boolean writeHeader;

    public static final Slice MAGIC = wrappedBuffer("PAR1".getBytes(US_ASCII));

    public ParquetWriter(
            OutputStream outputStream,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions writerOption,
            CompressionCodecName compressionCodecName,
            String trinoVersion)
    {
        this.outputStream = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputstream is null"));
        this.messageType = requireNonNull(messageType, "messageType is null");
        this.primitiveTypes = requireNonNull(primitiveTypes, "primitiveTypes is null");
        this.writerOption = requireNonNull(writerOption, "writerOption is null");
        this.compressionCodecName = requireNonNull(compressionCodecName, "compressionCodecName is null");
        initColumnWriters();
        this.chunkMaxLogicalBytes = max(1, CHUNK_MAX_BYTES / 2);
        this.createdBy = formatCreatedBy(requireNonNull(trinoVersion, "trinoVersion is null"));
    }

    public long getWrittenBytes()
    {
        return outputStream.longSize();
    }

    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                outputStream.getRetainedSize() +
                columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum();
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        checkState(!closed, "writer is closed");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        while (page != null) {
            int chunkRows = min(page.getPositionCount(), writerOption.getBatchSize());
            Page chunk = page.getRegion(0, chunkRows);

            // avoid chunk with huge logical size
            while (chunkRows > 1 && chunk.getLogicalSizeInBytes() > chunkMaxLogicalBytes) {
                chunkRows /= 2;
                chunk = chunk.getRegion(0, chunkRows);
            }

            // Remove chunk from current page
            if (chunkRows < page.getPositionCount()) {
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                page = null;
            }

            writeChunk(chunk);
        }
    }

    private void writeChunk(Page page)
            throws IOException
    {
        bufferedBytes = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(new ColumnChunk(page.getBlock(channel)));
            bufferedBytes += writer.getBufferedBytes();
        }
        rows += page.getPositionCount();

        if (bufferedBytes >= writerOption.getMaxRowGroupSize()) {
            columnWriters.forEach(ColumnWriter::close);
            flush();
            initColumnWriters();
            rows = 0;
            bufferedBytes = columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        try (outputStream) {
            columnWriters.forEach(ColumnWriter::close);
            flush();
            writeFooter();
        }
    }

    // Parquet File Layout:
    //
    // MAGIC
    // variable: Data
    // variable: Metadata
    // 4 bytes: MetadataLength
    // MAGIC
    private void flush()
            throws IOException
    {
        // write header
        if (!writeHeader) {
            createDataOutput(MAGIC).writeData(outputStream);
            writeHeader = true;
        }

        // get all data in buffer
        ImmutableList.Builder<BufferData> builder = ImmutableList.builder();
        for (ColumnWriter columnWriter : columnWriters) {
            columnWriter.getBuffer().forEach(builder::add);
        }
        List<BufferData> bufferDataList = builder.build();

        // update stats
        long stripeStartOffset = outputStream.longSize();
        List<ColumnMetaData> metadatas = bufferDataList.stream()
                .map(BufferData::getMetaData)
                .collect(toImmutableList());
        updateRowGroups(updateColumnMetadataOffset(metadatas, stripeStartOffset));

        // flush pages
        bufferDataList.stream()
                .map(BufferData::getData)
                .flatMap(List::stream)
                .forEach(data -> data.writeData(outputStream));
    }

    private void writeFooter()
            throws IOException
    {
        checkState(closed);
        Slice footer = getFooter(rowGroupBuilder.build(), messageType);
        createDataOutput(footer).writeData(outputStream);

        Slice footerSize = Slices.allocate(SIZE_OF_INT);
        footerSize.setInt(0, footer.length());
        createDataOutput(footerSize).writeData(outputStream);

        createDataOutput(MAGIC).writeData(outputStream);
    }

    Slice getFooter(List<RowGroup> rowGroups, MessageType messageType)
            throws IOException
    {
        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.setVersion(1);
        fileMetaData.setCreated_by(createdBy);
        fileMetaData.setSchema(MessageTypeConverter.toParquetSchema(messageType));
        long totalRows = rowGroups.stream().mapToLong(RowGroup::getNum_rows).sum();
        fileMetaData.setNum_rows(totalRows);
        fileMetaData.setRow_groups(ImmutableList.copyOf(rowGroups));

        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(40);
        Util.writeFileMetaData(fileMetaData, dynamicSliceOutput);
        return dynamicSliceOutput.slice();
    }

    private void updateRowGroups(List<ColumnMetaData> columnMetaData)
    {
        // TODO Avoid writing empty row group
        long totalBytes = columnMetaData.stream().mapToLong(ColumnMetaData::getTotal_compressed_size).sum();
        ImmutableList<org.apache.parquet.format.ColumnChunk> columnChunks = columnMetaData.stream().map(ParquetWriter::toColumnChunk).collect(toImmutableList());
        rowGroupBuilder.add(new RowGroup(columnChunks, totalBytes, rows));
    }

    private static org.apache.parquet.format.ColumnChunk toColumnChunk(ColumnMetaData metaData)
    {
        // TODO Not sure whether file_offset is used
        org.apache.parquet.format.ColumnChunk columnChunk = new org.apache.parquet.format.ColumnChunk(0);
        columnChunk.setMeta_data(metaData);
        return columnChunk;
    }

    private List<ColumnMetaData> updateColumnMetadataOffset(List<ColumnMetaData> columns, long offset)
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        long currentOffset = offset;
        for (ColumnMetaData column : columns) {
            ColumnMetaData columnMetaData = new ColumnMetaData(column.type, column.encodings, column.path_in_schema, column.codec, column.num_values, column.total_uncompressed_size, column.total_compressed_size, currentOffset);
            columnMetaData.setStatistics(column.getStatistics());
            columnMetaData.setEncoding_stats(column.getEncoding_stats());
            builder.add(columnMetaData);
            currentOffset += column.getTotal_compressed_size();
        }
        return builder.build();
    }

    @VisibleForTesting
    static String formatCreatedBy(String trinoVersion)
    {
        // Add "(build n/a)" suffix to satisfy Parquet's VersionParser expectations
        return "Trino version " + trinoVersion + " (build n/a)";
    }

    private void initColumnWriters()
    {
        ParquetProperties parquetProperties = ParquetProperties.builder()
                .withWriterVersion(PARQUET_1_0)
                .withPageSize(writerOption.getMaxPageSize())
                .build();

        this.columnWriters = ParquetWriters.getColumnWriters(messageType, primitiveTypes, parquetProperties, compressionCodecName);
    }
}
