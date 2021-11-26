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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.transforms.Transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_TOO_MANY_OPEN_PARTITIONS;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private static final int MAX_PAGE_POSITIONS = 4096;

    private final int maxOpenWriters;
    private final Schema outputSchema;
    private final PartitionSpec partitionSpec;
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JobConf jobConf;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final IcebergFileFormat fileFormat;
    private final MetricsConfig metricsConfig;
    private final PagePartitioner pagePartitioner;
    private final long targetMaxFileSize;

    private final List<WriteContext> writers = new ArrayList<>();
    private final List<WriteContext> closedWriters = new ArrayList<>();
    private final Collection<Slice> commitTasks = new ArrayList<>();

    private long writtenBytes;
    private long memoryUsage;
    private long validationCpuNanos;

    public IcebergPageSink(
            Schema outputSchema,
            PartitionSpec partitionSpec,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            List<IcebergColumnHandle> inputColumns,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            int maxOpenWriters)
    {
        requireNonNull(inputColumns, "inputColumns is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.jobConf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, new Path(locationProvider.newDataLocation("data-file"))));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.metricsConfig = MetricsConfig.fromProperties(requireNonNull(storageProperties, "storageProperties is null"));
        this.maxOpenWriters = maxOpenWriters;
        this.pagePartitioner = new PagePartitioner(pageIndexerFactory, toPartitionColumns(inputColumns, partitionSpec));
        this.targetMaxFileSize = IcebergSessionProperties.getTargetMaxFileSize(session);
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        hdfsEnvironment.doAs(session.getIdentity(), () -> doAppend(page));

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        for (WriteContext context : writers) {
            closeWriter(context);
        }

        writtenBytes = closedWriters.stream()
                .mapToLong(writer -> writer.getWriter().getWrittenBytes())
                .sum();
        validationCpuNanos = closedWriters.stream()
                .mapToLong(writer -> writer.getWriter().getValidationCpuNanos())
                .sum();

        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        RuntimeException error = null;
        for (WriteContext context : Iterables.concat(writers, closedWriters)) {
            try {
                if (context != null) {
                    context.getWriter().rollback();
                }
            }
            catch (Throwable t) {
                if (error == null) {
                    error = new RuntimeException("Exception during rollback");
                }
                error.addSuppressed(t);
            }
        }
        if (error != null) {
            throw error;
        }
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // position count for each writer
        int[] sizes = new int[writers.size()];
        for (int index : writerIndexes) {
            sizes[index]++;
        }

        // record which positions are used by which writer
        int[][] writerPositions = new int[writers.size()][];
        int[] counts = new int[writers.size()];

        for (int position = 0; position < page.getPositionCount(); position++) {
            int index = writerIndexes[position];

            int count = counts[index];
            if (count == 0) {
                writerPositions[index] = new int[sizes[index]];
            }
            writerPositions[index][count] = position;
            counts[index]++;
        }

        // invoke the writers
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // if write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = page;
            if (positions.length != page.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            IcebergFileWriter writer = writers.get(index).getWriter();

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getMemoryUsage();

            writer.appendRows(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            memoryUsage += (writer.getMemoryUsage() - currentMemory);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        int[] writerIndexes = pagePartitioner.partitionPage(page);

        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new TrinoException(ICEBERG_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            WriteContext writer = writers.get(writerIndex);
            if (writer != null) {
                if (writer.getWrittenBytes() <= targetMaxFileSize) {
                    continue;
                }
                closeWriter(writer);
            }

            Optional<PartitionData> partitionData = getPartitionData(pagePartitioner.getColumns(), page, position);
            writer = createWriter(partitionData);

            writers.set(writerIndex, writer);
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private void closeWriter(WriteContext writeContext)
    {
        long currentWritten = writeContext.getWriter().getWrittenBytes();
        long currentMemory = writeContext.getWriter().getMemoryUsage();
        writeContext.getWriter().commit();
        writtenBytes += (writeContext.getWriter().getWrittenBytes() - currentWritten);
        memoryUsage += (writeContext.getWriter().getMemoryUsage() - currentMemory);

        CommitTaskData task = new CommitTaskData(
                writeContext.getPath().toString(),
                writeContext.getWriter().getWrittenBytes(),
                new MetricsWrapper(writeContext.getWriter().getMetrics()),
                writeContext.getPartitionData().map(PartitionData::toJson),
                FileContent.DATA);

        commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));

        closedWriters.add(writeContext);
    }

    private WriteContext createWriter(Optional<PartitionData> partitionData)
    {
        // prepend query id to a file name so we can determine which files were written by which query. This is needed for opportunistic cleanup of extra files
        // which may be present for successfully completing query in presence of failure recovery mechanisms.
        String fileName = fileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
        Path outputPath = partitionData.map(partition -> new Path(locationProvider.newDataLocation(partitionSpec, partition, fileName)))
                .orElse(new Path(locationProvider.newDataLocation(fileName)));

        IcebergFileWriter writer = fileWriterFactory.createFileWriter(
                outputPath,
                outputSchema,
                jobConf,
                session,
                hdfsContext,
                fileFormat,
                metricsConfig,
                FileContent.DATA);

        return new WriteContext(writer, outputPath, partitionData);
    }

    private static Optional<PartitionData> getPartitionData(List<PartitionColumn> columns, Page page, int position)
    {
        if (columns.isEmpty()) {
            return Optional.empty();
        }

        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            PartitionColumn column = columns.get(i);
            Block block = page.getBlock(column.getSourceChannel());
            Type type = column.getSourceType();
            Object value = getIcebergValue(block, position, type);
            values[i] = applyTransform(column.getField().transform(), value);
        }
        return Optional.of(new PartitionData(values));
    }

    @SuppressWarnings("unchecked")
    private static Object applyTransform(Transform<?, ?> transform, Object value)
    {
        return ((Transform<Object, Object>) transform).apply(value);
    }

    public static Object getIcebergValue(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof BigintType) {
            return type.getLong(block, position);
        }
        if (type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType || type instanceof DateType) {
            return toIntExact(type.getLong(block, position));
        }
        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }
        if (type instanceof DecimalType) {
            return readBigDecimal((DecimalType) type, block, position);
        }
        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        if (type instanceof DoubleType) {
            return type.getDouble(block, position);
        }
        if (type.equals(TIME_MICROS)) {
            return type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return type.getLong(block, position);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros(getTimestampTz(block, position));
        }
        if (type instanceof VarbinaryType) {
            return type.getSlice(block, position).getBytes();
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof UuidType) {
            return trinoUuidToJavaUuid(type.getSlice(block, position));
        }
        throw new UnsupportedOperationException("Type not supported as partition column: " + type.getDisplayName());
    }

    private static List<PartitionColumn> toPartitionColumns(List<IcebergColumnHandle> handles, PartitionSpec partitionSpec)
    {
        Map<Integer, Integer> idChannels = new HashMap<>();
        for (int i = 0; i < handles.size(); i++) {
            idChannels.put(handles.get(i).getId(), i);
        }

        return partitionSpec.fields().stream()
                .map(field -> {
                    Integer channel = idChannels.get(field.sourceId());
                    checkArgument(channel != null, "partition field not found: %s", field);
                    Type inputType = handles.get(channel).getType();
                    ColumnTransform transform = getColumnTransform(field, inputType);
                    return new PartitionColumn(field, channel, inputType, transform.getType(), transform.getBlockTransform());
                })
                .collect(toImmutableList());
    }

    private static class WriteContext
    {
        private final IcebergFileWriter writer;
        private final Path path;
        private final Optional<PartitionData> partitionData;

        public WriteContext(IcebergFileWriter writer, Path path, Optional<PartitionData> partitionData)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.path = requireNonNull(path, "path is null");
            this.partitionData = requireNonNull(partitionData, "partitionData is null");
        }

        public IcebergFileWriter getWriter()
        {
            return writer;
        }

        public Path getPath()
        {
            return path;
        }

        public Optional<PartitionData> getPartitionData()
        {
            return partitionData;
        }

        public long getWrittenBytes()
        {
            return writer.getWrittenBytes();
        }
    }

    private static class PagePartitioner
    {
        private final PageIndexer pageIndexer;
        private final List<PartitionColumn> columns;

        public PagePartitioner(PageIndexerFactory pageIndexerFactory, List<PartitionColumn> columns)
        {
            this.pageIndexer = pageIndexerFactory.createPageIndexer(columns.stream()
                    .map(PartitionColumn::getResultType)
                    .collect(toImmutableList()));
            this.columns = ImmutableList.copyOf(columns);
        }

        public int[] partitionPage(Page page)
        {
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                PartitionColumn column = columns.get(i);
                Block block = page.getBlock(column.getSourceChannel());
                blocks[i] = column.getBlockTransform().apply(block);
            }
            Page transformed = new Page(page.getPositionCount(), blocks);

            return pageIndexer.indexPage(transformed);
        }

        public int getMaxIndex()
        {
            return pageIndexer.getMaxIndex();
        }

        public List<PartitionColumn> getColumns()
        {
            return columns;
        }
    }

    private static class PartitionColumn
    {
        private final PartitionField field;
        private final int sourceChannel;
        private final Type sourceType;
        private final Type resultType;
        private final Function<Block, Block> blockTransform;

        public PartitionColumn(PartitionField field, int sourceChannel, Type sourceType, Type resultType, Function<Block, Block> blockTransform)
        {
            this.field = requireNonNull(field, "field is null");
            this.sourceChannel = sourceChannel;
            this.sourceType = requireNonNull(sourceType, "sourceType is null");
            this.resultType = requireNonNull(resultType, "resultType is null");
            this.blockTransform = requireNonNull(blockTransform, "blockTransform is null");
        }

        public PartitionField getField()
        {
            return field;
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }

        public Type getSourceType()
        {
            return sourceType;
        }

        public Type getResultType()
        {
            return resultType;
        }

        public Function<Block, Block> getBlockTransform()
        {
            return blockTransform;
        }
    }
}
