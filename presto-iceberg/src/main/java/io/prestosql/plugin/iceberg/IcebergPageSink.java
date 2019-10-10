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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveFileWriter;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageIndexer;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.transforms.Transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.prestosql.plugin.hive.util.ParquetRecordWriterUtil.setParquetSchema;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_TOO_MANY_OPEN_PARTITIONS;
import static io.prestosql.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.Decimals.readBigDecimal;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private static final int MAX_PAGE_POSITIONS = 4096;

    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeStatic"})
    private final int maxOpenWriters = 100;  // TODO: make this configurable
    private final Schema outputSchema;
    private final PartitionSpec partitionSpec;
    private final String outputPath;
    private final HdfsEnvironment hdfsEnvironment;
    private final JobConf jobConf;
    private final List<HiveColumnHandle> inputColumns;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final TypeManager typeManager;
    private final FileFormat fileFormat;
    private final PagePartitioner pagePartitioner;

    private final List<WriteContext> writers = new ArrayList<>();

    private long writtenBytes;
    private long systemMemoryUsage;
    private long validationCpuNanos;

    public IcebergPageSink(
            Schema outputSchema,
            PartitionSpec partitionSpec,
            String outputPath,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            List<HiveColumnHandle> inputColumns,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            FileFormat fileFormat)
    {
        requireNonNull(inputColumns, "inputColumns is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        requireNonNull(hdfsContext, "hdfsContext is null");
        this.jobConf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, new Path(outputPath)));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.inputColumns = ImmutableList.copyOf(inputColumns);
        this.pagePartitioner = new PagePartitioner(pageIndexerFactory, toPartitionColumns(typeManager, inputColumns, partitionSpec));
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page));

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();

        for (WriteContext context : writers) {
            context.getWriter().commit();

            CommitTaskData task = new CommitTaskData(
                    context.getPath().toString(),
                    new MetricsWrapper(readMetrics(context.getPath())),
                    context.getPartitionData().map(PartitionData::toJson));

            commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
        }

        writtenBytes = writers.stream()
                .mapToLong(writer -> writer.getWriter().getWrittenBytes())
                .sum();
        validationCpuNanos = writers.stream()
                .mapToLong(writer -> writer.getWriter().getValidationCpuNanos())
                .sum();

        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        RuntimeException error = null;
        for (WriteContext context : writers) {
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

            HiveFileWriter writer = writers.get(index).getWriter();

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getSystemMemoryUsage();

            writer.appendRows(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            systemMemoryUsage += (writer.getSystemMemoryUsage() - currentMemory);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        int[] writerIndexes = pagePartitioner.partitionPage(page);

        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new PrestoException(ICEBERG_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            if (writers.get(writerIndex) != null) {
                continue;
            }

            Optional<PartitionData> partitionData = getPartitionData(pagePartitioner.getColumns(), page, position);
            Optional<String> partitionPath = partitionData.map(partitionSpec::partitionToPath);

            WriteContext writer = createWriter(partitionPath, partitionData);

            writers.set(writerIndex, writer);
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private WriteContext createWriter(Optional<String> partitionPath, Optional<PartitionData> partitionData)
    {
        Path outputPath = new Path(this.outputPath);
        if (partitionPath.isPresent()) {
            outputPath = new Path(outputPath, partitionPath.get());
        }
        outputPath = new Path(outputPath, randomUUID().toString());
        outputPath = new Path(fileFormat.addExtension(outputPath.toString()));

        HiveFileWriter writer = createWriter(outputPath);

        return new WriteContext(writer, outputPath, partitionData);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private HiveFileWriter createWriter(Path outputPath)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetWriter(outputPath);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private HiveFileWriter createParquetWriter(Path outputPath)
    {
        Properties properties = new Properties();
        properties.setProperty(IOConstants.COLUMNS, inputColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(joining(",")));
        properties.setProperty(IOConstants.COLUMNS_TYPES, inputColumns.stream()
                .map(column -> column.getHiveType().getHiveTypeName().toString())
                .collect(joining(":")));

        setParquetSchema(jobConf, convert(outputSchema, "table"));

        return new RecordFileWriter(
                outputPath,
                inputColumns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toImmutableList()),
                fromHiveStorageFormat(HiveStorageFormat.PARQUET),
                properties,
                HiveStorageFormat.PARQUET.getEstimatedWriterSystemMemoryUsage(),
                jobConf,
                typeManager,
                session);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private Metrics readMetrics(Path path)
    {
        switch (fileFormat) {
            case PARQUET:
                return ParquetUtil.fileMetrics(HadoopInputFile.fromPath(path, jobConf), MetricsConfig.getDefault());
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
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
        if (type instanceof TimestampType) {
            return MILLISECONDS.toMicros(type.getLong(block, position));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return MILLISECONDS.toMicros(unpackMillisUtc(type.getLong(block, position)));
        }
        if (type instanceof VarbinaryType) {
            return type.getSlice(block, position).getBytes();
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        throw new UnsupportedOperationException("Type not supported as partition column: " + type.getDisplayName());
    }

    private static List<PartitionColumn> toPartitionColumns(TypeManager typeManager, List<HiveColumnHandle> handles, PartitionSpec partitionSpec)
    {
        Map<String, Integer> nameChannels = new HashMap<>();
        for (int i = 0; i < handles.size(); i++) {
            nameChannels.put(handles.get(i).getName(), i);
        }

        return partitionSpec.fields().stream()
                .map(field -> {
                    String name = partitionSpec.schema().findColumnName(field.sourceId());
                    Integer channel = nameChannels.get(name);
                    checkArgument(channel != null, "partition field not found: %s", field);
                    Type inputType = typeManager.getType(handles.get(channel).getTypeSignature());
                    ColumnTransform transform = getColumnTransform(field, inputType);
                    return new PartitionColumn(field, channel, inputType, transform.getType(), transform.getTransform());
                })
                .collect(toImmutableList());
    }

    private static class WriteContext
    {
        private final HiveFileWriter writer;
        private final Path path;
        private final Optional<PartitionData> partitionData;

        public WriteContext(HiveFileWriter writer, Path path, Optional<PartitionData> partitionData)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.path = requireNonNull(path, "path is null");
            this.partitionData = requireNonNull(partitionData, "partitionData is null");
        }

        public HiveFileWriter getWriter()
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
