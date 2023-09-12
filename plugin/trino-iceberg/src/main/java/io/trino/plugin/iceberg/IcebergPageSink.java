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
import com.google.common.collect.Streams;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_TOO_MANY_OPEN_PARTITIONS;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isSortedWritingEnabled;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.FileContent.DATA;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private static final int MAX_PAGE_POSITIONS = 4096;

    private final int maxOpenWriters;
    private final Schema outputSchema;
    private final PartitionSpec partitionSpec;
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final IcebergFileFormat fileFormat;
    private final MetricsConfig metricsConfig;
    private final PagePartitioner pagePartitioner;
    private final long targetMaxFileSize;
    private final Map<String, String> storageProperties;
    private final List<TrinoSortField> sortOrder;
    private final boolean sortedWritingEnabled;
    private final DataSize sortingFileWriterBufferSize;
    private final Integer sortingFileWriterMaxOpenFiles;
    private final Location tempDirectory;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final List<Type> columnTypes;
    private final List<Integer> sortColumnIndexes;
    private final List<SortOrder> sortOrders;

    private final List<WriteContext> writers = new ArrayList<>();
    private final List<Closeable> closedWriterRollbackActions = new ArrayList<>();
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
            TrinoFileSystem fileSystem,
            List<IcebergColumnHandle> inputColumns,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            int maxOpenWriters,
            List<TrinoSortField> sortOrder,
            DataSize sortingFileWriterBufferSize,
            int sortingFileWriterMaxOpenFiles,
            TypeManager typeManager,
            PageSorter pageSorter)
    {
        requireNonNull(inputColumns, "inputColumns is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.metricsConfig = MetricsConfig.fromProperties(requireNonNull(storageProperties, "storageProperties is null"));
        this.maxOpenWriters = maxOpenWriters;
        this.pagePartitioner = new PagePartitioner(pageIndexerFactory, toPartitionColumns(inputColumns, partitionSpec));
        this.targetMaxFileSize = IcebergSessionProperties.getTargetMaxFileSize(session);
        this.storageProperties = requireNonNull(storageProperties, "storageProperties is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.sortedWritingEnabled = isSortedWritingEnabled(session);
        this.sortingFileWriterBufferSize = requireNonNull(sortingFileWriterBufferSize, "sortingFileWriterBufferSize is null");
        this.sortingFileWriterMaxOpenFiles = sortingFileWriterMaxOpenFiles;
        this.tempDirectory = Location.of(locationProvider.newDataLocation("trino-tmp-files"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.columnTypes = getColumns(outputSchema, typeManager).stream()
                .map(IcebergColumnHandle::getType)
                .collect(toImmutableList());

        if (sortedWritingEnabled) {
            ImmutableList.Builder<Integer> sortColumnIndexes = ImmutableList.builder();
            ImmutableList.Builder<SortOrder> sortOrders = ImmutableList.builder();
            for (TrinoSortField sortField : sortOrder) {
                Types.NestedField column = outputSchema.findField(sortField.getSourceColumnId());
                if (column == null) {
                    throw new TrinoException(ICEBERG_INVALID_METADATA, "Unable to find sort field source column in the table schema: " + sortField);
                }
                sortColumnIndexes.add(outputSchema.columns().indexOf(column));
                sortOrders.add(sortField.getSortOrder());
            }
            this.sortColumnIndexes = sortColumnIndexes.build();
            this.sortOrders = sortOrders.build();
        }
        else {
            this.sortColumnIndexes = ImmutableList.of();
            this.sortOrders = ImmutableList.of();
        }
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
        doAppend(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        for (int writerIndex = 0; writerIndex < writers.size(); writerIndex++) {
            closeWriter(writerIndex);
        }
        writers.clear();

        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        List<Closeable> rollbackActions = Streams.concat(
                        writers.stream()
                                .filter(Objects::nonNull)
                                .map(writer -> writer::rollback),
                        closedWriterRollbackActions.stream())
                .collect(toImmutableList());
        RuntimeException error = null;
        for (Closeable rollbackAction : rollbackActions) {
            try {
                rollbackAction.close();
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
        int writeOffset = 0;
        while (writeOffset < page.getPositionCount()) {
            Page chunk = page.getRegion(writeOffset, min(page.getPositionCount() - writeOffset, MAX_PAGE_POSITIONS));
            writeOffset += chunk.getPositionCount();
            writePage(chunk);
        }
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
                closeWriter(writerIndex);
            }

            Optional<PartitionData> partitionData = getPartitionData(pagePartitioner.getColumns(), page, position);
            // prepend query id to a file name so we can determine which files were written by which query. This is needed for opportunistic cleanup of extra files
            // which may be present for successfully completing query in presence of failure recovery mechanisms.
            String fileName = fileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
            String outputPath = partitionData
                    .map(partition -> locationProvider.newDataLocation(partitionSpec, partition, fileName))
                    .orElseGet(() -> locationProvider.newDataLocation(fileName));

            if (!sortOrder.isEmpty() && sortedWritingEnabled) {
                String tempName = "sorting-file-writer-%s-%s".formatted(session.getQueryId(), randomUUID());
                Location tempFilePrefix = tempDirectory.appendPath(tempName);
                WriteContext writerContext = createWriter(outputPath, partitionData);
                IcebergFileWriter sortedFileWriter = new IcebergSortingFileWriter(
                        fileSystem,
                        tempFilePrefix,
                        writerContext.getWriter(),
                        sortingFileWriterBufferSize,
                        sortingFileWriterMaxOpenFiles,
                        columnTypes,
                        sortColumnIndexes,
                        sortOrders,
                        pageSorter,
                        typeManager.getTypeOperators());
                writer = new WriteContext(sortedFileWriter, outputPath, partitionData);
            }
            else {
                writer = createWriter(outputPath, partitionData);
            }

            writers.set(writerIndex, writer);
            memoryUsage += writer.getWriter().getMemoryUsage();
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private void closeWriter(int writerIndex)
    {
        WriteContext writeContext = writers.get(writerIndex);
        IcebergFileWriter writer = writeContext.getWriter();

        long currentWritten = writer.getWrittenBytes();
        long currentMemory = writer.getMemoryUsage();

        closedWriterRollbackActions.add(writer.commit());

        validationCpuNanos += writer.getValidationCpuNanos();
        writtenBytes += (writer.getWrittenBytes() - currentWritten);
        memoryUsage -= currentMemory;

        writers.set(writerIndex, null);

        CommitTaskData task = new CommitTaskData(
                writeContext.getPath(),
                fileFormat,
                writer.getWrittenBytes(),
                new MetricsWrapper(writer.getMetrics()),
                PartitionSpecParser.toJson(partitionSpec),
                writeContext.getPartitionData().map(PartitionData::toJson),
                DATA,
                Optional.empty());

        commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
    }

    private WriteContext createWriter(String outputPath, Optional<PartitionData> partitionData)
    {
        IcebergFileWriter writer = fileWriterFactory.createDataFileWriter(
                fileSystem,
                Location.of(outputPath),
                outputSchema,
                session,
                fileFormat,
                metricsConfig,
                storageProperties);

        return new WriteContext(writer, outputPath, partitionData);
    }

    private Optional<PartitionData> getPartitionData(List<PartitionColumn> columns, Page page, int position)
    {
        if (columns.isEmpty()) {
            return Optional.empty();
        }

        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            PartitionColumn column = columns.get(i);
            Block block = page.getBlock(column.getSourceChannel());
            Type type = column.getSourceType();
            org.apache.iceberg.types.Type icebergType = outputSchema.findType(column.getField().sourceId());
            Object value = getIcebergValue(block, position, type);
            values[i] = applyTransform(column.getField().transform(), icebergType, value);
        }
        return Optional.of(new PartitionData(values));
    }

    @SuppressWarnings("unchecked")
    private static Object applyTransform(Transform<?, ?> transform, org.apache.iceberg.types.Type icebergType, Object value)
    {
        return ((Transform<Object, Object>) transform).bind(icebergType).apply(value);
    }

    public static Object getIcebergValue(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(TINYINT)) {
            return (int) TINYINT.getByte(block, position);
        }
        if (type.equals(SMALLINT)) {
            return (int) SMALLINT.getShort(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(DATE)) {
            return DATE.getInt(block, position);
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position);
        }
        if (type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type.equals(TIME_MICROS)) {
            return TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return TIMESTAMP_MICROS.getLong(block, position);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros(getTimestampTz(block, position));
        }
        if (type instanceof VarbinaryType varbinaryType) {
            return varbinaryType.getSlice(block, position).getBytes();
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(UUID)) {
            return trinoUuidToJavaUuid(UUID.getSlice(block, position));
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
        private final String path;
        private final Optional<PartitionData> partitionData;

        public WriteContext(IcebergFileWriter writer, String path, Optional<PartitionData> partitionData)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.path = requireNonNull(path, "path is null");
            this.partitionData = requireNonNull(partitionData, "partitionData is null");
        }

        public IcebergFileWriter getWriter()
        {
            return writer;
        }

        public String getPath()
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

        public void rollback()
        {
            writer.rollback();
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
