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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.deltalake.DataFileInfo.DataFileType;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.CompressionCodec;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getCompressionCodec;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.canonicalizeColumnName;
import static io.trino.plugin.hive.util.HiveUtil.escapePathName;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

public abstract class AbstractDeltaLakePageSink
        implements ConnectorPageSink
{
    private static final Logger LOG = Logger.get(AbstractDeltaLakePageSink.class);

    private static final int MAX_PAGE_POSITIONS = 4096;

    private final List<DeltaLakeColumnHandle> dataColumnHandles;
    private final int[] dataColumnInputIndex;
    private final List<String> dataColumnNames;
    private final List<Type> dataColumnTypes;

    private final int[] partitionColumnsInputIndex;
    private final List<String> originalPartitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final PageIndexer pageIndexer;
    private final TrinoFileSystem fileSystem;

    private final int maxOpenWriters;

    protected final JsonCodec<DataFileInfo> dataFileInfoCodec;

    private final List<DeltaLakeWriter> writers = new ArrayList<>();

    private final String tableLocation;
    protected final String outputPathDirectory;
    private final ConnectorSession session;
    private final DeltaLakeWriterStats stats;
    private final String trinoVersion;
    private final long targetMaxFileSize;

    private long writtenBytes;
    private long memoryUsage;

    private final List<Closeable> closedWriterRollbackActions = new ArrayList<>();
    protected final ImmutableList.Builder<DataFileInfo> dataFileInfos = ImmutableList.builder();

    public AbstractDeltaLakePageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String tableLocation,
            String outputPathDirectory,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            String trinoVersion)
    {
        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null").create(session);
        this.maxOpenWriters = maxOpenWriters;
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");

        // determine the input index of the partition columns and data columns
        int[] partitionColumnInputIndex = new int[originalPartitionColumns.size()];
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();

        Type[] partitionColumnTypes = new Type[originalPartitionColumns.size()];
        String[] originalPartitionColumnNames = new String[originalPartitionColumns.size()];

        ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> dataColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> dataColumnNames = ImmutableList.builder();

        Map<String, String> canonicalToOriginalPartitionColumns = new HashMap<>();
        Map<String, Integer> canonicalToOriginalPartitionPositions = new HashMap<>();
        int partitionColumnPosition = 0;
        for (String partitionColumnName : originalPartitionColumns) {
            String canonicalizeColumnName = canonicalizeColumnName(partitionColumnName);
            canonicalToOriginalPartitionColumns.put(canonicalizeColumnName, partitionColumnName);
            canonicalToOriginalPartitionPositions.put(canonicalizeColumnName, partitionColumnPosition++);
        }
        for (int inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
            DeltaLakeColumnHandle column = inputColumns.get(inputIndex);
            switch (column.getColumnType()) {
                case PARTITION_KEY:
                    int partitionPosition = canonicalToOriginalPartitionPositions.get(column.getName());
                    partitionColumnInputIndex[partitionPosition] = inputIndex;
                    originalPartitionColumnNames[partitionPosition] = canonicalToOriginalPartitionColumns.get(column.getName());
                    partitionColumnTypes[partitionPosition] = column.getType();
                    break;
                case REGULAR:
                    dataColumnHandles.add(column);
                    dataColumnsInputIndex.add(inputIndex);
                    dataColumnNames.add(column.getName());
                    dataColumnTypes.add(column.getType());
                    break;
                case SYNTHESIZED:
                    processSynthesizedColumn(column);
                    break;
                default:
                    throw new IllegalStateException("Unexpected column type: " + column.getColumnType());
            }
        }

        addSpecialColumns(inputColumns, dataColumnHandles, dataColumnsInputIndex, dataColumnNames, dataColumnTypes);
        this.partitionColumnsInputIndex = partitionColumnInputIndex;
        this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());
        this.originalPartitionColumnNames = ImmutableList.copyOf(originalPartitionColumnNames);
        this.dataColumnHandles = dataColumnHandles.build();
        this.partitionColumnTypes = ImmutableList.copyOf(partitionColumnTypes);
        this.dataColumnNames = dataColumnNames.build();
        this.dataColumnTypes = dataColumnTypes.build();

        this.pageIndexer = pageIndexerFactory.createPageIndexer(this.partitionColumnTypes);

        this.tableLocation = tableLocation;
        this.outputPathDirectory = outputPathDirectory;
        this.session = requireNonNull(session, "session is null");
        this.stats = stats;

        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.targetMaxFileSize = DeltaLakeSessionProperties.getTargetMaxFileSize(session);
    }

    protected abstract void processSynthesizedColumn(DeltaLakeColumnHandle column);

    protected abstract void addSpecialColumns(
            List<DeltaLakeColumnHandle> inputColumns,
            ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles,
            ImmutableList.Builder<Integer> dataColumnsInputIndex,
            ImmutableList.Builder<String> dataColumnNames,
            ImmutableList.Builder<Type> dataColumnTypes);

    protected abstract String getPathPrefix();

    protected abstract DataFileType getDataFileType();

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
        return 0;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        for (int writerIndex = 0; writerIndex < writers.size(); writerIndex++) {
            closeWriter(writerIndex);
        }
        writers.clear();

        List<DataFileInfo> dataFilesInfo = dataFileInfos.build();
        Collection<Slice> result = dataFilesInfo.stream()
                .map(dataFileInfo -> wrappedBuffer(dataFileInfoCodec.toJsonBytes(dataFileInfo)))
                .collect(toImmutableList());
        return MoreFutures.toCompletableFuture(Futures.immediateFuture(result));
    }

    @Override
    public void abort()
    {
        List<Closeable> rollbackActions = Streams.concat(
                        writers.stream()
                                // writers can contain nulls if an exception is thrown when doAppend expands the writer list
                                .filter(Objects::nonNull)
                                .map(writer -> writer::rollback),
                        closedWriterRollbackActions.stream())
                .collect(toImmutableList());
        RuntimeException rollbackException = null;
        for (Closeable rollbackAction : rollbackActions) {
            try {
                rollbackAction.close();
            }
            catch (Throwable t) {
                if (rollbackException == null) {
                    rollbackException = new TrinoException(DELTA_LAKE_BAD_WRITE, "Error rolling back write to Delta Lake");
                }
                rollbackException.addSuppressed(t);
            }
        }
        if (rollbackException != null) {
            throw rollbackException;
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
        return NOT_BLOCKED;
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
            counts[index] = count + 1;
        }

        // invoke the writers
        Page dataPage = getDataPage(page);
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // If write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = dataPage;
            if (positions.length != dataPage.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            DeltaLakeWriter writer = writers.get(index);

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getMemoryUsage();

            writer.appendRows(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            memoryUsage += (writer.getMemoryUsage() - currentMemory);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        int[] writerIndexes = pageIndexer.indexPage(partitionColumns);
        if (pageIndexer.getMaxIndex() >= maxOpenWriters) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Exceeded limit of %s open writers for partitions", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pageIndexer.getMaxIndex()) {
            writers.add(null);
        }
        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            DeltaLakeWriter deltaLakeWriter = writers.get(writerIndex);
            if (deltaLakeWriter != null) {
                if (deltaLakeWriter.getWrittenBytes() <= targetMaxFileSize) {
                    continue;
                }
                closeWriter(writerIndex);
            }

            Path filePath = new Path(outputPathDirectory);

            List<String> partitionValues = createPartitionValues(partitionColumnTypes, partitionColumns, position);
            Optional<String> partitionName = Optional.empty();
            if (!originalPartitionColumnNames.isEmpty()) {
                String partName = makePartName(originalPartitionColumnNames, partitionValues);
                filePath = new Path(outputPathDirectory, partName);
                partitionName = Optional.of(partName);
            }

            String fileName = session.getQueryId() + "-" + randomUUID();
            filePath = new Path(filePath, fileName);

            FileWriter fileWriter = createParquetFileWriter(filePath.toString());

            Path rootTableLocationPath = new Path(tableLocation);
            DeltaLakeWriter writer = new DeltaLakeWriter(
                    fileSystem,
                    fileWriter,
                    rootTableLocationPath,
                    getRelativeFilePath(partitionName, fileName),
                    partitionValues,
                    stats,
                    dataColumnHandles,
                    getDataFileType());

            writers.set(writerIndex, writer);
            memoryUsage += writer.getMemoryUsage();
        }
        verify(writers.size() == pageIndexer.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private String getRelativeFilePath(Optional<String> partitionName, String fileName)
    {
        return getPathPrefix() + partitionName.map(partition -> new Path(partition, fileName).toString()).orElse(fileName);
    }

    protected void closeWriter(int writerIndex)
    {
        DeltaLakeWriter writer = writers.get(writerIndex);

        long currentWritten = writer.getWrittenBytes();
        long currentMemory = writer.getMemoryUsage();

        closedWriterRollbackActions.add(writer.commit());

        writtenBytes += writer.getWrittenBytes() - currentWritten;
        memoryUsage -= currentMemory;

        writers.set(writerIndex, null);

        try {
            DataFileInfo dataFileInfo = writer.getDataFileInfo();
            dataFileInfos.add(dataFileInfo);
        }
        catch (IOException e) {
            LOG.warn("exception '%s' while finishing write on %s", e, writer);
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error committing Parquet file to Delta Lake", e);
        }
    }

    /**
     * Copy of {@link HiveUtil#makePartName} modified to preserve case of partition columns.
     */
    private static String makePartName(List<String> partitionColumns, List<String> partitionValues)
    {
        StringBuilder name = new StringBuilder();

        for (int i = 0; i < partitionColumns.size(); ++i) {
            if (i > 0) {
                name.append("/");
            }

            name.append(escapePathName(partitionColumns.get(i)));
            name.append('=');
            name.append(escapePathName(partitionValues.get(i)));
        }

        return name.toString();
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        return HiveWriteUtils.createPartitionValues(partitionColumnTypes, partitionColumns, position).stream()
                .map(value -> value.equals(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION) ? null : value)
                .collect(toList());
    }

    private FileWriter createParquetFileWriter(String path)
    {
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(getParquetWriterBlockSize(session))
                .setMaxPageSize(getParquetWriterPageSize(session))
                .build();
        CompressionCodec compressionCodec = getCompressionCodec(session).getParquetCompressionCodec();

        try {
            Closeable rollbackAction = () -> fileSystem.deleteFile(path);

            List<Type> parquetTypes = dataColumnTypes.stream()
                    .map(type -> {
                        if (type instanceof TimestampWithTimeZoneType) {
                            verify(((TimestampWithTimeZoneType) type).getPrecision() == 3, "Unsupported type: %s", type);
                            return TIMESTAMP_MILLIS;
                        }
                        return type;
                    })
                    .collect(toImmutableList());

            // we use identity column mapping; input page already contains only data columns per
            // DataLagePageSink.getDataPage()
            int[] identityMapping = new int[dataColumnTypes.size()];
            for (int i = 0; i < identityMapping.length; ++i) {
                identityMapping[i] = i;
            }

            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(parquetTypes, dataColumnNames, false, false);
            return new ParquetFileWriter(
                    fileSystem.newOutputFile(path),
                    rollbackAction,
                    parquetTypes,
                    dataColumnNames,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    identityMapping,
                    compressionCodec,
                    trinoVersion,
                    false,
                    Optional.empty(),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error creating Parquet file", e);
        }
    }

    private Page getDataPage(Page page)
    {
        Block[] blocks = new Block[dataColumnInputIndex.length];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
