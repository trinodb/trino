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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.FileOrcDataSource;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.raptor.legacy.RaptorColumnHandle;
import io.trino.plugin.raptor.legacy.backup.BackupManager;
import io.trino.plugin.raptor.legacy.backup.BackupStore;
import io.trino.plugin.raptor.legacy.metadata.ColumnInfo;
import io.trino.plugin.raptor.legacy.metadata.ColumnStats;
import io.trino.plugin.raptor.legacy.metadata.ShardDelta;
import io.trino.plugin.raptor.legacy.metadata.ShardInfo;
import io.trino.plugin.raptor.legacy.metadata.ShardRecorder;
import io.trino.plugin.raptor.legacy.storage.OrcFileRewriter.OrcFileInfo;
import io.trino.plugin.raptor.legacy.storage.RaptorPageSource.ColumnAdaptation;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.MoreFutures.allAsList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.isBucketNumberColumn;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.isHiddenColumn;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.isMergeRowIdColumn;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.isShardRowIdColumn;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.isShardUuidColumn;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_ERROR;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_LOCAL_DISK_FULL;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_RECOVERY_TIMEOUT;
import static io.trino.plugin.raptor.legacy.storage.ShardStats.computeColumnStats;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

public class RaptorStorageManager
        implements StorageManager
{
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);

    private static final long MAX_ROWS = 1_000_000_000;
    // TODO: do not limit the max size of blocks to read for now; enable the limit when the Hive connector is ready
    private static final DataSize HUGE_MAX_READ_BLOCK_SIZE = DataSize.of(1, PETABYTE);
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final String nodeId;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final OrcReaderOptions orcReaderOptions;
    private final BackupManager backupManager;
    private final ShardRecoveryManager recoveryManager;
    private final ShardRecorder shardRecorder;
    private final Duration recoveryTimeout;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final DataSize minAvailableSpace;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;

    @Inject
    public RaptorStorageManager(
            NodeManager nodeManager,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            StorageManagerConfig config,
            CatalogName catalogName,
            BackupManager backgroundBackupManager,
            ShardRecoveryManager recoveryManager,
            ShardRecorder shardRecorder,
            TypeManager typeManager)
    {
        this(nodeManager.getCurrentNode().getNodeIdentifier(),
                storageService,
                backupStore,
                config.toOrcReaderOptions(),
                backgroundBackupManager,
                recoveryManager,
                shardRecorder,
                typeManager,
                catalogName.toString(),
                config.getDeletionThreads(),
                config.getShardRecoveryTimeout(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getMinAvailableSpace());
    }

    public RaptorStorageManager(
            String nodeId,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            OrcReaderOptions orcReaderOptions,
            BackupManager backgroundBackupManager,
            ShardRecoveryManager recoveryManager,
            ShardRecorder shardRecorder,
            TypeManager typeManager,
            String connectorId,
            int deletionThreads,
            Duration shardRecoveryTimeout,
            long maxShardRows,
            DataSize maxShardSize,
            DataSize minAvailableSpace)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.orcReaderOptions = orcReaderOptions
                .withMaxReadBlockSize(HUGE_MAX_READ_BLOCK_SIZE);

        backupManager = requireNonNull(backgroundBackupManager, "backgroundBackupManager is null");
        this.recoveryManager = requireNonNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = requireNonNull(shardRecoveryTimeout, "shardRecoveryTimeout is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(maxShardRows, MAX_ROWS);
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");
        this.minAvailableSpace = requireNonNull(minAvailableSpace, "minAvailableSpace is null");
        this.shardRecorder = requireNonNull(shardRecorder, "shardRecorder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(deletionThreads, daemonThreadsNamed("raptor-delete-" + connectorId + "-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdown();
    }

    @Override
    public ConnectorPageSource getPageSource(
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OrcReaderOptions orcReaderOptions)
    {
        orcReaderOptions = orcReaderOptions.withMaxReadBlockSize(HUGE_MAX_READ_BLOCK_SIZE);
        OrcDataSource dataSource = openShard(shardUuid, orcReaderOptions);

        AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();

        try {
            OrcReader reader = OrcReader.createOrcReader(dataSource, orcReaderOptions)
                    .orElseThrow(() -> new TrinoException(RAPTOR_ERROR, "Data file is empty for shard " + shardUuid));

            Map<Long, OrcColumn> indexMap = columnIdIndex(reader.getRootColumn().getNestedColumns());
            List<OrcColumn> fileReadColumn = new ArrayList<>(columnIds.size());
            List<Type> fileReadTypes = new ArrayList<>(columnIds.size());
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(columnIds.size());
            for (int i = 0; i < columnIds.size(); i++) {
                long columnId = columnIds.get(i);

                if (isHiddenColumn(columnId)) {
                    columnAdaptations.add(specialColumnAdaptation(columnId, shardUuid, bucketNumber));
                    continue;
                }

                Type type = toOrcFileType(columnTypes.get(i), typeManager);
                OrcColumn fileColumn = indexMap.get(columnId);
                if (fileColumn == null) {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(type));
                }
                else {
                    int sourceIndex = fileReadColumn.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumn.add(fileColumn);
                    fileReadTypes.add(type);
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumn,
                    fileReadTypes,
                    predicate,
                    UTC,
                    memoryUsage,
                    INITIAL_BATCH_SIZE,
                    RaptorPageSource::handleException);

            return new RaptorPageSource(recordReader, columnAdaptations, dataSource, memoryUsage);
        }
        catch (IOException | RuntimeException e) {
            closeQuietly(dataSource);
            throw new TrinoException(RAPTOR_ERROR, "Failed to create page source for shard " + shardUuid, e);
        }
        catch (Throwable t) {
            closeQuietly(dataSource);
            throw t;
        }
    }

    private static ColumnAdaptation specialColumnAdaptation(long columnId, UUID shardUuid, OptionalInt bucketNumber)
    {
        if (isShardRowIdColumn(columnId)) {
            return ColumnAdaptation.rowIdColumn();
        }
        if (isShardUuidColumn(columnId)) {
            return ColumnAdaptation.shardUuidColumn(shardUuid);
        }
        if (isBucketNumberColumn(columnId)) {
            return ColumnAdaptation.bucketNumberColumn(bucketNumber);
        }
        if (isMergeRowIdColumn(columnId)) {
            return ColumnAdaptation.mergeRowIdColumn(bucketNumber, shardUuid);
        }
        throw new TrinoException(RAPTOR_ERROR, "Invalid column ID: " + columnId);
    }

    @Override
    public StoragePageSink createStoragePageSink(long transactionId, OptionalInt bucketNumber, List<Long> columnIds, List<Type> columnTypes, boolean checkSpace)
    {
        if (checkSpace && storageService.getAvailableBytes() < minAvailableSpace.toBytes()) {
            throw new TrinoException(RAPTOR_LOCAL_DISK_FULL, "Local disk is full on node " + nodeId);
        }
        return new RaptorStoragePageSink(transactionId, columnIds, columnTypes, bucketNumber);
    }

    @Override
    public ShardRewriter createShardRewriter(long transactionId, OptionalInt bucketNumber, UUID shardUuid)
    {
        return rowsToDelete -> {
            if (rowsToDelete.isEmpty()) {
                return completedFuture(ImmutableList.of());
            }
            return supplyAsync(() -> rewriteShard(transactionId, bucketNumber, shardUuid, rowsToDelete), deletionExecutor);
        };
    }

    private void writeShard(UUID shardUuid)
    {
        if (backupStore.isPresent() && !backupStore.get().shardExists(shardUuid)) {
            throw new TrinoException(RAPTOR_ERROR, "Backup does not exist after write");
        }

        File stagingFile = storageService.getStagingFile(shardUuid);
        File storageFile = storageService.getStorageFile(shardUuid);

        storageService.createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }
    }

    @VisibleForTesting
    OrcDataSource openShard(UUID shardUuid, OrcReaderOptions orcReaderOptions)
    {
        File file = storageService.getStorageFile(shardUuid).getAbsoluteFile();

        if (!file.exists() && backupStore.isPresent()) {
            try {
                Future<Void> future = recoveryManager.recoverShard(shardUuid);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                if (e.getCause() != null) {
                    throwIfInstanceOf(e.getCause(), TrinoException.class);
                }
                throw new TrinoException(RAPTOR_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new TrinoException(RAPTOR_RECOVERY_TIMEOUT, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }

        try {
            return fileOrcDataSource(orcReaderOptions, file);
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private static FileOrcDataSource fileOrcDataSource(OrcReaderOptions orcReaderOptions, File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, orcReaderOptions);
    }

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, File file, Set<String> nodes, long rowCount, long uncompressedSize)
    {
        return new ShardInfo(shardUuid, bucketNumber, nodes, computeShardStats(file), rowCount, file.length(), uncompressedSize, xxhash64(file));
    }

    private List<ColumnStats> computeShardStats(File file)
    {
        try (OrcDataSource dataSource = fileOrcDataSource(orcReaderOptions, file)) {
            OrcReader reader = OrcReader.createOrcReader(dataSource, orcReaderOptions)
                    .orElseThrow(() -> new TrinoException(RAPTOR_ERROR, "Data file is empty: " + file));

            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            for (ColumnInfo info : getColumnInfo(typeManager, reader)) {
                computeColumnStats(reader, info.getColumnId(), info.getType(), typeManager).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    @VisibleForTesting
    Collection<Slice> rewriteShard(long transactionId, OptionalInt bucketNumber, UUID shardUuid, BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }

        UUID newShardUuid = UUID.randomUUID();
        File input = storageService.getStorageFile(shardUuid);
        File output = storageService.getStagingFile(newShardUuid);

        OrcFileInfo info = rewriteFile(typeManager, input, output, rowsToDelete);
        long rowCount = info.getRowCount();

        if (rowCount == 0) {
            return shardDelta(shardUuid, Optional.empty());
        }

        shardRecorder.recordCreatedShard(transactionId, newShardUuid);

        // submit for backup and wait until it finishes
        getFutureValue(backupManager.submit(newShardUuid, output));

        Set<String> nodes = ImmutableSet.of(nodeId);
        long uncompressedSize = info.getUncompressedSize();

        ShardInfo shard = createShardInfo(newShardUuid, bucketNumber, output, nodes, rowCount, uncompressedSize);

        writeShard(newShardUuid);

        return shardDelta(shardUuid, Optional.of(shard));
    }

    private static Collection<Slice> shardDelta(UUID oldShardUuid, Optional<ShardInfo> shardInfo)
    {
        List<ShardInfo> newShards = shardInfo.map(ImmutableList::of).orElse(ImmutableList.of());
        ShardDelta delta = new ShardDelta(ImmutableList.of(oldShardUuid), newShards);
        return ImmutableList.of(Slices.wrappedBuffer(SHARD_DELTA_CODEC.toJsonBytes(delta)));
    }

    private static OrcFileInfo rewriteFile(TypeManager typeManager, File input, File output, BitSet rowsToDelete)
    {
        try {
            return OrcFileRewriter.rewrite(typeManager, input, output, rowsToDelete);
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to rewrite shard file: " + input, e);
        }
    }

    static List<ColumnInfo> getColumnInfo(TypeManager typeManager, OrcReader reader)
    {
        return getColumnInfoFromOrcUserMetadata(typeManager, getOrcFileMetadata(reader));
    }

    static long xxhash64(File file)
    {
        try (InputStream in = new FileInputStream(file)) {
            return XxHash64.hash(in);
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    private static OrcFileMetadata getOrcFileMetadata(OrcReader reader)
    {
        return Optional.ofNullable(reader.getFooter().getUserMetadata().get(OrcFileMetadata.KEY))
                .map(slice -> METADATA_CODEC.fromJson(slice.getBytes()))
                .orElseThrow(() -> new TrinoException(RAPTOR_ERROR, "No Raptor metadata in file"));
    }

    private static List<ColumnInfo> getColumnInfoFromOrcUserMetadata(TypeManager typeManager, OrcFileMetadata orcFileMetadata)
    {
        return orcFileMetadata.getColumnTypes().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new ColumnInfo(entry.getKey(), typeManager.getType(entry.getValue())))
                .collect(toList());
    }

    static Type toOrcFileType(Type raptorType, TypeManager typeManager)
    {
        if ((raptorType == BOOLEAN) ||
                (raptorType == TINYINT) ||
                (raptorType == SMALLINT) ||
                (raptorType == INTEGER) ||
                (raptorType == BIGINT) ||
                (raptorType == REAL) ||
                (raptorType == DOUBLE) ||
                (raptorType == DATE) ||
                (raptorType instanceof DecimalType) ||
                (raptorType instanceof VarcharType) ||
                (raptorType instanceof VarbinaryType)) {
            return raptorType;
        }
        if (raptorType.equals(TIMESTAMP_MILLIS)) {
            // TIMESTAMPS are stored as BIGINT to avoid the poor encoding in ORC
            return BIGINT;
        }
        if (raptorType instanceof ArrayType) {
            Type elementType = toOrcFileType(((ArrayType) raptorType).getElementType(), typeManager);
            return new ArrayType(elementType);
        }
        if (raptorType instanceof MapType) {
            TypeSignature keyType = toOrcFileType(((MapType) raptorType).getKeyType(), typeManager).getTypeSignature();
            TypeSignature valueType = toOrcFileType(((MapType) raptorType).getValueType(), typeManager).getTypeSignature();
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(typeParameter(keyType), typeParameter(valueType)));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + raptorType);
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, OrcColumn> indexMap)
    {
        TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, value) -> {
            OrcColumn fileColumn = indexMap.get(columnHandle.getColumnId());
            if (fileColumn != null) {
                predicateBuilder.addColumn(fileColumn.getColumnId(), value);
            }
        });
        return predicateBuilder.build();
    }

    private static Map<Long, OrcColumn> columnIdIndex(List<OrcColumn> columns)
    {
        return uniqueIndex(columns, column -> Long.valueOf(column.getColumnName()));
    }

    private class RaptorStoragePageSink
            implements StoragePageSink
    {
        private final long transactionId;
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final OptionalInt bucketNumber;

        private final List<File> stagingFiles = new ArrayList<>();
        private final List<ShardInfo> shards = new ArrayList<>();
        private final List<CompletableFuture<?>> futures = new ArrayList<>();

        private boolean committed;
        private OrcFileWriter writer;
        private UUID shardUuid;

        public RaptorStoragePageSink(long transactionId, List<Long> columnIds, List<Type> columnTypes, OptionalInt bucketNumber)
        {
            this.transactionId = transactionId;
            this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxShardRows) || (writer.getUncompressedSize() >= maxShardSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                writer.close();

                shardRecorder.recordCreatedShard(transactionId, shardUuid);

                File stagingFile = storageService.getStagingFile(shardUuid);
                futures.add(backupManager.submit(shardUuid, stagingFile));

                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                shards.add(createShardInfo(shardUuid, bucketNumber, stagingFile, nodes, rowCount, uncompressedSize));

                writer = null;
                shardUuid = null;
            }
        }

        @Override
        public CompletableFuture<List<ShardInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();

            return allAsList(futures).thenApplyAsync(ignored -> {
                for (ShardInfo shard : shards) {
                    writeShard(shard.getShardUuid());
                }
                return ImmutableList.copyOf(shards);
            }, commitExecutor);
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public void rollback()
        {
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            }
            finally {
                for (File file : stagingFiles) {
                    file.delete();
                }

                // cancel incomplete backup jobs
                futures.forEach(future -> future.cancel(true));

                // delete completed backup shards
                backupStore.ifPresent(backupStore -> {
                    for (ShardInfo shard : shards) {
                        backupStore.deleteShard(shard.getShardUuid());
                    }
                });
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                File stagingFile = storageService.getStagingFile(shardUuid);
                storageService.createParents(stagingFile);
                stagingFiles.add(stagingFile);
                writer = new OrcFileWriter(typeManager, columnIds, columnTypes, stagingFile);
            }
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }
}
