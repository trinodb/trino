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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot.MetadataAndProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.checkpoint.MetadataAndProtocolEntries;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isCheckpointFilteringEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TransactionLogAccess
{
    private static final Pattern CLASSIC_CHECKPOINT = Pattern.compile("(\\d*)\\.checkpoint\\.parquet");
    private static final Pattern MULTI_PART_CHECKPOINT = Pattern.compile("(\\d*)\\.checkpoint\\.(\\d*)\\.(\\d*)\\.parquet");
    private static final Pattern V2_CHECKPOINT = Pattern.compile("(\\d*)\\.checkpoint\\.[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\.(json|parquet)");

    private final TypeManager typeManager;
    private final CheckpointSchemaManager checkpointSchemaManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ParquetReaderOptions parquetReaderOptions;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private final int domainCompactionThreshold;
    private final DataSize transactionLogMaxCachedFileSize;

    private final Cache<TableLocation, TableSnapshot> tableSnapshots;
    private final Cache<TableVersion, DeltaLakeDataFileCacheEntry> activeDataFileCache;

    @Inject
    public TransactionLogAccess(
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            DeltaLakeConfig deltaLakeConfig,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            TrinoFileSystemFactory fileSystemFactory,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions().withBloomFilter(false);
        this.checkpointRowStatisticsWritingEnabled = deltaLakeConfig.isCheckpointRowStatisticsWritingEnabled();
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.transactionLogMaxCachedFileSize = deltaLakeConfig.getTransactionLogMaxCachedFileSize();

        tableSnapshots = EvictableCacheBuilder.newBuilder()
                .weigher((Weigher<TableLocation, TableSnapshot>) (key, value) -> Ints.saturatedCast(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
                .maximumWeight(deltaLakeConfig.getMetadataCacheMaxRetainedSize().toBytes())
                .expireAfterWrite(deltaLakeConfig.getMetadataCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        activeDataFileCache = EvictableCacheBuilder.newBuilder()
                .weigher((Weigher<TableVersion, DeltaLakeDataFileCacheEntry>) (key, value) -> Ints.saturatedCast(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
                .maximumWeight(deltaLakeConfig.getDataFileCacheSize().toBytes())
                .expireAfterWrite(deltaLakeConfig.getDataFileCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
    }

    @Managed
    @Nested
    public CacheStatsMBean getDataFileMetadataCacheStats()
    {
        return new CacheStatsMBean(activeDataFileCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getMetadataCacheStats()
    {
        return new CacheStatsMBean(tableSnapshots);
    }

    public TableSnapshot loadSnapshot(ConnectorSession session, SchemaTableName table, String tableLocation, Optional<Long> endVersion)
            throws IOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        if (endVersion.isPresent()) {
            return loadSnapshotForTimeTravel(fileSystem, table, tableLocation, endVersion.get());
        }

        TableLocation cacheKey = new TableLocation(table, tableLocation);
        TableSnapshot cachedSnapshot = tableSnapshots.getIfPresent(cacheKey);
        TableSnapshot snapshot;
        if (cachedSnapshot == null) {
            try {
                Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
                snapshot = tableSnapshots.get(cacheKey, () ->
                        TableSnapshot.load(
                                table,
                                lastCheckpoint,
                                fileSystem,
                                tableLocation,
                                parquetReaderOptions,
                                checkpointRowStatisticsWritingEnabled,
                                domainCompactionThreshold,
                                transactionLogMaxCachedFileSize,
                                endVersion));
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e);
            }
        }
        else {
            Optional<TableSnapshot> updatedSnapshot = cachedSnapshot.getUpdatedSnapshot(fileSystem, Optional.empty());
            if (updatedSnapshot.isPresent()) {
                snapshot = updatedSnapshot.get();
                tableSnapshots.asMap().replace(cacheKey, cachedSnapshot, snapshot);
            }
            else {
                snapshot = cachedSnapshot;
            }
        }
        return snapshot;
    }

    private TableSnapshot loadSnapshotForTimeTravel(TrinoFileSystem fileSystem, SchemaTableName table, String tableLocation, long endVersion)
            throws IOException
    {
        return TableSnapshot.load(
                table,
                findCheckpoint(fileSystem, tableLocation, endVersion),
                fileSystem,
                tableLocation,
                parquetReaderOptions,
                checkpointRowStatisticsWritingEnabled,
                domainCompactionThreshold,
                transactionLogMaxCachedFileSize,
                Optional.of(endVersion));
    }

    private static Optional<LastCheckpoint> findCheckpoint(TrinoFileSystem fileSystem, String tableLocation, long endVersion)
    {
        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
        if (lastCheckpoint.isPresent() && lastCheckpoint.get().version() <= endVersion) {
            return lastCheckpoint;
        }

        // TODO https://github.com/trinodb/trino/issues/21366 Make this logic efficient
        Optional<LastCheckpoint> latestCheckpoint = Optional.empty();
        Location transactionDirectory = Location.of(getTransactionLogDir(tableLocation));
        try {
            FileIterator files = fileSystem.listFiles(transactionDirectory);
            while (files.hasNext()) {
                FileEntry file = files.next();
                Optional<LastCheckpoint> checkpoint = extractCheckpointVersion(file);
                if (checkpoint.isEmpty()) {
                    continue;
                }

                long version = checkpoint.get().version();
                if (version == endVersion) {
                    return checkpoint;
                }
                if (version > endVersion || (latestCheckpoint.isPresent() && version < latestCheckpoint.get().version())) {
                    continue;
                }
                latestCheckpoint = checkpoint;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return latestCheckpoint;
    }

    private static Optional<LastCheckpoint> extractCheckpointVersion(FileEntry file)
    {
        String fileName = file.location().fileName();
        Matcher classicCheckpoint = CLASSIC_CHECKPOINT.matcher(fileName);
        if (classicCheckpoint.matches()) {
            long version = Long.parseLong(classicCheckpoint.group(1));
            return Optional.of(new LastCheckpoint(version, file.length(), Optional.empty(), Optional.empty()));
        }

        Matcher multiPartCheckpoint = MULTI_PART_CHECKPOINT.matcher(fileName);
        if (multiPartCheckpoint.matches()) {
            long version = Long.parseLong(multiPartCheckpoint.group(1));
            int parts = Integer.parseInt(multiPartCheckpoint.group(3));
            return Optional.of(new LastCheckpoint(version, file.length(), Optional.of(parts), Optional.empty()));
        }

        Matcher v2Checkpoint = V2_CHECKPOINT.matcher(fileName);
        if (v2Checkpoint.matches()) {
            long version = Long.parseLong(v2Checkpoint.group(1));
            return Optional.of(new LastCheckpoint(version, file.length(), Optional.empty(), Optional.of(new V2Checkpoint(fileName))));
        }

        return Optional.empty();
    }

    public void flushCache()
    {
        tableSnapshots.invalidateAll();
        activeDataFileCache.invalidateAll();
    }

    public void invalidateCache(SchemaTableName schemaTableName, Optional<String> tableLocation)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        // Invalidate by location in case one table (location) unregistered and re-register under different name
        tableLocation.ifPresent(location -> {
            invalidateAllIf(tableSnapshots, cacheKey -> cacheKey.location().equals(location));
            invalidateAllIf(activeDataFileCache, cacheKey -> cacheKey.tableLocation().location().equals(location));
        });
        invalidateAllIf(tableSnapshots, cacheKey -> cacheKey.tableName().equals(schemaTableName));
        invalidateAllIf(activeDataFileCache, cacheKey -> cacheKey.tableLocation().tableName().equals(schemaTableName));
    }

    public MetadataEntry getMetadataEntry(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        if (tableSnapshot.getCachedMetadata().isEmpty()) {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try (Stream<MetadataEntry> metadataEntries = getEntries(
                    session,
                    tableSnapshot,
                    ImmutableSet.of(METADATA),
                    (checkpointStream, jsonTransactions) ->
                            Stream.concat(
                                    checkpointStream
                                            .map(DeltaLakeTransactionLogEntry::getMetaData)
                                            .filter(Objects::nonNull),
                                    jsonTransactions.stream()
                                            .map(transaction -> transaction.transactionEntries().getMetadataAndProtocol(fileSystem))
                                            .filter(entry -> entry.metadata().isPresent())
                                            .map(entry -> entry.metadata().get())),
                    fileSystem,
                    fileFormatDataSourceStats)) {
                // Get last entry in the stream
                tableSnapshot.setCachedMetadata(metadataEntries.reduce((first, second) -> second));
            }
        }
        return tableSnapshot.getCachedMetadata()
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + tableSnapshot.getTable()));
    }

    public Stream<AddFileEntry> getActiveFiles(
            ConnectorSession session,
            TableSnapshot tableSnapshot,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Set<DeltaLakeColumnHandle> projectedColumns)
    {
        Set<String> baseColumnNames = projectedColumns.stream()
                .filter(DeltaLakeColumnHandle::isBaseColumn) // Only base column stats are supported
                .map(DeltaLakeColumnHandle::columnName)
                .collect(toImmutableSet());
        return getActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, partitionConstraint, baseColumnNames::contains);
    }

    public Stream<AddFileEntry> getActiveFiles(
            ConnectorSession session,
            TableSnapshot tableSnapshot,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Predicate<String> addStatsMinMaxColumnFilter)
    {
        try {
            if (isCheckpointFilteringEnabled(session)) {
                return loadActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, partitionConstraint, addStatsMinMaxColumnFilter);
            }

            TableVersion tableVersion = new TableVersion(new TableLocation(tableSnapshot.getTable(), tableSnapshot.getTableLocation()), tableSnapshot.getVersion());

            DeltaLakeDataFileCacheEntry cacheEntry = activeDataFileCache.get(tableVersion, () -> {
                DeltaLakeDataFileCacheEntry oldCached = activeDataFileCache.asMap().keySet().stream()
                        .filter(key -> key.tableLocation().equals(tableVersion.tableLocation()) &&
                                key.version() < tableVersion.version())
                        .flatMap(key -> Optional.ofNullable(activeDataFileCache.getIfPresent(key))
                                .map(value -> Map.entry(key, value))
                                .stream())
                        .max(Comparator.comparing(entry -> entry.getKey().version()))
                        .map(Map.Entry::getValue)
                        .orElse(null);
                if (oldCached != null) {
                    try {
                        List<DeltaLakeTransactionLogEntry> newEntries = getJsonEntries(
                                oldCached.getVersion(),
                                tableSnapshot.getVersion(),
                                tableSnapshot,
                                fileSystemFactory.create(session));
                        return oldCached.withUpdatesApplied(newEntries, tableSnapshot.getVersion());
                    }
                    catch (MissingTransactionLogException e) {
                        // The cached state cannot be used to calculate current state, as some
                        // intermediate transaction files are expired.
                    }
                }

                List<AddFileEntry> activeFiles;
                try (Stream<AddFileEntry> addFileEntryStream = loadActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
                    activeFiles = addFileEntryStream.collect(toImmutableList());
                }
                return new DeltaLakeDataFileCacheEntry(tableSnapshot.getVersion(), activeFiles);
            });
            return cacheEntry.getActiveFiles().stream();
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Failed accessing transaction log for table: " + tableSnapshot.getTable(), e);
        }
    }

    public Stream<AddFileEntry> loadActiveFiles(
            ConnectorSession session,
            TableSnapshot tableSnapshot,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Predicate<String> addStatsMinMaxColumnFilter)
    {
        List<Transaction> transactions = tableSnapshot.getTransactions();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try (Stream<DeltaLakeTransactionLogEntry> checkpointEntries = tableSnapshot.getCheckpointTransactionLogEntries(
                session,
                ImmutableSet.of(ADD),
                checkpointSchemaManager,
                typeManager,
                fileSystem,
                fileFormatDataSourceStats,
                Optional.of(new MetadataAndProtocolEntry(metadataEntry, protocolEntry)),
                partitionConstraint,
                Optional.of(addStatsMinMaxColumnFilter))) {
            return activeAddEntries(checkpointEntries, transactions, fileSystem)
                    .filter(partitionConstraint.isAll()
                            ? addAction -> true
                            : addAction -> partitionMatchesPredicate(addAction.getCanonicalPartitionValues(), partitionConstraint.getDomains().orElseThrow()));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error reading transaction log for " + tableSnapshot.getTable(), e);
        }
    }

    public static List<DeltaLakeColumnMetadata> columnsWithStats(MetadataEntry metadataEntry, ProtocolEntry protocolEntry, TypeManager typeManager)
    {
        return columnsWithStats(DeltaLakeSchemaSupport.extractSchema(metadataEntry, protocolEntry, typeManager), metadataEntry.getOriginalPartitionColumns());
    }

    public static ImmutableList<DeltaLakeColumnMetadata> columnsWithStats(List<DeltaLakeColumnMetadata> schema, List<String> partitionColumns)
    {
        return schema.stream()
                .filter(column -> !partitionColumns.contains(column.name()))
                .filter(column -> {
                    Type type = column.type();
                    return !(type instanceof MapType || type instanceof ArrayType || type.equals(BooleanType.BOOLEAN) || type.equals(VarbinaryType.VARBINARY));
                })
                .collect(toImmutableList());
    }

    private Stream<AddFileEntry> activeAddEntries(Stream<DeltaLakeTransactionLogEntry> checkpointEntries, List<Transaction> transactions, TrinoFileSystem fileSystem)
    {
        Map<FileEntryKey, AddFileEntry> activeJsonEntries = new LinkedHashMap<>();
        HashSet<FileEntryKey> removedFiles = new HashSet<>();

        // The json entries containing the last few entries in the log need to be applied on top of the parquet snapshot:
        // - Any files which have been removed need to be excluded
        // - Any files with newer add actions need to be updated with the most recent metadata
        transactions.forEach(transaction -> {
            Map<FileEntryKey, AddFileEntry> addFilesInTransaction = new LinkedHashMap<>();
            Set<FileEntryKey> removedFilesInTransaction = new HashSet<>();
            try (Stream<DeltaLakeTransactionLogEntry> entries = transaction.transactionEntries().getEntries(fileSystem)) {
                entries.forEach(deltaLakeTransactionLogEntry -> {
                    if (deltaLakeTransactionLogEntry.getAdd() != null) {
                        AddFileEntry add = deltaLakeTransactionLogEntry.getAdd();
                        addFilesInTransaction.put(new FileEntryKey(add.getPath(), add.getDeletionVector().map(DeletionVectorEntry::uniqueId)), add);
                    }
                    else if (deltaLakeTransactionLogEntry.getRemove() != null) {
                        RemoveFileEntry remove = deltaLakeTransactionLogEntry.getRemove();
                        removedFilesInTransaction.add(new FileEntryKey(remove.path(), remove.deletionVector().map(DeletionVectorEntry::uniqueId)));
                    }
                });
            }

            // Process 'remove' entries first because deletion vectors register both 'add' and 'remove' entries and the 'add' entry should be kept
            removedFiles.addAll(removedFilesInTransaction);
            removedFilesInTransaction.forEach(activeJsonEntries::remove);
            activeJsonEntries.putAll(addFilesInTransaction);
        });

        Stream<AddFileEntry> filteredCheckpointEntries = checkpointEntries
                .map(DeltaLakeTransactionLogEntry::getAdd)
                .filter(Objects::nonNull)
                .filter(addEntry -> {
                    FileEntryKey key = new FileEntryKey(addEntry.getPath(), addEntry.getDeletionVector().map(DeletionVectorEntry::uniqueId));
                    return !removedFiles.contains(key) && !activeJsonEntries.containsKey(key);
                });

        return Stream.concat(filteredCheckpointEntries, activeJsonEntries.values().stream());
    }

    private record FileEntryKey(String path, Optional<String> deletionVectorId) {}

    public MetadataAndProtocolEntries getMetadataAndProtocolEntry(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        if (tableSnapshot.getCachedMetadata().isEmpty() || tableSnapshot.getCachedProtocol().isEmpty()) {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try (Stream<MetadataAndProtocolEntries> entries = getEntries(
                    session,
                    tableSnapshot,
                    ImmutableSet.of(METADATA, PROTOCOL),
                    (checkpointStream, jsonTransactions) ->
                            Stream.concat(
                                    checkpointStream
                                            .filter(entry -> entry.getMetaData() != null || entry.getProtocol() != null)
                                            .map(entry -> new MetadataAndProtocolEntries(entry.getMetaData(), entry.getProtocol())),
                                    jsonTransactions.stream()
                                            .map(transaction -> transaction.transactionEntries().getMetadataAndProtocol(fileSystem))),
                    fileSystem,
                    fileFormatDataSourceStats)) {
                Map<Class<?>, Object> logEntries = entries
                        .flatMap(MetadataAndProtocolEntries::stream)
                        .collect(toImmutableMap(Object::getClass, Function.identity(), (_, second) -> second));
                tableSnapshot.setCachedMetadata(Optional.ofNullable((MetadataEntry) logEntries.get(MetadataEntry.class)));
                tableSnapshot.setCachedProtocol(Optional.ofNullable((ProtocolEntry) logEntries.get(ProtocolEntry.class)));
            }
        }
        return new MetadataAndProtocolEntries(tableSnapshot.getCachedMetadata(), tableSnapshot.getCachedProtocol());
    }

    public ProtocolEntry getProtocolEntry(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        if (tableSnapshot.getCachedProtocol().isEmpty()) {
            try (Stream<ProtocolEntry> protocolEntries = getProtocolEntries(session, tableSnapshot)) {
                // Get last entry in the stream
                tableSnapshot.setCachedProtocol(protocolEntries.reduce((first, second) -> second));
            }
        }
        return tableSnapshot.getCachedProtocol()
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol entry not found in transaction log for table " + tableSnapshot.getTable()));
    }

    public Stream<ProtocolEntry> getProtocolEntries(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        return getEntries(
                session,
                tableSnapshot,
                ImmutableSet.of(PROTOCOL),
                (checkpointStream, jsonTransactions) ->
                        Stream.concat(
                                checkpointStream
                                        .map(DeltaLakeTransactionLogEntry::getProtocol)
                                        .filter(Objects::nonNull),
                                jsonTransactions.stream()
                                        .map(transaction -> transaction.transactionEntries().getMetadataAndProtocol(fileSystem))
                                        .filter(entry -> entry.protocol().isPresent())
                                        .map(entry -> entry.protocol().get())),
                fileSystem,
                fileFormatDataSourceStats);
    }

    /**
     * Produces a stream of actions from the transaction log, combining the Parquet checkpoint and the JSON transactions.
     * While {@link DeltaLakeTransactionLogEntry} has fields for multiple actions, only one can be populated in any instance.
     * We take advantage of that by only reading one column from the Parquet checkpoint.
     *
     * @param session the current session
     * @param entryTypes A set of transaction log entry types to retrieve
     * @param entryMapper extracts and filters out the required elements from the TransactionLogEntries. Receives the actions from the checkpoint file, and the actions
     * from the JSON commits in chronological order
     * @param <T> the type of the action
     * @return an object that encapsulates a stream of actions
     */
    private <T> Stream<T> getEntries(
            ConnectorSession session,
            TableSnapshot tableSnapshot,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            BiFunction<Stream<DeltaLakeTransactionLogEntry>, List<Transaction>, Stream<T>> entryMapper,
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats)
    {
        try {
            List<Transaction> transactions = tableSnapshot.getTransactions();
            // Passing TupleDomain.all() because this method is used for getting all entries
            Stream<DeltaLakeTransactionLogEntry> checkpointEntries = tableSnapshot.getCheckpointTransactionLogEntries(
                    session, entryTypes, checkpointSchemaManager, typeManager, fileSystem, stats, Optional.empty(), TupleDomain.all(), Optional.of(alwaysTrue()));

            return entryMapper.apply(
                    checkpointEntries,
                    transactions);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error reading transaction log for " + tableSnapshot.getTable(), e);
        }
    }

    public Stream<DeltaLakeTransactionLogEntry> getJsonEntries(TrinoFileSystem fileSystem, String transactionLogDir, List<Long> forVersions)
    {
        return forVersions.stream()
                .flatMap(version -> {
                    try {
                        Optional<TransactionLogEntries> entriesFromJson = getEntriesFromJson(version, transactionLogDir, fileSystem, transactionLogMaxCachedFileSize);
                        return entriesFromJson.map(entries -> entries.getEntries(fileSystem))
                                // transaction log does not exist. Might have been expired.
                                .orElseGet(Stream::of);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    /**
     * Returns a stream of transaction log versions between {@code startAt} point in time and {@code lastVersion}.
     */
    public List<Long> getPastTableVersions(TrinoFileSystem fileSystem, String transactionLogDir, Instant startAt, long lastVersion)
    {
        ImmutableList.Builder<Long> result = ImmutableList.builder();
        for (long version = lastVersion; version >= 0; version--) {
            Location entryPath = getTransactionLogJsonEntryPath(transactionLogDir, version);
            TrinoInputFile inputFile = fileSystem.newInputFile(entryPath);
            try {
                if (inputFile.lastModified().isBefore(startAt)) {
                    // already too old
                    break;
                }
            }
            catch (FileNotFoundException e) {
                // no longer exists, break iteration
                break;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            result.add(version);
        }
        return result.build();
    }

    private List<DeltaLakeTransactionLogEntry> getJsonEntries(long startVersion, long endVersion, TableSnapshot tableSnapshot, TrinoFileSystem fileSystem)
            throws IOException
    {
        Optional<Long> lastCheckpointVersion = tableSnapshot.getLastCheckpointVersion();
        if (lastCheckpointVersion.isPresent() && startVersion < lastCheckpointVersion.get()) {
            return ImmutableList.<DeltaLakeTransactionLogEntry>builder()
                    .addAll(TransactionLogTail.loadNewTail(fileSystem, tableSnapshot.getTableLocation(), Optional.of(startVersion), lastCheckpointVersion, transactionLogMaxCachedFileSize).getFileEntries(fileSystem))
                    .addAll(tableSnapshot.getJsonTransactionLogEntries(fileSystem))
                    .build();
        }
        return TransactionLogTail.loadNewTail(fileSystem, tableSnapshot.getTableLocation(), Optional.of(startVersion), Optional.of(endVersion), transactionLogMaxCachedFileSize).getFileEntries(fileSystem);
    }

    public static String canonicalizeColumnName(String columnName)
    {
        return columnName.toLowerCase(Locale.ENGLISH);
    }

    public static <T> Map<CanonicalColumnName, T> toCanonicalNameKeyedMap(Map<String, T> map, Map<String, CanonicalColumnName> canonicalColumnNames)
    {
        return map.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(toImmutableMap(
                        entry -> requireNonNull(
                                canonicalColumnNames.get(entry.getKey()),
                                format("Did not find CanonicalColumnName for %s", entry.getKey())),
                        Map.Entry::getValue));
    }

    @Deprecated
    // we should migrate codebase to use maps keyed on CanonicalColumnName all over the place.
    public static <T> Map<String, T> toOriginalNameKeyedMap(Map<CanonicalColumnName, T> map)
    {
        return map.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> entry.getKey().getOriginalName(),
                        Map.Entry::getValue));
    }

    private record TableLocation(SchemaTableName tableName, String location)
    {
        private static final int INSTANCE_SIZE = instanceSize(TableLocation.class);

        TableLocation
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(location, "location is null");
        }

        long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    tableName.getRetainedSizeInBytes() +
                    estimatedSizeOf(location);
        }
    }

    private record TableVersion(TableLocation tableLocation, long version)
    {
        private static final int INSTANCE_SIZE = instanceSize(TableVersion.class);

        TableVersion
        {
            requireNonNull(tableLocation, "tableLocation is null");
        }

        long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    tableLocation.getRetainedSizeInBytes();
        }
    }
}
