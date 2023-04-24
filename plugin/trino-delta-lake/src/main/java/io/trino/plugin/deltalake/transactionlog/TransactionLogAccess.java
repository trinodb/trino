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
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.log.Logger;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
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
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.collect.cache.CacheUtils.invalidateAllIf;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.COMMIT;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TransactionLogAccess
{
    private static final Logger log = Logger.get(TransactionLogAccess.class);

    private final TypeManager typeManager;
    private final CheckpointSchemaManager checkpointSchemaManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ParquetReaderOptions parquetReaderOptions;
    private final Cache<CacheKey, TableSnapshot> tableSnapshots;
    private final Cache<CacheKey, DeltaLakeDataFileCacheEntry> activeDataFileCache;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private final int domainCompactionThreshold;

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

        tableSnapshots = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(deltaLakeConfig.getMetadataCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .maximumSize(deltaLakeConfig.getMetadataCacheMaxSize())
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        activeDataFileCache = EvictableCacheBuilder.newBuilder()
                .weigher((Weigher<CacheKey, DeltaLakeDataFileCacheEntry>) (key, value) -> Ints.saturatedCast(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
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

    public TableSnapshot loadSnapshot(SchemaTableName table, String tableLocation, ConnectorSession session)
            throws IOException
    {
        CacheKey cacheKey = new CacheKey(table, tableLocation);
        TableSnapshot cachedSnapshot = tableSnapshots.getIfPresent(cacheKey);
        TableSnapshot snapshot;
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        if (cachedSnapshot == null) {
            try {
                snapshot = tableSnapshots.get(cacheKey, () ->
                        TableSnapshot.load(
                                table,
                                fileSystem,
                                tableLocation,
                                parquetReaderOptions,
                                checkpointRowStatisticsWritingEnabled,
                                domainCompactionThreshold));
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e);
            }
        }
        else {
            Optional<TableSnapshot> updatedSnapshot = cachedSnapshot.getUpdatedSnapshot(fileSystem);
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
            invalidateAllIf(activeDataFileCache, cacheKey -> cacheKey.location().equals(location));
        });
        invalidateAllIf(tableSnapshots, cacheKey -> cacheKey.tableName().equals(schemaTableName));
        invalidateAllIf(activeDataFileCache, cacheKey -> cacheKey.tableName().equals(schemaTableName));
    }

    public MetadataEntry getMetadataEntry(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        if (tableSnapshot.getCachedMetadata().isEmpty()) {
            try (Stream<MetadataEntry> metadataEntries = getEntries(
                    tableSnapshot,
                    METADATA,
                    entryStream -> entryStream.map(DeltaLakeTransactionLogEntry::getMetaData).filter(Objects::nonNull),
                    session,
                    fileSystemFactory.create(session),
                    fileFormatDataSourceStats)) {
                // Get last entry in the stream
                tableSnapshot.setCachedMetadata(metadataEntries.reduce((first, second) -> second));
            }
        }
        return tableSnapshot.getCachedMetadata()
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + tableSnapshot.getTable()));
    }

    public List<AddFileEntry> getActiveFiles(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        try {
            CacheKey cacheKey = new CacheKey(tableSnapshot.getTable(), tableSnapshot.getTableLocation());
            DeltaLakeDataFileCacheEntry cachedTable = activeDataFileCache.get(cacheKey, () -> {
                List<AddFileEntry> activeFiles = loadActiveFiles(tableSnapshot, session);
                return new DeltaLakeDataFileCacheEntry(tableSnapshot.getVersion(), activeFiles);
            });
            if (cachedTable.getVersion() > tableSnapshot.getVersion()) {
                log.warn("Query run with outdated Transaction Log Snapshot, retrieved stale table entries for table: %s and query %s", tableSnapshot.getTable(), session.getQueryId());
                return loadActiveFiles(tableSnapshot, session);
            }
            if (cachedTable.getVersion() < tableSnapshot.getVersion()) {
                DeltaLakeDataFileCacheEntry updatedCacheEntry;
                try {
                    List<DeltaLakeTransactionLogEntry> newEntries = getJsonEntries(
                            cachedTable.getVersion(),
                            tableSnapshot.getVersion(),
                            tableSnapshot,
                            fileSystemFactory.create(session));
                    updatedCacheEntry = cachedTable.withUpdatesApplied(newEntries, tableSnapshot.getVersion());
                }
                catch (MissingTransactionLogException e) {
                    // Reset the cached table when there are transaction files which are newer than
                    // the cached table version which are already garbage collected.
                    List<AddFileEntry> activeFiles = loadActiveFiles(tableSnapshot, session);
                    updatedCacheEntry = new DeltaLakeDataFileCacheEntry(tableSnapshot.getVersion(), activeFiles);
                }

                activeDataFileCache.asMap().replace(cacheKey, cachedTable, updatedCacheEntry);
                cachedTable = updatedCacheEntry;
            }
            return cachedTable.getActiveFiles();
        }
        catch (IOException | ExecutionException | UncheckedExecutionException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Failed accessing transaction log for table: " + tableSnapshot.getTable(), e);
        }
    }

    private List<AddFileEntry> loadActiveFiles(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        try (Stream<AddFileEntry> entries = getEntries(
                tableSnapshot,
                ImmutableSet.of(ADD),
                this::activeAddEntries,
                session,
                fileSystemFactory.create(session),
                fileFormatDataSourceStats)) {
            List<AddFileEntry> activeFiles = entries.collect(toImmutableList());
            return activeFiles;
        }
    }

    public static List<DeltaLakeColumnMetadata> columnsWithStats(MetadataEntry metadataEntry, TypeManager typeManager)
    {
        return columnsWithStats(DeltaLakeSchemaSupport.extractSchema(metadataEntry, typeManager), metadataEntry.getCanonicalPartitionColumns());
    }

    public static ImmutableList<DeltaLakeColumnMetadata> columnsWithStats(List<DeltaLakeColumnMetadata> schema, List<String> partitionColumns)
    {
        return schema.stream()
                .filter(column -> !partitionColumns.contains(column.getName()))
                .filter(column -> {
                    Type type = column.getType();
                    return !(type instanceof MapType || type instanceof ArrayType || type.equals(BooleanType.BOOLEAN) || type.equals(VarbinaryType.VARBINARY));
                })
                .collect(toImmutableList());
    }

    private Stream<AddFileEntry> activeAddEntries(Stream<DeltaLakeTransactionLogEntry> checkpointEntries, Stream<DeltaLakeTransactionLogEntry> jsonEntries)
    {
        Map<String, AddFileEntry> activeJsonEntries = new LinkedHashMap<>();
        HashSet<String> removedFiles = new HashSet<>();

        // The json entries containing the last few entries in the log need to be applied on top of the parquet snapshot:
        // - Any files which have been removed need to be excluded
        // - Any files with newer add actions need to be updated with the most recent metadata
        jsonEntries.forEach(deltaLakeTransactionLogEntry -> {
            AddFileEntry addEntry = deltaLakeTransactionLogEntry.getAdd();
            if (addEntry != null) {
                activeJsonEntries.put(addEntry.getPath(), addEntry);
            }

            RemoveFileEntry removeEntry = deltaLakeTransactionLogEntry.getRemove();
            if (removeEntry != null) {
                activeJsonEntries.remove(removeEntry.getPath());
                removedFiles.add(removeEntry.getPath());
            }
        });

        Stream<AddFileEntry> filteredCheckpointEntries = checkpointEntries
                .map(DeltaLakeTransactionLogEntry::getAdd)
                .filter(Objects::nonNull)
                .filter(addEntry -> !removedFiles.contains(addEntry.getPath()) && !activeJsonEntries.containsKey(addEntry.getPath()));

        return Stream.concat(filteredCheckpointEntries, activeJsonEntries.values().stream());
    }

    public Stream<RemoveFileEntry> getRemoveEntries(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        return getEntries(
                tableSnapshot,
                REMOVE,
                entryStream -> entryStream.map(DeltaLakeTransactionLogEntry::getRemove).filter(Objects::nonNull),
                session,
                fileSystemFactory.create(session),
                fileFormatDataSourceStats);
    }

    public Stream<ProtocolEntry> getProtocolEntries(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        return getEntries(
                tableSnapshot,
                PROTOCOL,
                entryStream -> entryStream.map(DeltaLakeTransactionLogEntry::getProtocol).filter(Objects::nonNull),
                session,
                fileSystemFactory.create(session),
                fileFormatDataSourceStats);
    }

    public Stream<CommitInfoEntry> getCommitInfoEntries(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        return getEntries(
                tableSnapshot,
                COMMIT,
                entryStream -> entryStream.map(DeltaLakeTransactionLogEntry::getCommitInfo).filter(Objects::nonNull),
                session,
                fileSystemFactory.create(session),
                fileFormatDataSourceStats);
    }

    /**
     * Produces a stream of actions from the transaction log, combining the Parquet checkpoint and the JSON transactions.
     * While {@link DeltaLakeTransactionLogEntry} has fields for multiple actions, only one can be populated in any instance.
     * We take advantage of that by only reading one column from the Parquet checkpoint.
     *
     * @param entryTypes A set of transaction log entry types to retrieve
     * @param entryMapper extracts and filters out the required elements from the TransactionLogEntries. Receives the actions from the checkpoint file, and the actions
     * from the JSON commits in chronological order
     * @param session the current session
     * @param <T> the type of the action
     * @return an object that encapsulates a stream of actions
     */
    private <T> Stream<T> getEntries(
            TableSnapshot tableSnapshot,
            Set<CheckpointEntryIterator.EntryType> entryTypes,
            BiFunction<Stream<DeltaLakeTransactionLogEntry>, Stream<DeltaLakeTransactionLogEntry>, Stream<T>> entryMapper,
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats)
    {
        try {
            Stream<DeltaLakeTransactionLogEntry> jsonEntries = tableSnapshot.getJsonTransactionLogEntries().stream();
            Stream<DeltaLakeTransactionLogEntry> checkpointEntries = tableSnapshot.getCheckpointTransactionLogEntries(
                    session, entryTypes, checkpointSchemaManager, typeManager, fileSystem, stats);

            return entryMapper.apply(
                    checkpointEntries,
                    jsonEntries);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error reading transaction log for " + tableSnapshot.getTable(), e);
        }
    }

    /**
     * Convenience method for accessors which don't need to separate out the checkpoint entries from the json entries.
     */
    private <T> Stream<T> getEntries(
            TableSnapshot tableSnapshot,
            CheckpointEntryIterator.EntryType entryType,
            Function<Stream<DeltaLakeTransactionLogEntry>, Stream<T>> entryMapper,
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats)
    {
        return getEntries(
                tableSnapshot,
                ImmutableSet.of(entryType),
                (checkpointStream, jsonStream) -> entryMapper.apply(Stream.concat(checkpointStream, jsonStream)),
                session,
                fileSystem,
                stats);
    }

    public Stream<DeltaLakeTransactionLogEntry> getJsonEntries(TrinoFileSystem fileSystem, String transactionLogDir, List<Long> forVersions)
    {
        return forVersions.stream()
                .flatMap(version -> {
                    try {
                        Optional<List<DeltaLakeTransactionLogEntry>> entriesFromJson = getEntriesFromJson(version, transactionLogDir, fileSystem);
                        //noinspection SimplifyOptionalCallChains
                        return entriesFromJson.map(List::stream)
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
                return null;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            result.add(version);
        }
        return result.build();
    }

    private static List<DeltaLakeTransactionLogEntry> getJsonEntries(long startVersion, long endVersion, TableSnapshot tableSnapshot, TrinoFileSystem fileSystem)
            throws IOException
    {
        Optional<Long> lastCheckpointVersion = tableSnapshot.getLastCheckpointVersion();
        if (lastCheckpointVersion.isPresent() && startVersion < lastCheckpointVersion.get()) {
            return ImmutableList.<DeltaLakeTransactionLogEntry>builder()
                    .addAll(TransactionLogTail.loadNewTail(fileSystem, tableSnapshot.getTableLocation(), Optional.of(startVersion), lastCheckpointVersion).getFileEntries())
                    .addAll(tableSnapshot.getJsonTransactionLogEntries())
                    .build();
        }
        return TransactionLogTail.loadNewTail(fileSystem, tableSnapshot.getTableLocation(), Optional.of(startVersion), Optional.of(endVersion)).getFileEntries();
    }

    public static <T> String canonicalizeColumnName(String columnName)
    {
        return columnName.toLowerCase(Locale.ENGLISH);
    }

    public static <T> Map<CanonicalColumnName, T> toCanonicalNameKeyedMap(Map<String, T> map)
    {
        return map.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> new CanonicalColumnName(entry.getKey()),
                        Map.Entry::getValue));
    }

    public static <T> Map<CanonicalColumnName, T> toCanonicalNameKeyedMap(Map<String, T> map, Map<String, CanonicalColumnName> canonicalColumnNames)
    {
        return map.entrySet().stream()
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

    private record CacheKey(SchemaTableName tableName, String location)
    {
        private static final int INSTANCE_SIZE = instanceSize(CacheKey.class);

        CacheKey
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
}
