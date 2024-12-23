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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.units.Duration;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.metastore.HivePartitionName;
import io.trino.plugin.hive.metastore.HiveTableName;
import io.trino.plugin.hive.metastore.PartitionFilter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.trino.plugin.hive.metastore.PartitionFilter.partitionFilter;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.ObjectType.OTHER;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.ObjectType.PARTITION;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.ObjectType.STATS;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public final class CachingHiveMetastore
        implements HiveMetastore
{
    public enum StatsRecording
    {
        ENABLED,
        DISABLED
    }

    public enum ObjectType
    {
        PARTITION,
        STATS,
        OTHER,
    }

    private final HiveMetastore delegate;
    private final Set<ObjectType> cacheMissing;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<HiveTableName, Optional<Table>> tableCache;
    private final LoadingCache<String, List<TableInfo>> tablesCacheNew;
    private final Cache<HiveTableName, AtomicReference<Map<String, HiveColumnStatistics>>> tableColumnStatisticsCache;
    private final Cache<HivePartitionName, AtomicReference<Map<String, HiveColumnStatistics>>> partitionStatisticsCache;
    private final Cache<HivePartitionName, AtomicReference<Optional<Partition>>> partitionCache;
    private final LoadingCache<PartitionFilter, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final LoadingCache<String, Optional<String>> configValuesCache;

    public static CachingHiveMetastore createPerTransactionCache(HiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                ImmutableSet.copyOf(ObjectType.values()),
                new CacheFactory(maximumSize),
                new CacheFactory(maximumSize),
                new CacheFactory(maximumSize),
                new CacheFactory(maximumSize));
    }

    public static CachingHiveMetastore createCachingHiveMetastore(
            HiveMetastore delegate,
            Duration metadataCacheTtl,
            Duration statsCacheTtl,
            Optional<Duration> refreshInterval,
            Executor refreshExecutor,
            long maximumSize,
            StatsRecording statsRecording,
            boolean partitionCacheEnabled,
            Set<ObjectType> cacheMissing)
    {
        // refresh executor is only required when the refresh interval is set, but the executor is
        // always set, so it is simpler to just enforce that
        requireNonNull(refreshExecutor, "refreshExecutor is null");

        long metadataCacheMillis = metadataCacheTtl.toMillis();
        long statsCacheMillis = statsCacheTtl.toMillis();
        checkArgument(metadataCacheMillis > 0 || statsCacheMillis > 0, "Cache not enabled");

        OptionalLong refreshMillis = refreshInterval.stream().mapToLong(Duration::toMillis).findAny();

        CacheFactory cacheFactory = CacheFactory.NEVER_CACHE;
        CacheFactory partitionCacheFactory = CacheFactory.NEVER_CACHE;
        if (metadataCacheMillis > 0) {
            cacheFactory = new CacheFactory(OptionalLong.of(metadataCacheMillis), refreshMillis, Optional.of(refreshExecutor), maximumSize, statsRecording);
            if (partitionCacheEnabled) {
                partitionCacheFactory = cacheFactory;
            }
        }

        CacheFactory statsCacheFactory = CacheFactory.NEVER_CACHE;
        CacheFactory partitionStatsCacheFactory = CacheFactory.NEVER_CACHE;
        if (statsCacheMillis > 0) {
            statsCacheFactory = new CacheFactory(OptionalLong.of(statsCacheMillis), refreshMillis, Optional.of(refreshExecutor), maximumSize, statsRecording);
            if (partitionCacheEnabled) {
                partitionStatsCacheFactory = statsCacheFactory;
            }
        }

        return new CachingHiveMetastore(
                delegate,
                cacheMissing,
                cacheFactory,
                partitionCacheFactory,
                statsCacheFactory,
                partitionStatsCacheFactory);
    }

    private CachingHiveMetastore(
            HiveMetastore delegate,
            Set<ObjectType> cacheMissing,
            CacheFactory cacheFactory,
            CacheFactory partitionCacheFactory,
            CacheFactory statsCacheFactory,
            CacheFactory partitionStatsCacheFactory)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheMissing = cacheMissing;

        databaseNamesCache = cacheFactory.buildCache(_ -> loadAllDatabases());
        databaseCache = cacheFactory.buildCache(this::loadDatabase);
        tablesCacheNew = cacheFactory.buildCache(this::loadTablesNew);
        tableColumnStatisticsCache = statsCacheFactory.buildCache(this::refreshTableColumnStatistics);
        tableCache = cacheFactory.buildCache(this::loadTable);
        tablePrivilegesCache = cacheFactory.buildCache(key -> loadTablePrivileges(key.database(), key.table(), key.owner(), key.principal()));
        rolesCache = cacheFactory.buildCache(_ -> loadRoles());
        roleGrantsCache = cacheFactory.buildCache(this::loadRoleGrants);
        configValuesCache = cacheFactory.buildCache(this::loadConfigValue);

        partitionStatisticsCache = partitionStatsCacheFactory.buildBulkCache();
        partitionFilterCache = partitionCacheFactory.buildCache(this::loadPartitionNamesByFilter);
        partitionCache = partitionCacheFactory.buildBulkCache();
    }

    @Managed
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tablesCacheNew.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
        tablePrivilegesCache.invalidateAll();
        tableColumnStatisticsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        rolesCache.invalidateAll();
    }

    public void flushPartitionCache(String schemaName, String tableName, List<String> partitionColumns, List<String> partitionValues)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionColumns, "partitionColumns is null");
        requireNonNull(partitionValues, "partitionValues is null");

        String providedPartitionName = makePartName(partitionColumns, partitionValues);
        invalidatePartitionCache(schemaName, tableName, partitionNameToCheck -> partitionNameToCheck.map(value -> value.equals(providedPartitionName)).orElse(false));
    }

    private AtomicReference<Map<String, HiveColumnStatistics>> refreshTableColumnStatistics(HiveTableName tableName, AtomicReference<Map<String, HiveColumnStatistics>> currentValueHolder)
    {
        Map<String, HiveColumnStatistics> currentValue = currentValueHolder.get();
        if (currentValue == null) {
            // do not refresh empty value
            return currentValueHolder;
        }

        // only refresh currently loaded columns
        Map<String, HiveColumnStatistics> columnStatistics = delegate.getTableColumnStatistics(tableName.getDatabaseName(), tableName.getTableName(), currentValue.keySet());

        // return new value holder to have only fresh data in case of concurrent loads
        return new AtomicReference<>(columnStatistics);
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            V value = cache.getUnchecked(key);
            checkState(!(value instanceof Optional), "This must not be used for caches with Optional values, as it doesn't implement cacheMissing logic. Use getOptional()");
            return value;
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private <K, V> Optional<V> getOptional(ObjectType objectType, LoadingCache<K, Optional<V>> cache, K key)
    {
        try {
            Optional<V> value = cache.getIfPresent(key);
            @SuppressWarnings("OptionalAssignedToNull")
            boolean valueIsPresent = value != null;
            if (valueIsPresent) {
                if (value.isPresent() || cacheMissing.contains(objectType)) {
                    return value;
                }
                cache.invalidate(key);
            }
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private static <K, V> V getWithValueHolder(Cache<K, AtomicReference<V>> cache, K key, Supplier<V> loader)
    {
        AtomicReference<V> valueHolder = uncheckedCacheGet(cache, key, AtomicReference::new);
        V value = valueHolder.get();
        if (value != null) {
            return value;
        }
        value = loader.get();
        if (value == null) {
            throw new InvalidCacheLoadException("Failed to return a value for " + key);
        }
        valueHolder.compareAndSet(null, value);
        return value;
    }

    private static <K, V> V getIncrementally(
            Cache<K, AtomicReference<V>> cache,
            K key,
            Predicate<V> isSufficient,
            Supplier<V> loader,
            Function<V, V> incrementalLoader,
            BinaryOperator<V> merger)
    {
        AtomicReference<V> valueHolder = uncheckedCacheGet(cache, key, AtomicReference::new);
        V oldValue = valueHolder.get();
        if (oldValue != null && isSufficient.test(oldValue)) {
            return oldValue;
        }

        V newValue = oldValue == null ? loader.get() : incrementalLoader.apply(oldValue);

        verifyNotNull(newValue, "loader returned null for %s", key);

        V merged = merger.apply(oldValue, newValue);
        if (!valueHolder.compareAndSet(oldValue, merged)) {
            // if the value changed in the valueHolder, we only add newly loaded value to be sure we have up-to-date value
            valueHolder.accumulateAndGet(newValue, merger);
        }
        return merged;
    }

    private static <K, V> Map<K, V> getAll(Cache<K, AtomicReference<V>> cache, Iterable<K> keys, Function<Set<K>, Map<K, V>> bulkLoader)
    {
        ImmutableMap.Builder<K, V> result = ImmutableMap.builder();
        ImmutableMap.Builder<K, AtomicReference<V>> toLoadBuilder = ImmutableMap.builder();

        for (K key : keys) {
            AtomicReference<V> valueHolder = uncheckedCacheGet(cache, key, AtomicReference::new);
            V value = valueHolder.get();
            if (value != null) {
                result.put(key, value);
            }
            else {
                toLoadBuilder.put(key, valueHolder);
            }
        }

        Map<K, AtomicReference<V>> toLoad = toLoadBuilder.buildOrThrow();
        if (toLoad.isEmpty()) {
            return result.buildOrThrow();
        }

        Map<K, V> newEntries = bulkLoader.apply(unmodifiableSet(toLoad.keySet()));
        toLoad.forEach((key, valueHolder) -> {
            V value = newEntries.get(key);
            if (value == null) {
                throw new InvalidCacheLoadException("Failed to return a value for " + key);
            }
            result.put(key, value);
            valueHolder.compareAndSet(null, value);
        });

        return result.buildOrThrow();
    }

    private static <K, V> Map<K, V> getAll(
            Cache<K, AtomicReference<V>> cache,
            Iterable<K> keys,
            Function<Collection<K>, Map<K, V>> bulkLoader,
            Predicate<V> isSufficient,
            BinaryOperator<V> merger)
    {
        ImmutableMap.Builder<K, V> result = ImmutableMap.builder();
        Map<K, AtomicReference<V>> toLoad = new HashMap<>();

        keys.forEach(key -> {
            // make sure the value holder is retrieved before the new values are loaded
            // so that in case of invalidation, we will not set the stale value
            AtomicReference<V> currentValueHolder = uncheckedCacheGet(cache, key, AtomicReference::new);
            V currentValue = currentValueHolder.get();
            if (currentValue != null && isSufficient.test(currentValue)) {
                result.put(key, currentValue);
            }
            else {
                toLoad.put(key, currentValueHolder);
            }
        });

        if (toLoad.isEmpty()) {
            return result.buildOrThrow();
        }

        Map<K, V> newEntries = bulkLoader.apply(toLoad.keySet());
        toLoad.forEach((key, valueHolder) -> {
            V newValue = newEntries.get(key);
            verifyNotNull(newValue, "loader returned null for %s", key);
            V merged = valueHolder.accumulateAndGet(newValue, merger);
            result.put(key, merged);
        });

        return result.buildOrThrow();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return getOptional(OTHER, databaseCache, databaseName);
    }

    private Optional<Database> loadDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return getOptional(OTHER, tableCache, hiveTableName(databaseName, tableName));
    }

    private Optional<Table> loadTable(HiveTableName hiveTableName)
    {
        return delegate.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        Map<String, HiveColumnStatistics> columnStatistics = getIncrementally(
                tableColumnStatisticsCache,
                hiveTableName(databaseName, tableName),
                currentStatistics -> currentStatistics.keySet().containsAll(columnNames),
                () -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames),
                currentStatistics -> {
                    SetView<String> missingColumns = difference(columnNames, currentStatistics.keySet());
                    return delegate.getTableColumnStatistics(databaseName, tableName, missingColumns);
                },
                (currentStats, newStats) -> mergeColumnStatistics(currentStats, newStats, columnNames));
        // HiveColumnStatistics.empty() are removed to make output consistent with non-cached metastore which simplifies testing
        return removeEmptyColumnStatistics(columnNames, columnStatistics);
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
        List<HivePartitionName> hivePartitionNames = partitionNames.stream().map(partitionName -> hivePartitionName(hiveTableName, partitionName)).toList();
        Map<HivePartitionName, Map<String, HiveColumnStatistics>> statistics = getAll(
                partitionStatisticsCache,
                hivePartitionNames,
                missingPartitions -> loadPartitionsColumnStatistics(databaseName, tableName, columnNames, missingPartitions),
                currentStats -> currentStats.keySet().containsAll(columnNames),
                (currentStats, newStats) -> mergeColumnStatistics(currentStats, newStats, columnNames));
        // HiveColumnStatistics.empty() are removed to make output consistent with non-cached metastore which simplifies testing
        return statistics.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> entry.getKey().getPartitionName().orElseThrow(),
                        entry -> removeEmptyColumnStatistics(columnNames, entry.getValue())));
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return delegate.useSparkTableStatistics();
    }

    private static ImmutableMap<String, HiveColumnStatistics> removeEmptyColumnStatistics(Set<String> columnNames, Map<String, HiveColumnStatistics> columnStatistics)
    {
        return columnStatistics.entrySet().stream()
                .filter(entry -> columnNames.contains(entry.getKey()) && !entry.getValue().equals(HiveColumnStatistics.empty()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    private Map<String, HiveColumnStatistics> mergeColumnStatistics(Map<String, HiveColumnStatistics> currentStats, Map<String, HiveColumnStatistics> newStats, Set<String> dataColumns)
    {
        requireNonNull(newStats, "newStats is null");
        ImmutableMap.Builder<String, HiveColumnStatistics> columnStatisticsBuilder = ImmutableMap.builder();
        // Populate empty statistics for all requested columns to cache absence of column statistics for future requests.
        if (cacheMissing.contains(STATS)) {
            columnStatisticsBuilder.putAll(Iterables.transform(
                    dataColumns,
                    column -> new AbstractMap.SimpleEntry<>(column, HiveColumnStatistics.empty())));
        }
        if (currentStats != null) {
            columnStatisticsBuilder.putAll(currentStats);
        }
        columnStatisticsBuilder.putAll(newStats);
        return columnStatisticsBuilder.buildKeepingLast();
    }

    private Map<HivePartitionName, Map<String, HiveColumnStatistics>> loadPartitionsColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> columnNames,
            Collection<HivePartitionName> partitionNamesToLoad)
    {
        if (partitionNamesToLoad.isEmpty()) {
            return ImmutableMap.of();
        }
        Set<String> partitionsToLoad = partitionNamesToLoad.stream()
                .map(partitionName -> partitionName.getPartitionName().orElseThrow())
                .collect(toImmutableSet());
        Map<String, Map<String, HiveColumnStatistics>> columnStatistics = delegate.getPartitionColumnStatistics(databaseName, tableName, partitionsToLoad, columnNames);

        ImmutableMap.Builder<HivePartitionName, Map<String, HiveColumnStatistics>> result = ImmutableMap.builder();
        for (HivePartitionName partitionName : partitionNamesToLoad) {
            result.put(partitionName, columnStatistics.getOrDefault(partitionName.getPartitionName().orElseThrow(), ImmutableMap.of()));
        }
        return result.buildOrThrow();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        try {
            delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate);
        }
        finally {
            HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
            tableColumnStatisticsCache.invalidate(hiveTableName);
            // basic stats are stored as table properties
            tableCache.invalidate(hiveTableName);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        try {
            delegate.updatePartitionStatistics(table, mode, partitionUpdates);
        }
        finally {
            partitionUpdates.keySet().forEach(partitionName -> {
                HivePartitionName hivePartitionName = hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName);
                partitionStatisticsCache.invalidate(hivePartitionName);
                // basic stats are stored as partition properties
                partitionCache.invalidate(hivePartitionName);
            });
        }
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return get(tablesCacheNew, databaseName);
    }

    private List<TableInfo> loadTablesNew(String databaseName)
    {
        return delegate.getTables(databaseName);
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            delegate.createDatabase(database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            delegate.dropDatabase(databaseName, deleteData);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            delegate.renameDatabase(databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        try {
            delegate.setDatabaseOwner(databaseName, principal);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    private void invalidateDatabase(String databaseName)
    {
        databaseCache.invalidate(databaseName);
        databaseNamesCache.invalidateAll();
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.createTable(table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            delegate.dropTable(databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        try {
            delegate.commentTable(databaseName, tableName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        try {
            delegate.setTableOwner(databaseName, tableName, principal);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        try {
            delegate.commentColumn(databaseName, tableName, columnName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        try {
            delegate.dropColumn(databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    public void invalidateTable(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = new HiveTableName(databaseName, tableName);
        tableCache.invalidate(hiveTableName);
        tablesCacheNew.invalidate(databaseName);
        invalidateAllIf(tablePrivilegesCache, userTableKey -> userTableKey.matches(databaseName, tableName));
        tableColumnStatisticsCache.invalidate(hiveTableName);
        invalidatePartitionCache(databaseName, tableName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return getWithValueHolder(partitionCache, hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues), () -> delegate.getPartition(table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return getOptional(PARTITION, partitionFilterCache, partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter));
    }

    private Optional<List<String>> loadPartitionNamesByFilter(PartitionFilter partitionFilter)
    {
        return delegate.getPartitionNamesByFilter(
                partitionFilter.getHiveTableName().getDatabaseName(),
                partitionFilter.getHiveTableName().getTableName(),
                partitionFilter.getPartitionColumnNames(),
                partitionFilter.getPartitionKeysFilter());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        List<HivePartitionName> names = partitionNames.stream()
                .map(name -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), name))
                .collect(toImmutableList());

        Map<HivePartitionName, Optional<Partition>> all = getAll(
                partitionCache,
                names,
                namesToLoad -> loadPartitionsByNames(table, namesToLoad));
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<HivePartitionName, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionName().orElseThrow(), entry.getValue());
        }
        return partitionsByName.buildOrThrow();
    }

    private Map<HivePartitionName, Optional<Partition>> loadPartitionsByNames(Table table, Iterable<? extends HivePartitionName> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        HivePartitionName firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getHiveTableName();
        List<String> partitionsToFetch = new ArrayList<>();
        for (HivePartitionName partitionName : partitionNames) {
            checkArgument(partitionName.getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getHiveTableName());
            partitionsToFetch.add(partitionName.getPartitionName().orElseThrow());
        }

        ImmutableMap.Builder<HivePartitionName, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(table, partitionsToFetch);
        for (HivePartitionName partitionName : partitionNames) {
            partitions.put(partitionName, partitionsByNames.getOrDefault(partitionName.getPartitionName().orElseThrow(), Optional.empty()));
        }
        return partitions.buildOrThrow();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            delegate.addPartitions(databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            delegate.dropPartition(databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            delegate.alterPartition(databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            delegate.createRole(role, grantor);
        }
        finally {
            rolesCache.invalidateAll();
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            delegate.dropRole(role);
        }
        finally {
            rolesCache.invalidateAll();
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<String> listRoles()
    {
        return get(rolesCache, "");
    }

    private Set<String> loadRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        try {
            delegate.grantRoles(roles, grantees, adminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        try {
            delegate.revokeRoles(roles, grantees, adminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return get(roleGrantsCache, principal);
    }

    private Set<RoleGrant> loadRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        invalidatePartitionCache(databaseName, tableName, partitionName -> true);
    }

    private void invalidatePartitionCache(String databaseName, String tableName, Predicate<Optional<String>> partitionPredicate)
    {
        HiveTableName hiveTableName = hiveTableName(databaseName, tableName);

        Predicate<HivePartitionName> hivePartitionPredicate = partitionName -> partitionName.getHiveTableName().equals(hiveTableName) &&
                partitionPredicate.test(partitionName.getPartitionName());

        invalidateAllIf(partitionCache, hivePartitionPredicate);
        invalidateAllIf(partitionFilterCache, partitionFilter -> partitionFilter.getHiveTableName().equals(hiveTableName));
        invalidateAllIf(partitionStatisticsCache, hivePartitionPredicate);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        try {
            delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
        }
        finally {
            invalidateTablePrivilegeCacheEntries(databaseName, tableName, tableOwner, grantee);
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        try {
            delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
        }
        finally {
            invalidateTablePrivilegeCacheEntries(databaseName, tableName, tableOwner, grantee);
        }
    }

    private void invalidateTablePrivilegeCacheEntries(String databaseName, String tableName, String tableOwner, HivePrincipal grantee)
    {
        // some callers of table privilege methods use Optional.of(grantee), some Optional.empty() (to get all privileges), so have to invalidate them both
        tablePrivilegesCache.invalidate(new UserTableKey(Optional.of(grantee), databaseName, tableName, Optional.of(tableOwner)));
        tablePrivilegesCache.invalidate(new UserTableKey(Optional.empty(), databaseName, tableName, Optional.of(tableOwner)));
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return get(tablePrivilegesCache, new UserTableKey(principal, databaseName, tableName, tableOwner));
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return getOptional(OTHER, configValuesCache, name);
    }

    private Optional<String> loadConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    @Override
    public void checkSupportsTransactions()
    {
        delegate.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return delegate.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        delegate.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        delegate.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        delegate.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return delegate.getValidWriteIds(tables, currentTransactionId);
    }

    private Set<HivePrivilegeInfo> loadTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(dbName, tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            AcidOperation operation,
            boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        try {
            delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
        }
        finally {
            invalidateTable(dbName, tableName);
        }
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        try {
            delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
        }
        finally {
            invalidatePartitionCache(dbName, tableName);
        }
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return delegate.functionExists(databaseName, functionName, signatureToken);
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return delegate.getAllFunctions(databaseName);
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return delegate.getFunctions(databaseName, functionName);
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        delegate.createFunction(databaseName, functionName, function);
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        delegate.replaceFunction(databaseName, functionName, function);
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        delegate.dropFunction(databaseName, functionName, signatureToken);
    }

    private static <K, V> LoadingCache<K, V> buildCache(
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMillis,
            Optional<Executor> refreshExecutor,
            long maximumSize,
            StatsRecording statsRecording,
            CacheLoader<K, V> cacheLoader)
    {
        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        checkArgument(refreshMillis.isEmpty() || refreshExecutor.isPresent(), "refreshMillis is provided but refreshExecutor is not");
        if (refreshMillis.isPresent() && (expiresAfterWriteMillis.isEmpty() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
            cacheLoader = asyncReloading(cacheLoader, refreshExecutor.orElseThrow(() -> new IllegalArgumentException("Executor not provided")));
        }
        cacheBuilder.maximumSize(maximumSize);
        if (statsRecording == StatsRecording.ENABLED) {
            cacheBuilder.recordStats();
        }
        cacheBuilder.shareNothingWhenDisabled();

        return cacheBuilder.build(cacheLoader);
    }

    private static <K, V> Cache<K, AtomicReference<V>> buildBulkCache(
            OptionalLong expiresAfterWriteMillis,
            long maximumSize,
            StatsRecording statsRecording)
    {
        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        // cannot use refreshAfterWrite since it can't use the bulk loading and causes too many requests

        cacheBuilder.maximumSize(maximumSize);
        if (statsRecording == StatsRecording.ENABLED) {
            cacheBuilder.recordStats();
        }
        cacheBuilder.shareNothingWhenDisabled();

        return cacheBuilder.build();
    }

    record UserTableKey(Optional<HivePrincipal> principal, String database, String table, Optional<String> owner)
    {
        UserTableKey
        {
            requireNonNull(principal, "principal is null");
            requireNonNull(database, "database is null");
            requireNonNull(table, "table is null");
            requireNonNull(owner, "owner is null");
        }

        public boolean matches(String databaseName, String tableName)
        {
            return this.database.equals(databaseName) && this.table.equals(tableName);
        }
    }

    //
    // Stats used for non-impersonation shared caching
    //

    @Managed
    @Nested
    public CacheStatsMBean getDatabaseStats()
    {
        return new CacheStatsMBean(databaseCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getDatabaseNamesStats()
    {
        return new CacheStatsMBean(databaseNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableStats()
    {
        return new CacheStatsMBean(tableCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableNamesStats()
    {
        return new CacheStatsMBean(tablesCacheNew);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableColumnStatisticsStats()
    {
        return new CacheStatsMBean(tableColumnStatisticsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionStatisticsStats()
    {
        return new CacheStatsMBean(partitionStatisticsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionStats()
    {
        return new CacheStatsMBean(partitionCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionFilterStats()
    {
        return new CacheStatsMBean(partitionFilterCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTablePrivilegesStats()
    {
        return new CacheStatsMBean(tablePrivilegesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getRolesStats()
    {
        return new CacheStatsMBean(rolesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getRoleGrantsStats()
    {
        return new CacheStatsMBean(roleGrantsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getConfigValuesStats()
    {
        return new CacheStatsMBean(configValuesCache);
    }

    //
    // Expose caches with ImpersonationCachingHiveMetastoreFactory so they can be aggregated
    //
    LoadingCache<String, Optional<Database>> getDatabaseCache()
    {
        return databaseCache;
    }

    LoadingCache<String, List<String>> getDatabaseNamesCache()
    {
        return databaseNamesCache;
    }

    LoadingCache<HiveTableName, Optional<Table>> getTableCache()
    {
        return tableCache;
    }

    public LoadingCache<String, List<TableInfo>> getTablesCacheNew()
    {
        return tablesCacheNew;
    }

    Cache<HiveTableName, AtomicReference<Map<String, HiveColumnStatistics>>> getTableColumnStatisticsCache()
    {
        return tableColumnStatisticsCache;
    }

    Cache<HivePartitionName, AtomicReference<Map<String, HiveColumnStatistics>>> getPartitionStatisticsCache()
    {
        return partitionStatisticsCache;
    }

    Cache<HivePartitionName, AtomicReference<Optional<Partition>>> getPartitionCache()
    {
        return partitionCache;
    }

    LoadingCache<PartitionFilter, Optional<List<String>>> getPartitionFilterCache()
    {
        return partitionFilterCache;
    }

    LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> getTablePrivilegesCache()
    {
        return tablePrivilegesCache;
    }

    LoadingCache<String, Set<String>> getRolesCache()
    {
        return rolesCache;
    }

    LoadingCache<HivePrincipal, Set<RoleGrant>> getRoleGrantsCache()
    {
        return roleGrantsCache;
    }

    LoadingCache<String, Optional<String>> getConfigValuesCache()
    {
        return configValuesCache;
    }

    private record CacheFactory(
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMillis,
            Optional<Executor> refreshExecutor,
            long maximumSize,
            StatsRecording statsRecording)
    {
        private static final CacheFactory NEVER_CACHE = new CacheFactory(OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), 0, StatsRecording.DISABLED);

        private CacheFactory(long maximumSize)
        {
            this(OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), maximumSize, StatsRecording.DISABLED);
        }

        private CacheFactory
        {
            requireNonNull(expiresAfterWriteMillis, "expiresAfterWriteMillis is null");
            checkArgument(expiresAfterWriteMillis.isEmpty() || expiresAfterWriteMillis.getAsLong() > 0, "expiresAfterWriteMillis must be empty or at least 1 millisecond");
            requireNonNull(refreshMillis, "refreshMillis is null");
            checkArgument(refreshMillis.isEmpty() || refreshMillis.getAsLong() > 0, "refreshMillis must be empty or at least 1 millisecond");
            requireNonNull(refreshExecutor, "refreshExecutor is null");
            requireNonNull(statsRecording, "statsRecording is null");
        }

        public <K, V> LoadingCache<K, V> buildCache(Function<K, V> loader)
        {
            return CachingHiveMetastore.buildCache(expiresAfterWriteMillis, refreshMillis, refreshExecutor, maximumSize, statsRecording, CacheLoader.from(loader::apply));
        }

        public <K, V> Cache<K, V> buildCache(BiFunction<K, V, V> loader)
        {
            CacheLoader<K, V> cacheLoader = new CacheLoader<>()
            {
                @Override
                public V load(K key)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ListenableFuture<V> reload(K key, V oldValue)
                {
                    requireNonNull(key);
                    requireNonNull(oldValue);
                    // async reloading is configured in CachingHiveMetastore.buildCache if refreshMillis is present
                    return immediateFuture(loader.apply(key, oldValue));
                }
            };
            return CachingHiveMetastore.buildCache(expiresAfterWriteMillis, refreshMillis, refreshExecutor, maximumSize, statsRecording, cacheLoader);
        }

        public <K, V> Cache<K, AtomicReference<V>> buildBulkCache()
        {
            // disable refresh since it can't use the bulk loading and causes too many requests
            return CachingHiveMetastore.buildBulkCache(expiresAfterWriteMillis, maximumSize, statsRecording);
        }
    }
}
