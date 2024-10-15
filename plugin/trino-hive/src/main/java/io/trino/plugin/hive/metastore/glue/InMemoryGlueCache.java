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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.SafeCaches;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.metastore.cache.ReentrantBoundedExecutor;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import jakarta.annotation.PreDestroy;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class InMemoryGlueCache
        implements GlueCache
{
    private enum Global
    {
        GLOBAL
    }

    private record PartitionKey(String databaseName, String tableName, PartitionName partitionName) {}

    private record PartitionNamesKey(String databaseName, String tableName, String glueFilterExpression) {}

    private record FunctionKey(String databaseName, String functionName) {}

    private final ExecutorService refreshExecutor;

    private final LoadingCache<Global, ValueHolder<List<String>>> databaseNamesCache;
    private final LoadingCache<String, ValueHolder<Optional<Database>>> databaseCache;
    private final LoadingCache<String, ValueHolder<List<TableInfo>>> tableNamesCache;
    private final LoadingCache<SchemaTableName, ValueHolder<Optional<Table>>> tableCache;
    private final LoadingCache<SchemaTableName, ColumnStatisticsHolder> tableColumnStatsCache;
    private final LoadingCache<PartitionNamesKey, ValueHolder<Set<PartitionName>>> partitionNamesCache;
    private final LoadingCache<PartitionKey, ValueHolder<Optional<Partition>>> partitionCache;
    private final LoadingCache<PartitionKey, ColumnStatisticsHolder> partitionColumnStatsCache;
    private final LoadingCache<String, ValueHolder<Collection<LanguageFunction>>> allFunctionsCache;
    private final LoadingCache<FunctionKey, ValueHolder<Collection<LanguageFunction>>> functionCache;

    private final AtomicLong databaseInvalidationCounter = new AtomicLong();
    private final AtomicLong tableInvalidationCounter = new AtomicLong();
    private final AtomicLong partitionInvalidationCounter = new AtomicLong();

    public InMemoryGlueCache(
            CatalogName catalogName,
            Duration metadataCacheTtl,
            Duration statsCacheTtl,
            Optional<Duration> refreshInterval,
            int maxMetastoreRefreshThreads,
            long maximumSize)
    {
        this.refreshExecutor = newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s"));
        Executor boundedRefreshExecutor = new ReentrantBoundedExecutor(refreshExecutor, maxMetastoreRefreshThreads);

        OptionalLong refreshMillis = refreshInterval.stream().mapToLong(Duration::toMillis).findAny();

        OptionalLong metadataCacheTtlMillis = OptionalLong.of(metadataCacheTtl.toMillis());
        this.databaseNamesCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.databaseCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.tableNamesCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.tableCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.partitionNamesCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.partitionCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.allFunctionsCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);
        this.functionCache = buildCache(metadataCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ValueHolder::new);

        OptionalLong statsCacheTtlMillis = OptionalLong.of(statsCacheTtl.toMillis());
        this.tableColumnStatsCache = buildCache(statsCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ColumnStatisticsHolder::new);
        this.partitionColumnStatsCache = buildCache(statsCacheTtlMillis, refreshMillis, boundedRefreshExecutor, maximumSize, ColumnStatisticsHolder::new);
    }

    @PreDestroy
    public void stop()
    {
        refreshExecutor.shutdownNow();
    }

    @Override
    public List<String> getDatabaseNames(Function<Consumer<Database>, List<String>> loader)
    {
        long invalidationCounter = databaseInvalidationCounter.get();
        return databaseNamesCache.getUnchecked(Global.GLOBAL).getValue(() -> loader.apply(database -> cacheDatabase(invalidationCounter, database)));
    }

    private void cacheDatabase(long invalidationCounter, Database database)
    {
        cacheValue(databaseCache, database.getDatabaseName(), Optional.of(database), () -> invalidationCounter == databaseInvalidationCounter.get());
    }

    @Override
    public void invalidateDatabase(String databaseName)
    {
        databaseInvalidationCounter.incrementAndGet();
        databaseCache.invalidate(databaseName);
        for (SchemaTableName schemaTableName : Sets.union(tableCache.asMap().keySet(), tableColumnStatsCache.asMap().keySet())) {
            if (schemaTableName.getSchemaName().equals(databaseName)) {
                invalidateTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), true);
            }
        }
        for (PartitionKey partitionKey : Sets.union(partitionCache.asMap().keySet(), partitionColumnStatsCache.asMap().keySet())) {
            if (partitionKey.databaseName().equals(databaseName)) {
                invalidatePartition(partitionKey);
            }
        }
        invalidateAllIf(partitionNamesCache, partitionNamesKey -> partitionNamesKey.databaseName().equals(databaseName));
        invalidateAllIf(functionCache, functionKey -> functionKey.databaseName().equals(databaseName));
        allFunctionsCache.invalidate(databaseName);
    }

    @Override
    public void invalidateDatabaseNames()
    {
        databaseNamesCache.invalidate(Global.GLOBAL);
    }

    @Override
    public Optional<Database> getDatabase(String databaseName, Supplier<Optional<Database>> loader)
    {
        return databaseCache.getUnchecked(databaseName).getValue(loader);
    }

    @Override
    public List<TableInfo> getTables(String databaseName, Function<Consumer<Table>, List<TableInfo>> loader)
    {
        long invalidationCounter = tableInvalidationCounter.get();
        return tableNamesCache.getUnchecked(databaseName).getValue(() -> loader.apply(table -> cacheTable(invalidationCounter, table)));
    }

    private void cacheTable(long invalidationCounter, Table table)
    {
        cacheValue(tableCache, table.getSchemaTableName(), Optional.of(table), () -> invalidationCounter == tableInvalidationCounter.get());
    }

    @Override
    public void invalidateTables(String databaseName)
    {
        tableNamesCache.invalidate(databaseName);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName, Supplier<Optional<Table>> loader)
    {
        return tableCache.getUnchecked(new SchemaTableName(databaseName, tableName)).getValue(loader);
    }

    @Override
    public void invalidateTable(String databaseName, String tableName, boolean cascade)
    {
        tableInvalidationCounter.incrementAndGet();
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        tableCache.invalidate(schemaTableName);
        tableColumnStatsCache.invalidate(schemaTableName);
        if (cascade) {
            for (PartitionKey partitionKey : Sets.union(partitionCache.asMap().keySet(), partitionColumnStatsCache.asMap().keySet())) {
                if (partitionKey.databaseName().equals(databaseName) && partitionKey.tableName().equals(tableName)) {
                    invalidatePartition(partitionKey);
                }
            }
            invalidatePartitionNames(databaseName, tableName);
        }
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
    {
        return tableColumnStatsCache.getUnchecked(new SchemaTableName(databaseName, tableName))
                .getColumnStatistics(columnNames, loader);
    }

    @Override
    public void invalidateTableColumnStatistics(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        tableColumnStatsCache.invalidate(schemaTableName);
    }

    @Override
    public Set<PartitionName> getPartitionNames(String databaseName, String tableName, String glueExpression, Function<Consumer<Partition>, Set<PartitionName>> loader)
    {
        long invalidationCounter = partitionInvalidationCounter.get();
        return partitionNamesCache.getUnchecked(new PartitionNamesKey(databaseName, tableName, glueExpression))
                .getValue(() -> loader.apply(partition -> cachePartition(invalidationCounter, partition)));
    }

    private void invalidatePartitionNames(String databaseName, String tableName)
    {
        invalidateAllIf(partitionNamesCache, partitionNamesKey -> partitionNamesKey.databaseName().equals(databaseName) && partitionNamesKey.tableName().equals(tableName));
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, PartitionName partitionName, Supplier<Optional<Partition>> loader)
    {
        return partitionCache.getUnchecked(new PartitionKey(databaseName, tableName, partitionName)).getValue(loader);
    }

    @Override
    public Collection<Partition> batchGetPartitions(
            String databaseName,
            String tableName,
            Collection<PartitionName> partitionNames,
            BiFunction<Consumer<Partition>, Collection<PartitionName>, Collection<Partition>> loader)
    {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        Set<PartitionName> missingPartitionNames = new HashSet<>();
        for (PartitionName partitionName : partitionNames) {
            ValueHolder<Optional<Partition>> valueHolder = partitionCache.getIfPresent(new PartitionKey(databaseName, tableName, partitionName));
            if (valueHolder != null) {
                Optional<Partition> partition = valueHolder.getValueIfPresent().flatMap(Function.identity());
                if (partition.isPresent()) {
                    partitions.add(partition.get());
                    continue;
                }
            }
            missingPartitionNames.add(partitionName);
        }
        if (!missingPartitionNames.isEmpty()) {
            // NOTE: loader is expected to directly insert the partitions into the cache, so there is no need to do it here
            long invalidationCounter = partitionInvalidationCounter.get();
            partitions.addAll(loader.apply(partition -> cachePartition(invalidationCounter, partition), missingPartitionNames));
        }
        return partitions.build();
    }

    private void cachePartition(long invalidationCounter, Partition partition)
    {
        PartitionKey partitionKey = new PartitionKey(partition.getDatabaseName(), partition.getTableName(), new PartitionName(partition.getValues()));
        cacheValue(partitionCache, partitionKey, Optional.of(partition), () -> invalidationCounter == partitionInvalidationCounter.get());
    }

    @Override
    public void invalidatePartition(String databaseName, String tableName, PartitionName partitionName)
    {
        invalidatePartition(new PartitionKey(databaseName, tableName, partitionName));
    }

    private void invalidatePartition(PartitionKey partitionKey)
    {
        partitionInvalidationCounter.incrementAndGet();
        partitionCache.invalidate(partitionKey);
        partitionColumnStatsCache.invalidate(partitionKey);
    }

    @Override
    public Map<String, HiveColumnStatistics> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            PartitionName partitionName,
            Set<String> columnNames,
            Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
    {
        return partitionColumnStatsCache.getUnchecked(new PartitionKey(databaseName, tableName, partitionName))
                .getColumnStatistics(columnNames, loader);
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName, Supplier<Collection<LanguageFunction>> loader)
    {
        return allFunctionsCache.getUnchecked(databaseName).getValue(loader);
    }

    @Override
    public Collection<LanguageFunction> getFunction(String databaseName, String functionName, Supplier<Collection<LanguageFunction>> loader)
    {
        return functionCache.getUnchecked(new FunctionKey(databaseName, functionName)).getValue(loader);
    }

    @Override
    public void invalidateFunction(String databaseName, String functionName)
    {
        functionCache.invalidate(new FunctionKey(databaseName, functionName));
        allFunctionsCache.invalidate(databaseName);
    }

    @Override
    public void flushCache()
    {
        databaseInvalidationCounter.incrementAndGet();
        tableInvalidationCounter.incrementAndGet();
        partitionInvalidationCounter.incrementAndGet();

        databaseNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableNamesCache.invalidateAll();
        tableCache.invalidateAll();
        tableColumnStatsCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionColumnStatsCache.invalidateAll();
        allFunctionsCache.invalidateAll();
        functionCache.invalidateAll();
    }

    @Managed
    @Nested
    public CacheStatsMBean getDatabaseNamesCacheStats()
    {
        return new CacheStatsMBean(databaseNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getDatabaseCacheStats()
    {
        return new CacheStatsMBean(databaseCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableNamesCacheStats()
    {
        return new CacheStatsMBean(tableNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableCacheStats()
    {
        return new CacheStatsMBean(tableCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableColumnStatsCacheStats()
    {
        return new CacheStatsMBean(tableColumnStatsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionNamesCacheStats()
    {
        return new CacheStatsMBean(partitionNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionCacheStats()
    {
        return new CacheStatsMBean(partitionCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionColumnStatsCacheStats()
    {
        return new CacheStatsMBean(partitionColumnStatsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getAllFunctionsCacheStats()
    {
        return new CacheStatsMBean(allFunctionsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getFunctionCacheStats()
    {
        return new CacheStatsMBean(functionCache);
    }

    @SuppressModernizer
    private static <K, V> LoadingCache<K, V> buildCache(
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMillis,
            Executor refreshExecutor,
            long maximumSize,
            Supplier<V> loader)
    {
        if (expiresAfterWriteMillis.isEmpty()) {
            return SafeCaches.emptyLoadingCache(CacheLoader.from(ignores -> loader.get()), true);
        }

        CacheLoader<? super K, V> cacheLoader = CacheLoader.from(loader::get);

        // this does not use EvictableCache because we want to inject values directly into the cache,
        // and we want a lock per key, instead of striped locks
        CacheBuilder<? super K, ? super V> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS)
                .maximumSize(maximumSize)
                .recordStats();

        if (refreshMillis.isPresent() && (expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
            cacheLoader = asyncReloading(cacheLoader, refreshExecutor);
        }

        return cacheBuilder.build(cacheLoader);
    }

    private static <K, V> void cacheValue(LoadingCache<K, ValueHolder<V>> cache, K key, V value, BooleanSupplier test)
    {
        // get the current value before checking the invalidation counter
        ValueHolder<V> valueHolder = cache.getUnchecked(key);
        if (!test.getAsBoolean()) {
            return;
        }
        // at this point, we know our value is ok to use in the value cache we fetched before the check
        valueHolder.tryOverwrite(value);
        // The value is updated, but Guava does not know the update happened, so the expiration time is not extended.
        // We need to replace the value in the cache to extend the expiration time iff this is still the latest value.
        cache.asMap().replace(key, valueHolder, valueHolder);
    }

    private static class ValueHolder<V>
    {
        private final Lock writeLock = new ReentrantLock();
        private volatile V value;

        public ValueHolder() {}

        public V getValue(Supplier<V> loader)
        {
            if (value == null) {
                writeLock.lock();
                try {
                    if (value == null) {
                        value = loader.get();
                        if (value == null) {
                            throw new IllegalStateException("Value loader returned null");
                        }
                    }
                }
                finally {
                    writeLock.unlock();
                }
            }
            return value;
        }

        public Optional<V> getValueIfPresent()
        {
            return Optional.ofNullable(value);
        }

        /**
         * Overwrite the value unless it is currently being loaded by another thread.
         */
        public void tryOverwrite(V value)
        {
            if (writeLock.tryLock()) {
                try {
                    this.value = value;
                }
                finally {
                    writeLock.unlock();
                }
            }
        }
    }

    private static class ColumnStatisticsHolder
    {
        private final Lock writeLock = new ReentrantLock();
        private final Map<String, Optional<HiveColumnStatistics>> cache = new ConcurrentHashMap<>();

        public Map<String, HiveColumnStatistics> getColumnStatistics(Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
        {
            Set<String> missingColumnNames = new HashSet<>();
            Map<String, HiveColumnStatistics> result = new ConcurrentHashMap<>();
            for (String columnName : columnNames) {
                Optional<HiveColumnStatistics> columnStatistics = cache.get(columnName);
                if (columnStatistics == null) {
                    missingColumnNames.add(columnName);
                }
                else {
                    columnStatistics.ifPresent(value -> result.put(columnName, value));
                }
            }
            if (!missingColumnNames.isEmpty()) {
                writeLock.lock();
                try {
                    Map<String, HiveColumnStatistics> loadedColumnStatistics = loader.apply(missingColumnNames);
                    for (String missingColumnName : missingColumnNames) {
                        HiveColumnStatistics value = loadedColumnStatistics.get(missingColumnName);
                        cache.put(missingColumnName, Optional.ofNullable(value));
                        if (value != null) {
                            result.put(missingColumnName, value);
                        }
                    }
                }
                finally {
                    writeLock.unlock();
                }
            }
            return result;
        }
    }
}
