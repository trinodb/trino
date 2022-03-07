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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.units.Duration;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePartitionName;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.HiveTableName;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionFilter;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TablesWithParameterCacheKey;
import io.trino.plugin.hive.metastore.UserTableKey;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.PartitionFilter.partitionFilter;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements HiveMetastore
{
    public enum StatsRecording
    {
        ENABLED,
        DISABLED
    }

    protected final HiveMetastore delegate;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<HiveTableName, Optional<Table>> tableCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
    private final LoadingCache<TablesWithParameterCacheKey, List<String>> tablesWithParameterCache;
    private final LoadingCache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<HivePartitionName, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<String, List<String>> viewNamesCache;
    private final LoadingCache<HivePartitionName, Optional<Partition>> partitionCache;
    private final LoadingCache<PartitionFilter, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final LoadingCache<String, Set<RoleGrant>> grantedPrincipalsCache;
    private final LoadingCache<String, Optional<String>> configValuesCache;

    public static CachingHiveMetastore cachingHiveMetastore(HiveMetastore delegate, Executor executor, Duration cacheTtl, Optional<Duration> refreshInterval, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval
                        .map(Duration::toMillis)
                        .map(OptionalLong::of)
                        .orElseGet(OptionalLong::empty),
                Optional.of(executor),
                maximumSize,
                StatsRecording.ENABLED);
    }

    public static CachingHiveMetastore memoizeMetastore(HiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                maximumSize,
                StatsRecording.DISABLED);
    }

    protected CachingHiveMetastore(HiveMetastore delegate, OptionalLong expiresAfterWriteMillis, OptionalLong refreshMills, Optional<Executor> executor, long maximumSize, StatsRecording statsRecording)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");

        databaseNamesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, ignored -> loadAllDatabases());

        databaseCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadDatabase);

        tableNamesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadAllTables);

        tablesWithParameterCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadTablesMatchingParameter);

        tableStatisticsCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadTableColumnStatistics);

        // disable refresh since it can't use the bulk loading and causes too many requests
        partitionStatisticsCache = buildCache(expiresAfterWriteMillis, maximumSize, statsRecording, this::loadPartitionColumnStatistics, this::loadPartitionsColumnStatistics);

        tableCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadTable);

        viewNamesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadAllViews);

        partitionFilterCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadPartitionNamesByFilter);

        // disable refresh since it can't use the bulk loading and causes too many requests
        partitionCache = buildCache(expiresAfterWriteMillis, maximumSize, statsRecording, this::loadPartitionByName, this::loadPartitionsByNames);

        tablePrivilegesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, key ->
                loadTablePrivileges(key.getDatabase(), key.getTable(), key.getOwner(), key.getPrincipal()));

        rolesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, ignored -> loadRoles());

        roleGrantsCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadRoleGrants);

        grantedPrincipalsCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadPrincipals);

        configValuesCache = buildCache(expiresAfterWriteMillis, refreshMills, executor, maximumSize, statsRecording, this::loadConfigValue);
    }

    @Managed
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        viewNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
        tablePrivilegesCache.invalidateAll();
        tableStatisticsCache.invalidateAll();
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

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return get(databaseCache, databaseName);
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

    private Table getExistingTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return get(tableCache, hiveTableName(databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    private Optional<Table> loadTable(HiveTableName hiveTableName)
    {
        return delegate.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return get(tableStatisticsCache, hiveTableName(table.getDatabaseName(), table.getTableName()));
    }

    private PartitionStatistics loadTableColumnStatistics(HiveTableName tableName)
    {
        Table table = getExistingTable(tableName.getDatabaseName(), tableName.getTableName());
        return delegate.getTableStatistics(table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        HiveTableName hiveTableName = hiveTableName(table.getDatabaseName(), table.getTableName());
        List<HivePartitionName> partitionNames = partitions.stream()
                .map(partition -> hivePartitionName(hiveTableName, makePartitionName(table, partition)))
                .collect(toImmutableList());
        Map<HivePartitionName, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitionNames);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getPartitionName().orElseThrow(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(HivePartitionName partition)
    {
        HiveTableName tableName = partition.getHiveTableName();
        String partitionName = partition.getPartitionName().orElseThrow();
        Table table = getExistingTable(tableName.getDatabaseName(), tableName.getTableName());
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                table,
                ImmutableList.of(getExistingPartition(table, partition.getPartitionValues())));
        return partitionStatistics.get(partitionName);
    }

    private Map<HivePartitionName, PartitionStatistics> loadPartitionsColumnStatistics(Iterable<? extends HivePartitionName> keys)
    {
        SetMultimap<HiveTableName, HivePartitionName> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(HivePartitionName::getHiveTableName, Function.identity()));
        ImmutableMap.Builder<HivePartitionName, PartitionStatistics> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(tableName -> {
            Set<HivePartitionName> partitionNames = tablePartitions.get(tableName);
            Set<String> partitionNameStrings = partitionNames.stream()
                    .map(partitionName -> partitionName.getPartitionName().orElseThrow())
                    .collect(toImmutableSet());
            Table table = getExistingTable(tableName.getDatabaseName(), tableName.getTableName());
            List<Partition> partitions = getExistingPartitionsByNames(table, ImmutableList.copyOf(partitionNameStrings));
            Map<String, PartitionStatistics> statisticsByPartitionName = delegate.getPartitionStatistics(table, partitions);
            for (HivePartitionName partitionName : partitionNames) {
                String stringNameForPartition = partitionName.getPartitionName().orElseThrow();
                result.put(partitionName, statisticsByPartitionName.get(stringNameForPartition));
            }
        });
        return result.buildOrThrow();
    }

    @Override
    public void updateTableStatistics(String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updateTableStatistics(databaseName, tableName, transaction, update);
        }
        finally {
            HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
            tableStatisticsCache.invalidate(hiveTableName);
            // basic stats are stored as table properties
            tableCache.invalidate(hiveTableName);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updatePartitionStatistics(table, partitionName, update);
        }
        finally {
            HivePartitionName hivePartitionName = hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName);
            partitionStatisticsCache.invalidate(hivePartitionName);
            // basic stats are stored as partition properties
            partitionCache.invalidate(hivePartitionName);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        try {
            delegate.updatePartitionStatistics(table, updates);
        }
        finally {
            updates.forEach((partitionName, update) -> {
                HivePartitionName hivePartitionName = hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName);
                partitionStatisticsCache.invalidate(hivePartitionName);
                // basic stats are stored as partition properties
                partitionCache.invalidate(hivePartitionName);
            });
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return get(tableNamesCache, databaseName);
    }

    private List<String> loadAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        TablesWithParameterCacheKey key = new TablesWithParameterCacheKey(databaseName, parameterKey, parameterValue);
        return get(tablesWithParameterCache, key);
    }

    private List<String> loadTablesMatchingParameter(TablesWithParameterCacheKey key)
    {
        return delegate.getTablesWithParameter(key.getDatabaseName(), key.getParameterKey(), key.getParameterValue());
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return get(viewNamesCache, databaseName);
    }

    private List<String> loadAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
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

    protected void invalidateDatabase(String databaseName)
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
        invalidateTableCache(databaseName, tableName);
        tableNamesCache.invalidate(databaseName);
        viewNamesCache.invalidate(databaseName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        invalidateTableStatisticsCache(databaseName, tableName);
        invalidateTablesWithParameterCache(databaseName, tableName);
        invalidatePartitionCache(databaseName, tableName);
    }

    private void invalidateTableCache(String databaseName, String tableName)
    {
        tableCache.asMap().keySet().stream()
                .filter(table -> table.getDatabaseName().equals(databaseName) && table.getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private void invalidateTableStatisticsCache(String databaseName, String tableName)
    {
        tableStatisticsCache.asMap().keySet().stream()
                .filter(table -> table.getDatabaseName().equals(databaseName) && table.getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private void invalidateTablesWithParameterCache(String databaseName, String tableName)
    {
        tablesWithParameterCache.asMap().keySet().stream()
                .filter(cacheKey -> cacheKey.getDatabaseName().equals(databaseName))
                .filter(cacheKey -> {
                    List<String> cacheValue = tablesWithParameterCache.getIfPresent(cacheKey);
                    return cacheValue != null && cacheValue.contains(tableName);
                })
                .forEach(tablesWithParameterCache::invalidate);
    }

    private Partition getExistingPartition(Table table, List<String> partitionValues)
    {
        return getPartition(table, partitionValues)
                .orElseThrow(() -> new PartitionNotFoundException(table.getSchemaTableName(), partitionValues));
    }

    private List<Partition> getExistingPartitionsByNames(Table table, List<String> partitionNames)
    {
        Map<String, Partition> partitions = getPartitionsByNames(table, partitionNames).entrySet().stream()
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().orElseThrow(() ->
                        new PartitionNotFoundException(table.getSchemaTableName(), extractPartitionValues(entry.getKey())))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return partitionNames.stream()
                .map(partitions::get)
                .collect(toImmutableList());
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return get(partitionCache, hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return get(partitionFilterCache, partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter));
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

        Map<HivePartitionName, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<HivePartitionName, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionName().orElseThrow(), entry.getValue());
        }
        return partitionsByName.buildOrThrow();
    }

    private Optional<Partition> loadPartitionByName(HivePartitionName partitionName)
    {
        HiveTableName hiveTableName = partitionName.getHiveTableName();
        return getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName())
                .flatMap(table -> delegate.getPartition(table, partitionName.getPartitionValues()));
    }

    private Map<HivePartitionName, Optional<Partition>> loadPartitionsByNames(Iterable<? extends HivePartitionName> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        HivePartitionName firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getHiveTableName();
        Optional<Table> table = getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
        if (table.isEmpty()) {
            return stream(partitionNames)
                    .collect(toImmutableMap(name -> name, name -> Optional.empty()));
        }

        List<String> partitionsToFetch = new ArrayList<>();
        for (HivePartitionName partitionName : partitionNames) {
            checkArgument(partitionName.getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getHiveTableName());
            partitionsToFetch.add(partitionName.getPartitionName().orElseThrow());
        }

        ImmutableMap.Builder<HivePartitionName, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(table.get(), partitionsToFetch);
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
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return get(grantedPrincipalsCache, role);
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

    private Set<RoleGrant> loadPrincipals(String role)
    {
        return delegate.listGrantedPrincipals(role);
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

        partitionCache.asMap().keySet().stream()
                .filter(hivePartitionPredicate)
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
        partitionStatisticsCache.asMap().keySet().stream()
                .filter(hivePartitionPredicate)
                .forEach(partitionStatisticsCache::invalidate);
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
        return get(configValuesCache, name);
    }

    private Optional<String> loadConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    @Override
    public long openTransaction()
    {
        return delegate.openTransaction();
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
    public void acquireSharedReadLock(String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(queryId, transactionId, fullTables, partitions);
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
    public void acquireTableWriteLock(String queryId,
            long transactionId,
            String dbName,
            String tableName,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
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
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        try {
            delegate.alterPartitions(dbName, tableName, partitions, writeId);
        }
        finally {
            invalidatePartitionCache(dbName, tableName);
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

    private static <K, V> LoadingCache<K, V> buildCache(
            OptionalLong expiresAfterWriteMillis,
            OptionalLong refreshMillis,
            Optional<Executor> refreshExecutor,
            long maximumSize,
            StatsRecording statsRecording,
            com.google.common.base.Function<K, V> loader)
    {
        CacheLoader<K, V> cacheLoader = CacheLoader.from(loader);
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

        return cacheBuilder.build(cacheLoader);
    }

    private static <K, V> LoadingCache<K, V> buildCache(
            OptionalLong expiresAfterWriteMillis,
            long maximumSize,
            StatsRecording statsRecording,
            Function<K, V> loader,
            Function<Iterable<K>, Map<K, V>> bulkLoader)
    {
        requireNonNull(loader, "loader is null");
        requireNonNull(bulkLoader, "bulkLoader is null");
        CacheLoader<K, V> cacheLoader = new CacheLoader<>()
        {
            @Override
            public V load(K key)
            {
                return loader.apply(key);
            }

            @Override
            public Map<K, V> loadAll(Iterable<? extends K> keys)
            {
                return bulkLoader.apply(Iterables.transform(keys, identity()));
            }
        };

        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        // cannot use refreshAfterWrite since it can't use the bulk loading and causes too many requests

        cacheBuilder.maximumSize(maximumSize);
        if (statsRecording == StatsRecording.ENABLED) {
            cacheBuilder.recordStats();
        }

        return cacheBuilder.build(cacheLoader);
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
        return new CacheStatsMBean(tableNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableWithParameterStats()
    {
        return new CacheStatsMBean(tablesWithParameterCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableStatisticsStats()
    {
        return new CacheStatsMBean(tableStatisticsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPartitionStatisticsStats()
    {
        return new CacheStatsMBean(partitionStatisticsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getViewNamesStats()
    {
        return new CacheStatsMBean(viewNamesCache);
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
    public CacheStatsMBean getGrantedPrincipalsStats()
    {
        return new CacheStatsMBean(grantedPrincipalsCache);
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

    LoadingCache<String, List<String>> getTableNamesCache()
    {
        return tableNamesCache;
    }

    LoadingCache<TablesWithParameterCacheKey, List<String>> getTablesWithParameterCache()
    {
        return tablesWithParameterCache;
    }

    LoadingCache<HiveTableName, PartitionStatistics> getTableStatisticsCache()
    {
        return tableStatisticsCache;
    }

    LoadingCache<HivePartitionName, PartitionStatistics> getPartitionStatisticsCache()
    {
        return partitionStatisticsCache;
    }

    LoadingCache<String, List<String>> getViewNamesCache()
    {
        return viewNamesCache;
    }

    LoadingCache<HivePartitionName, Optional<Partition>> getPartitionCache()
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

    LoadingCache<String, Set<RoleGrant>> getGrantedPrincipalsCache()
    {
        return grantedPrincipalsCache;
    }

    LoadingCache<String, Optional<String>> getConfigValuesCache()
    {
        return configValuesCache;
    }
}
