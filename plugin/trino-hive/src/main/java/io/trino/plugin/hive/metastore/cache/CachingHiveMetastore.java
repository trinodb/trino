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
import io.trino.plugin.base.cache.EvictableLoadingCache;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
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
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Functions.identity;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreConfig.isCacheEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
    private final LoadingCache<WithIdentity<HiveTableName>, Optional<Table>> tableCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
    private final LoadingCache<TablesWithParameterCacheKey, List<String>> tablesWithParameterCache;
    private final LoadingCache<WithIdentity<HiveTableName>, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<String, List<String>> viewNamesCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, Optional<Partition>> partitionCache;
    private final LoadingCache<WithIdentity<PartitionFilter>, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final LoadingCache<String, Set<RoleGrant>> grantedPrincipalsCache;
    private final LoadingCache<String, Optional<String>> configValuesCache;

    public static CachingHiveMetastore cachingHiveMetastore(HiveMetastore delegate, Executor executor, CachingHiveMetastoreConfig config)
    {
        return cachingHiveMetastore(
                delegate,
                executor,
                config.getMetastoreCacheTtl(),
                config.getMetastoreRefreshInterval(),
                config.getMetastoreCacheMaximumSize());
    }

    public static CachingHiveMetastore cachingHiveMetastore(HiveMetastore delegate, Executor executor, Duration cacheTtl, Optional<Duration> refreshInterval, long maximumSize)
    {
        checkState(
                isCacheEnabled(cacheTtl, maximumSize),
                format("Invalid cache parameters (cacheTtl: %s, maxSize: %s)", cacheTtl, maximumSize));
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

    private Table getExistingTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        identity = updateIdentity(identity);
        return get(tableCache, new WithIdentity<>(identity, hiveTableName(databaseName, tableName)));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    private Optional<Table> loadTable(WithIdentity<HiveTableName> hiveTableName)
    {
        return delegate.getTable(hiveTableName.getIdentity(), hiveTableName.key.getDatabaseName(), hiveTableName.key.getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return get(tableStatisticsCache, new WithIdentity<>(updateIdentity(identity), hiveTableName(table.getDatabaseName(), table.getTableName())));
    }

    private PartitionStatistics loadTableColumnStatistics(WithIdentity<HiveTableName> hiveTableName)
    {
        HiveTableName tableName = hiveTableName.getKey();
        Table table = getExistingTable(hiveTableName.getIdentity(), tableName.getDatabaseName(), tableName.getTableName());
        return delegate.getTableStatistics(hiveTableName.getIdentity(), table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        HiveTableName hiveTableName = hiveTableName(table.getDatabaseName(), table.getTableName());
        List<WithIdentity<HivePartitionName>> partitionNames = partitions.stream()
                .map(partition -> new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName, makePartitionName(table, partition))))
                .collect(toImmutableList());
        Map<WithIdentity<HivePartitionName>, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitionNames);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(WithIdentity<HivePartitionName> partition)
    {
        HiveTableName tableName = partition.getKey().getHiveTableName();
        String partitionName = partition.getKey().getPartitionName().get();
        Table table = getExistingTable(partition.getIdentity(), tableName.getDatabaseName(), tableName.getTableName());
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                partition.getIdentity(),
                table,
                ImmutableList.of(getExistingPartition(partition.getIdentity(), table, partition.getKey().getPartitionValues())));
        return partitionStatistics.get(partitionName);
    }

    private Map<WithIdentity<HivePartitionName>, PartitionStatistics> loadPartitionsColumnStatistics(Iterable<? extends WithIdentity<HivePartitionName>> keys)
    {
        SetMultimap<WithIdentity<HiveTableName>, WithIdentity<HivePartitionName>> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(value -> new WithIdentity<>(value.getIdentity(), value.getKey().getHiveTableName()), Function.identity()));
        ImmutableMap.Builder<WithIdentity<HivePartitionName>, PartitionStatistics> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(tableName -> {
            Set<WithIdentity<HivePartitionName>> partitionNames = tablePartitions.get(tableName);
            Set<String> partitionNameStrings = partitionNames.stream()
                    .map(partitionName -> partitionName.getKey().getPartitionName().get())
                    .collect(toImmutableSet());
            Table table = getExistingTable(tableName.getIdentity(), tableName.getKey().getDatabaseName(), tableName.getKey().getTableName());
            List<Partition> partitions = getExistingPartitionsByNames(tableName.getIdentity(), table, ImmutableList.copyOf(partitionNameStrings));
            Map<String, PartitionStatistics> statisticsByPartitionName = delegate.getPartitionStatistics(tableName.getIdentity(), table, partitions);
            for (WithIdentity<HivePartitionName> partitionName : partitionNames) {
                String stringNameForPartition = partitionName.getKey().getPartitionName().get();
                result.put(partitionName, statisticsByPartitionName.get(stringNameForPartition));
            }
        });
        return result.buildOrThrow();
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        identity = updateIdentity(identity);
        try {
            delegate.updateTableStatistics(identity, databaseName, tableName, transaction, update);
        }
        finally {
            HiveTableName hiveTableName = hiveTableName(databaseName, tableName);
            tableStatisticsCache.invalidate(new WithIdentity<>(identity, hiveTableName));
            // basic stats are stored as table properties
            tableCache.invalidate(new WithIdentity<>(identity, hiveTableName));
        }
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        identity = updateIdentity(identity);
        try {
            delegate.updatePartitionStatistics(identity, table, partitionName, update);
        }
        finally {
            HivePartitionName hivePartitionName = hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName);
            partitionStatisticsCache.invalidate(new WithIdentity<>(identity, hivePartitionName));
            // basic stats are stored as partition properties
            partitionCache.invalidate(new WithIdentity<>(identity, hivePartitionName));
        }
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        try {
            delegate.updatePartitionStatistics(updateIdentity(identity), table, updates);
        }
        finally {
            HiveIdentity hiveIdentity = updateIdentity(identity);
            updates.forEach((partitionName, update) -> {
                HivePartitionName hivePartitionName = hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName);
                partitionStatisticsCache.invalidate(new WithIdentity<>(hiveIdentity, hivePartitionName));
                // basic stats are stored as partition properties
                partitionCache.invalidate(new WithIdentity<>(hiveIdentity, hivePartitionName));
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
    public void createDatabase(HiveIdentity identity, Database database)
    {
        identity = updateIdentity(identity);
        try {
            delegate.createDatabase(identity, database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName, boolean deleteData)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropDatabase(identity, databaseName, deleteData);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.renameDatabase(identity, databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    @Override
    public void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {
        identity = updateIdentity(identity);
        try {
            delegate.setDatabaseOwner(identity, databaseName, principal);
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
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        identity = updateIdentity(identity);
        try {
            delegate.createTable(identity, table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropTable(identity, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        identity = updateIdentity(identity);
        try {
            delegate.replaceTable(identity, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.renameTable(identity, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        identity = updateIdentity(identity);
        try {
            delegate.commentTable(identity, databaseName, tableName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void setTableOwner(HiveIdentity identity, String databaseName, String tableName, HivePrincipal principal)
    {
        identity = updateIdentity(identity);
        try {
            delegate.setTableOwner(identity, databaseName, tableName, principal);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void commentColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        identity = updateIdentity(identity);
        try {
            delegate.commentColumn(identity, databaseName, tableName, columnName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        identity = updateIdentity(identity);
        try {
            delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropColumn(identity, databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    protected void invalidateTable(String databaseName, String tableName)
    {
        invalidateTableCache(databaseName, tableName);
        tableNamesCache.invalidate(databaseName);
        viewNamesCache.invalidate(databaseName);
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        invalidateTableStatisticsCache(databaseName, tableName);
        invalidatePartitionCache(databaseName, tableName);
    }

    private void invalidateTableCache(String databaseName, String tableName)
    {
        tableCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private void invalidateTableStatisticsCache(String databaseName, String tableName)
    {
        tableStatisticsCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private Partition getExistingPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        return getPartition(identity, table, partitionValues)
                .orElseThrow(() -> new PartitionNotFoundException(table.getSchemaTableName(), partitionValues));
    }

    private List<Partition> getExistingPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        Map<String, Partition> partitions = getPartitionsByNames(identity, table, partitionNames).entrySet().stream()
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().orElseThrow(() ->
                        new PartitionNotFoundException(table.getSchemaTableName(), extractPartitionValues(entry.getKey())))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return partitionNames.stream()
                .map(partitions::get)
                .collect(toImmutableList());
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        return get(partitionCache, new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues)));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(HiveIdentity identity, String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return get(partitionFilterCache, new WithIdentity<>(updateIdentity(identity), partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter)));
    }

    private Optional<List<String>> loadPartitionNamesByFilter(WithIdentity<PartitionFilter> partitionFilter)
    {
        return delegate.getPartitionNamesByFilter(
                partitionFilter.getIdentity(),
                partitionFilter.getKey().getHiveTableName().getDatabaseName(),
                partitionFilter.getKey().getHiveTableName().getTableName(),
                partitionFilter.getKey().getPartitionColumnNames(),
                partitionFilter.getKey().getPartitionKeysFilter());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        List<WithIdentity<HivePartitionName>> names = partitionNames.stream()
                .map(name -> new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), name)))
                .collect(toImmutableList());

        Map<WithIdentity<HivePartitionName>, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<WithIdentity<HivePartitionName>, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getKey().getPartitionName().get(), entry.getValue());
        }
        return partitionsByName.buildOrThrow();
    }

    private Optional<Partition> loadPartitionByName(WithIdentity<HivePartitionName> partitionName)
    {
        HiveTableName hiveTableName = partitionName.getKey().getHiveTableName();
        return getTable(partitionName.getIdentity(), hiveTableName.getDatabaseName(), hiveTableName.getTableName())
                .flatMap(table -> delegate.getPartition(partitionName.getIdentity(), table, partitionName.getKey().getPartitionValues()));
    }

    private Map<WithIdentity<HivePartitionName>, Optional<Partition>> loadPartitionsByNames(Iterable<? extends WithIdentity<HivePartitionName>> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        WithIdentity<HivePartitionName> firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getKey().getHiveTableName();
        HiveIdentity identity = updateIdentity(firstPartition.getIdentity());
        Optional<Table> table = getTable(identity, hiveTableName.getDatabaseName(), hiveTableName.getTableName());
        if (table.isEmpty()) {
            return stream(partitionNames)
                    .collect(toImmutableMap(name -> name, name -> Optional.empty()));
        }

        List<String> partitionsToFetch = new ArrayList<>();
        for (WithIdentity<HivePartitionName> partitionName : partitionNames) {
            checkArgument(partitionName.getKey().getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getKey().getHiveTableName());
            checkArgument(identity.equals(partitionName.getIdentity()), "Expected identity %s but got %s", identity, partitionName.getIdentity());
            partitionsToFetch.add(partitionName.getKey().getPartitionName().get());
        }

        ImmutableMap.Builder<WithIdentity<HivePartitionName>, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(identity, table.get(), partitionsToFetch);
        for (WithIdentity<HivePartitionName> partitionName : partitionNames) {
            partitions.put(partitionName, partitionsByNames.getOrDefault(partitionName.getKey().getPartitionName().get(), Optional.empty()));
        }
        return partitions.buildOrThrow();
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        identity = updateIdentity(identity);
        try {
            delegate.addPartitions(identity, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropPartition(identity, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        identity = updateIdentity(identity);
        try {
            delegate.alterPartition(identity, databaseName, tableName, partition);
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

        Predicate<WithIdentity<HivePartitionName>> hivePartitionPredicate =
                partitionName -> partitionName.getKey().getHiveTableName().equals(hiveTableName) &&
                        partitionPredicate.test(partitionName.getKey().getPartitionName());

        partitionCache.asMap().keySet().stream()
                .filter(hivePartitionPredicate)
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getKey().getHiveTableName().equals(hiveTableName))
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
        // some callers of xxxxTablePrivileges use Optional.of(grantee), some Optional.empty() (to get all privileges), so have to invalidate them both
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
    public long openTransaction(HiveIdentity identity)
    {
        return delegate.openTransaction(identity);
    }

    @Override
    public void commitTransaction(HiveIdentity identity, long transactionId)
    {
        delegate.commitTransaction(identity, transactionId);
    }

    @Override
    public void abortTransaction(HiveIdentity identity, long transactionId)
    {
        delegate.abortTransaction(identity, transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(HiveIdentity identity, long transactionId)
    {
        delegate.sendTransactionHeartbeat(identity, transactionId);
    }

    @Override
    public void acquireSharedReadLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(identity, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId)
    {
        return delegate.getValidWriteIds(identity, tables, currentTransactionId);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }

    private Set<HivePrivilegeInfo> loadTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
    }

    @Override
    public long allocateWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(identity, dbName, tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(HiveIdentity identity, String queryId, long transactionId, String dbName, String tableName, DataOperationType operation, boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(identity, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        try {
            delegate.updateTableWriteId(identity, dbName, tableName, transactionId, writeId, rowCountChange);
        }
        finally {
            invalidateTable(dbName, tableName);
        }
    }

    @Override
    public void alterPartitions(HiveIdentity identity, String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        identity = updateIdentity(identity);
        try {
            delegate.alterPartitions(identity, dbName, tableName, partitions, writeId);
        }
        finally {
            invalidatePartitionCache(dbName, tableName);
        }
    }

    @Override
    public void addDynamicPartitions(HiveIdentity identity, String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        identity = updateIdentity(identity);
        try {
            delegate.addDynamicPartitions(identity, dbName, tableName, partitionNames, transactionId, writeId, operation);
        }
        finally {
            invalidatePartitionCache(dbName, tableName);
        }
    }

    @Override
    public void alterTransactionalTable(HiveIdentity identity, Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.alterTransactionalTable(identity, table, transactionId, writeId, principalPrivileges);
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

        checkArgument(refreshMillis.isEmpty() || refreshExecutor.isPresent(), "refreshMillis is provided but refreshExecutor is not");
        if (refreshMillis.isPresent() && (expiresAfterWriteMillis.isEmpty() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheLoader = asyncReloading(cacheLoader, refreshExecutor.orElseThrow(() -> new IllegalArgumentException("Executor not provided")));
        }
        else {
            refreshMillis = OptionalLong.empty();
        }

        return EvictableLoadingCache.build(
                expiresAfterWriteMillis,
                refreshMillis,
                maximumSize,
                statsRecording == StatsRecording.ENABLED,
                cacheLoader);
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

        return EvictableLoadingCache.build(
                expiresAfterWriteMillis,
                // cannot use refreshAfterWrite since it can't use the bulk loading and causes too many requests
                OptionalLong.empty(),
                maximumSize,
                statsRecording == StatsRecording.ENABLED,
                cacheLoader);
    }

    private static class WithIdentity<T>
    {
        private final HiveIdentity identity;
        private final T key;
        private final int hashCode;

        public WithIdentity(HiveIdentity identity, T key)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.key = requireNonNull(key, "key is null");
            this.hashCode = Objects.hash(identity, key);
        }

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        public T getKey()
        {
            return key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WithIdentity<?> other = (WithIdentity<?>) o;
            return hashCode == other.hashCode &&
                    Objects.equals(identity, other.identity) &&
                    Objects.equals(key, other.key);
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("identity", identity)
                    .add("key", key)
                    .toString();
        }
    }

    private HiveIdentity updateIdentity(HiveIdentity identity)
    {
        // remove identity if not doing impersonation
        return delegate.isImpersonationEnabled() ? identity : HiveIdentity.none();
    }

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
}
