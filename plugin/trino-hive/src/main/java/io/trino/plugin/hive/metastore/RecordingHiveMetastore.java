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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.plugin.hive.ForRecordingHiveMetastore;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.trino.plugin.hive.metastore.PartitionFilter.partitionFilter;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RecordingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore delegate;
    private final JsonCodec<Recording> recordingCodec;
    private final Path recordingPath;
    private final boolean replay;

    private volatile Optional<List<String>> allDatabases = Optional.empty();
    private volatile Optional<Set<String>> allRoles = Optional.empty();

    private final Cache<String, Optional<Database>> databaseCache;
    private final Cache<HiveTableName, Optional<Table>> tableCache;
    private final Cache<String, Set<ColumnStatisticType>> supportedColumnStatisticsCache;
    private final Cache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final Cache<Set<HivePartitionName>, Map<String, PartitionStatistics>> partitionStatisticsCache;
    private final Cache<String, List<String>> allTablesCache;
    private final Cache<TablesWithParameterCacheKey, List<String>> tablesWithParameterCache;
    private final Cache<String, List<String>> allViewsCache;
    private final Cache<HivePartitionName, Optional<Partition>> partitionCache;
    private final Cache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final Cache<PartitionFilter, Optional<List<String>>> partitionNamesByPartsCache;
    private final Cache<Set<HivePartitionName>, Map<String, Optional<Partition>>> partitionsByNamesCache;
    private final Cache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final Cache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final Cache<String, Set<RoleGrant>> grantedPrincipalsCache;

    @Inject
    public RecordingHiveMetastore(@ForRecordingHiveMetastore HiveMetastore delegate, RecordingMetastoreConfig config, JsonCodec<RecordingHiveMetastore.Recording> recordingCodec)
            throws IOException
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.recordingCodec = recordingCodec;
        requireNonNull(config, "config is null");
        this.recordingPath = Paths.get(requireNonNull(config.getRecordingPath(), "recordingPath is null"));
        this.replay = config.isReplay();

        Duration recordingDuration = config.getRecordingDuration();
        databaseCache = createCache(replay, recordingDuration);
        tableCache = createCache(replay, recordingDuration);
        supportedColumnStatisticsCache = createCache(replay, recordingDuration);
        tableStatisticsCache = createCache(replay, recordingDuration);
        partitionStatisticsCache = createCache(replay, recordingDuration);
        allTablesCache = createCache(replay, recordingDuration);
        tablesWithParameterCache = createCache(replay, recordingDuration);
        allViewsCache = createCache(replay, recordingDuration);
        partitionCache = createCache(replay, recordingDuration);
        partitionNamesCache = createCache(replay, recordingDuration);
        partitionNamesByPartsCache = createCache(replay, recordingDuration);
        partitionsByNamesCache = createCache(replay, recordingDuration);
        tablePrivilegesCache = createCache(replay, recordingDuration);
        roleGrantsCache = createCache(replay, recordingDuration);
        grantedPrincipalsCache = createCache(replay, recordingDuration);

        if (replay) {
            loadRecording();
        }
    }

    @VisibleForTesting
    void loadRecording()
            throws IOException
    {
        Recording recording = recordingCodec.fromJson(readAllBytes(recordingPath));

        allDatabases = recording.getAllDatabases();
        allRoles = recording.getAllRoles();
        databaseCache.putAll(toMap(recording.getDatabases()));
        tableCache.putAll(toMap(recording.getTables()));
        supportedColumnStatisticsCache.putAll(toMap(recording.getSupportedColumnStatistics()));
        tableStatisticsCache.putAll(toMap(recording.getTableStatistics()));
        partitionStatisticsCache.putAll(toMap(recording.getPartitionStatistics()));
        allTablesCache.putAll(toMap(recording.getAllTables()));
        tablesWithParameterCache.putAll(toMap(recording.getTablesWithParameter()));
        allViewsCache.putAll(toMap(recording.getAllViews()));
        partitionCache.putAll(toMap(recording.getPartitions()));
        partitionNamesCache.putAll(toMap(recording.getPartitionNames()));
        partitionNamesByPartsCache.putAll(toMap(recording.getPartitionNamesByParts()));
        partitionsByNamesCache.putAll(toMap(recording.getPartitionsByNames()));
        tablePrivilegesCache.putAll(toMap(recording.getTablePrivileges()));
        roleGrantsCache.putAll(toMap(recording.getRoleGrants()));
        grantedPrincipalsCache.putAll(toMap(recording.getGrantedPrincipals()));
    }

    private static <K, V> Cache<K, V> createCache(boolean reply, Duration recordingDuration)
    {
        if (reply) {
            return CacheBuilder.newBuilder()
                    .build();
        }

        return CacheBuilder.newBuilder()
                .expireAfterWrite(recordingDuration.toMillis(), MILLISECONDS)
                .build();
    }

    @Managed
    public void writeRecording()
            throws IOException
    {
        if (replay) {
            throw new IllegalStateException("Cannot write recording in replay mode");
        }

        Recording recording = new Recording(
                allDatabases,
                allRoles,
                toPairs(databaseCache),
                toPairs(tableCache),
                toPairs(supportedColumnStatisticsCache),
                toPairs(tableStatisticsCache),
                toPairs(partitionStatisticsCache),
                toPairs(allTablesCache),
                toPairs(tablesWithParameterCache),
                toPairs(allViewsCache),
                toPairs(partitionCache),
                toPairs(partitionNamesCache),
                toPairs(partitionNamesByPartsCache),
                toPairs(partitionsByNamesCache),
                toPairs(tablePrivilegesCache),
                toPairs(roleGrantsCache),
                toPairs(grantedPrincipalsCache));

        Files.write(recordingPath, recordingCodec.toJsonBytes(recording));
    }

    private static <K, V> Map<K, V> toMap(List<Pair<K, V>> pairs)
    {
        return pairs.stream()
                .collect(toImmutableMap(Pair::getKey, Pair::getValue));
    }

    private static <K, V> List<Pair<K, V>> toPairs(Cache<K, V> cache)
    {
        return cache.asMap().entrySet().stream()
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return loadValue(databaseCache, databaseName, () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        if (replay) {
            return allDatabases.orElseThrow(() -> new TrinoException(NOT_FOUND, "Missing entry for all databases"));
        }

        List<String> result = delegate.getAllDatabases();
        allDatabases = Optional.of(result);
        return result;
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return loadValue(tableCache, hiveTableName(databaseName, tableName), () -> delegate.getTable(identity, databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return loadValue(supportedColumnStatisticsCache, type.getTypeSignature().toString(), () -> delegate.getSupportedColumnStatistics(type));
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return loadValue(
                tableStatisticsCache,
                hiveTableName(table.getDatabaseName(), table.getTableName()),
                () -> delegate.getTableStatistics(identity, table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return loadValue(
                partitionStatisticsCache,
                partitions.stream()
                        .map(partition -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partition.getValues()))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionStatistics(identity, table, partitions));
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updateTableStatistics(identity, databaseName, tableName, transaction, update);
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(identity, table, partitionName, update);
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(identity, table, updates);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return loadValue(allTablesCache, databaseName, () -> delegate.getAllTables(databaseName));
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        TablesWithParameterCacheKey key = new TablesWithParameterCacheKey(databaseName, parameterKey, parameterValue);
        return loadValue(tablesWithParameterCache, key, () -> delegate.getTablesWithParameter(databaseName, parameterKey, parameterValue));
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return loadValue(allViewsCache, databaseName, () -> delegate.getAllViews(databaseName));
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        verifyRecordingMode();
        delegate.createDatabase(identity, database);
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropDatabase(identity, databaseName, deleteData);
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        verifyRecordingMode();
        delegate.renameDatabase(identity, databaseName, newDatabaseName);
    }

    @Override
    public void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {
        verifyRecordingMode();
        delegate.setDatabaseOwner(identity, databaseName, principal);
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.createTable(identity, table, principalPrivileges);
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropTable(identity, databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.replaceTable(identity, databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        verifyRecordingMode();
        delegate.renameTable(identity, databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        verifyRecordingMode();
        delegate.commentTable(identity, databaseName, tableName, comment);
    }

    @Override
    public void setTableOwner(HiveIdentity identity, String databaseName, String tableName, HivePrincipal principal)
    {
        verifyRecordingMode();
        delegate.setTableOwner(identity, databaseName, tableName, principal);
    }

    @Override
    public void commentColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        verifyRecordingMode();
        delegate.commentColumn(identity, databaseName, tableName, columnName, comment);
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        verifyRecordingMode();
        delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        verifyRecordingMode();
        delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        verifyRecordingMode();
        delegate.dropColumn(identity, databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        return loadValue(
                partitionCache,
                hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues),
                () -> delegate.getPartition(identity, table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(HiveIdentity identity, String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return loadValue(
                partitionNamesByPartsCache,
                partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter),
                () -> delegate.getPartitionNamesByFilter(identity, databaseName, tableName, columnNames, partitionKeysFilter));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        return loadValue(
                partitionsByNamesCache,
                partitionNames.stream()
                        .map(partitionName -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionsByNames(identity, table, partitionNames));
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        verifyRecordingMode();
        delegate.addPartitions(identity, databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropPartition(identity, databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        verifyRecordingMode();
        delegate.alterPartition(identity, databaseName, tableName, partition);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return loadValue(
                tablePrivilegesCache,
                new UserTableKey(principal, databaseName, tableName, tableOwner),
                () -> delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        verifyRecordingMode();
        delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        verifyRecordingMode();
        delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        verifyRecordingMode();
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        verifyRecordingMode();
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        if (replay) {
            return allRoles.orElseThrow(() -> new TrinoException(NOT_FOUND, "Missing entry for roles"));
        }

        Set<String> result = delegate.listRoles();
        allRoles = Optional.of(result);
        return result;
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        verifyRecordingMode();
        delegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        verifyRecordingMode();
        delegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return loadValue(
                grantedPrincipalsCache,
                role,
                () -> delegate.listGrantedPrincipals(role));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return loadValue(
                roleGrantsCache,
                principal,
                () -> delegate.listRoleGrants(principal));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }

    private <K, V> V loadValue(Cache<K, V> cache, K key, Supplier<V> valueSupplier)
    {
        if (replay) {
            return Optional.ofNullable(cache.getIfPresent(key))
                    .orElseThrow(() -> new TrinoException(NOT_FOUND, "Missing entry found for key: " + key));
        }

        V value = valueSupplier.get();
        cache.put(key, value);
        return value;
    }

    private void verifyRecordingMode()
    {
        if (replay) {
            throw new IllegalStateException("Cannot perform Metastore updates in replay mode");
        }
    }

    @Immutable
    public static class Recording
    {
        private final Optional<List<String>> allDatabases;
        private final Optional<Set<String>> allRoles;
        private final List<Pair<String, Optional<Database>>> databases;
        private final List<Pair<HiveTableName, Optional<Table>>> tables;
        private final List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics;
        private final List<Pair<HiveTableName, PartitionStatistics>> tableStatistics;
        private final List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics;
        private final List<Pair<String, List<String>>> allTables;
        private final List<Pair<TablesWithParameterCacheKey, List<String>>> tablesWithParameter;
        private final List<Pair<String, List<String>>> allViews;
        private final List<Pair<HivePartitionName, Optional<Partition>>> partitions;
        private final List<Pair<HiveTableName, Optional<List<String>>>> partitionNames;
        private final List<Pair<PartitionFilter, Optional<List<String>>>> partitionNamesByParts;
        private final List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames;
        private final List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges;
        private final List<Pair<HivePrincipal, Set<RoleGrant>>> roleGrants;
        private final List<Pair<String, Set<RoleGrant>>> grantedPrincipals;

        @JsonCreator
        public Recording(
                @JsonProperty("allDatabases") Optional<List<String>> allDatabases,
                @JsonProperty("allRoles") Optional<Set<String>> allRoles,
                @JsonProperty("databases") List<Pair<String, Optional<Database>>> databases,
                @JsonProperty("tables") List<Pair<HiveTableName, Optional<Table>>> tables,
                @JsonProperty("supportedColumnStatistics") List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics,
                @JsonProperty("tableStatistics") List<Pair<HiveTableName, PartitionStatistics>> tableStatistics,
                @JsonProperty("partitionStatistics") List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics,
                @JsonProperty("allTables") List<Pair<String, List<String>>> allTables,
                @JsonProperty("tablesWithParameter") List<Pair<TablesWithParameterCacheKey, List<String>>> tablesWithParameter,
                @JsonProperty("allViews") List<Pair<String, List<String>>> allViews,
                @JsonProperty("partitions") List<Pair<HivePartitionName, Optional<Partition>>> partitions,
                @JsonProperty("partitionNames") List<Pair<HiveTableName, Optional<List<String>>>> partitionNames,
                @JsonProperty("partitionNamesByParts") List<Pair<PartitionFilter, Optional<List<String>>>> partitionNamesByParts,
                @JsonProperty("partitionsByNames") List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames,
                @JsonProperty("tablePrivileges") List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges,
                @JsonProperty("roleGrants") List<Pair<HivePrincipal, Set<RoleGrant>>> roleGrants,
                @JsonProperty("grantedPrincipals") List<Pair<String, Set<RoleGrant>>> grantedPrincipals)
        {
            this.allDatabases = allDatabases;
            this.allRoles = allRoles;
            this.databases = databases;
            this.tables = tables;
            this.supportedColumnStatistics = supportedColumnStatistics;
            this.tableStatistics = tableStatistics;
            this.partitionStatistics = partitionStatistics;
            this.allTables = allTables;
            this.tablesWithParameter = tablesWithParameter;
            this.allViews = allViews;
            this.partitions = partitions;
            this.partitionNames = partitionNames;
            this.partitionNamesByParts = partitionNamesByParts;
            this.partitionsByNames = partitionsByNames;
            this.tablePrivileges = tablePrivileges;
            this.roleGrants = roleGrants;
            this.grantedPrincipals = grantedPrincipals;
        }

        @JsonProperty
        public Optional<List<String>> getAllDatabases()
        {
            return allDatabases;
        }

        @JsonProperty
        public Optional<Set<String>> getAllRoles()
        {
            return allRoles;
        }

        @JsonProperty
        public List<Pair<String, Optional<Database>>> getDatabases()
        {
            return databases;
        }

        @JsonProperty
        public List<Pair<HiveTableName, Optional<Table>>> getTables()
        {
            return tables;
        }

        @JsonProperty
        public List<Pair<TablesWithParameterCacheKey, List<String>>> getTablesWithParameter()
        {
            return tablesWithParameter;
        }

        @JsonProperty
        public List<Pair<String, Set<ColumnStatisticType>>> getSupportedColumnStatistics()
        {
            return supportedColumnStatistics;
        }

        @JsonProperty
        public List<Pair<HiveTableName, PartitionStatistics>> getTableStatistics()
        {
            return tableStatistics;
        }

        @JsonProperty
        public List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> getPartitionStatistics()
        {
            return partitionStatistics;
        }

        @JsonProperty
        public List<Pair<String, List<String>>> getAllTables()
        {
            return allTables;
        }

        @JsonProperty
        public List<Pair<String, List<String>>> getAllViews()
        {
            return allViews;
        }

        @JsonProperty
        public List<Pair<HivePartitionName, Optional<Partition>>> getPartitions()
        {
            return partitions;
        }

        @JsonProperty
        public List<Pair<HiveTableName, Optional<List<String>>>> getPartitionNames()
        {
            return partitionNames;
        }

        @JsonProperty
        public List<Pair<PartitionFilter, Optional<List<String>>>> getPartitionNamesByParts()
        {
            return partitionNamesByParts;
        }

        @JsonProperty
        public List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> getPartitionsByNames()
        {
            return partitionsByNames;
        }

        @JsonProperty
        public List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> getTablePrivileges()
        {
            return tablePrivileges;
        }

        @JsonProperty
        public List<Pair<String, Set<RoleGrant>>> getGrantedPrincipals()
        {
            return grantedPrincipals;
        }

        @JsonProperty
        public List<Pair<HivePrincipal, Set<RoleGrant>>> getRoleGrants()
        {
            return roleGrants;
        }
    }

    @Immutable
    public static class Pair<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public Pair(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }
    }
}
