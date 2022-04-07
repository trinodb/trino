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
package io.trino.plugin.hive.metastore.recording;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePartitionName;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HiveTableName;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionFilter;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TablesWithParameterCacheKey;
import io.trino.plugin.hive.metastore.UserTableKey;
import io.trino.spi.TrinoException;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
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
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HiveMetastoreRecording
{
    private final JsonCodec<Recording> recordingCodec;
    private final Path recordingPath;
    private final boolean replay;

    private volatile Optional<List<String>> allDatabases = Optional.empty();
    private volatile Optional<Set<String>> allRoles = Optional.empty();
    private final NonEvictableCache<String, Optional<Database>> databaseCache;
    private final NonEvictableCache<HiveTableName, Optional<Table>> tableCache;
    private final NonEvictableCache<String, Set<ColumnStatisticType>> supportedColumnStatisticsCache;
    private final NonEvictableCache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final NonEvictableCache<Set<HivePartitionName>, Map<String, PartitionStatistics>> partitionStatisticsCache;
    private final NonEvictableCache<String, List<String>> allTablesCache;
    private final NonEvictableCache<TablesWithParameterCacheKey, List<String>> tablesWithParameterCache;
    private final NonEvictableCache<String, List<String>> allViewsCache;
    private final NonEvictableCache<HivePartitionName, Optional<Partition>> partitionCache;
    private final NonEvictableCache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final NonEvictableCache<PartitionFilter, Optional<List<String>>> partitionNamesByPartsCache;
    private final NonEvictableCache<Set<HivePartitionName>, Map<String, Optional<Partition>>> partitionsByNamesCache;
    private final NonEvictableCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final NonEvictableCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final NonEvictableCache<String, Set<RoleGrant>> grantedPrincipalsCache;

    @Inject
    public HiveMetastoreRecording(RecordingMetastoreConfig config, JsonCodec<Recording> recordingCodec)
            throws IOException
    {
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
        Recording recording;
        try (GZIPInputStream inputStream = new GZIPInputStream(Files.newInputStream(recordingPath))) {
            recording = recordingCodec.fromJson(inputStream.readAllBytes());
        }

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

    public boolean isReplay()
    {
        return replay;
    }

    public Optional<Database> getDatabase(String databaseName, Supplier<Optional<Database>> valueSupplier)
    {
        return loadValue(databaseCache, databaseName, valueSupplier);
    }

    public List<String> getAllDatabases(Supplier<List<String>> valueSupplier)
    {
        if (replay) {
            return allDatabases.orElseThrow(() -> new TrinoException(NOT_FOUND, "Missing entry for all databases"));
        }

        List<String> result = valueSupplier.get();
        allDatabases = Optional.of(result);
        return result;
    }

    public Optional<Table> getTable(HiveTableName hiveTableName, Supplier<Optional<Table>> valueSupplier)
    {
        return loadValue(tableCache, hiveTableName, valueSupplier);
    }

    public Set<ColumnStatisticType> getSupportedColumnStatistics(String type, Supplier<Set<ColumnStatisticType>> valueSupplier)
    {
        return loadValue(supportedColumnStatisticsCache, type, valueSupplier);
    }

    public PartitionStatistics getTableStatistics(HiveTableName hiveTableName, Supplier<PartitionStatistics> valueSupplier)
    {
        return loadValue(tableStatisticsCache, hiveTableName, valueSupplier);
    }

    public Map<String, PartitionStatistics> getPartitionStatistics(Set<HivePartitionName> partitionNames, Supplier<Map<String, PartitionStatistics>> valueSupplier)
    {
        return loadValue(partitionStatisticsCache, partitionNames, valueSupplier);
    }

    public List<String> getAllTables(String databaseName, Supplier<List<String>> valueSupplier)
    {
        return loadValue(allTablesCache, databaseName, valueSupplier);
    }

    public List<String> getTablesWithParameter(TablesWithParameterCacheKey tablesWithParameterCacheKey, Supplier<List<String>> valueSupplier)
    {
        return loadValue(tablesWithParameterCache, tablesWithParameterCacheKey, valueSupplier);
    }

    public List<String> getAllViews(String databaseName, Supplier<List<String>> valueSupplier)
    {
        return loadValue(allViewsCache, databaseName, valueSupplier);
    }

    public Optional<Partition> getPartition(HivePartitionName hivePartitionName, Supplier<Optional<Partition>> valueSupplier)
    {
        return loadValue(partitionCache, hivePartitionName, valueSupplier);
    }

    public Optional<List<String>> getPartitionNamesByFilter(PartitionFilter partitionFilter, Supplier<Optional<List<String>>> valueSupplier)
    {
        return loadValue(partitionNamesByPartsCache, partitionFilter, valueSupplier);
    }

    public Map<String, Optional<Partition>> getPartitionsByNames(Set<HivePartitionName> partitionNames, Supplier<Map<String, Optional<Partition>>> valueSupplier)
    {
        return loadValue(partitionsByNamesCache, partitionNames, valueSupplier);
    }

    public Set<HivePrivilegeInfo> listTablePrivileges(UserTableKey userTableKey, Supplier<Set<HivePrivilegeInfo>> valueSupplier)
    {
        return loadValue(tablePrivilegesCache, userTableKey, valueSupplier);
    }

    public Set<String> listRoles(Supplier<Set<String>> valueSupplier)
    {
        if (replay) {
            return allRoles.orElseThrow(() -> new TrinoException(NOT_FOUND, "Missing entry for roles"));
        }

        Set<String> result = valueSupplier.get();
        allRoles = Optional.of(result);
        return result;
    }

    public Set<RoleGrant> listGrantedPrincipals(String role, Supplier<Set<RoleGrant>> valueSupplier)
    {
        return loadValue(grantedPrincipalsCache, role, valueSupplier);
    }

    public Set<RoleGrant> listRoleGrants(HivePrincipal principal, Supplier<Set<RoleGrant>> valueSupplier)
    {
        return loadValue(roleGrantsCache, principal, valueSupplier);
    }

    private static <K, V> NonEvictableCache<K, V> createCache(boolean reply, Duration recordingDuration)
    {
        if (reply) {
            return buildNonEvictableCache(CacheBuilder.newBuilder());
        }

        return buildNonEvictableCache(CacheBuilder.newBuilder()
                .expireAfterWrite(recordingDuration.toMillis(), MILLISECONDS));
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

        try (GZIPOutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(recordingPath))) {
            outputStream.write(recordingCodec.toJsonBytes(recording));
        }
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
