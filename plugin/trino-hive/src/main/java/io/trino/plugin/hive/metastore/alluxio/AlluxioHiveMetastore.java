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
package io.trino.plugin.hive.metastore.alluxio;

import alluxio.client.table.TableMasterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;

/**
 * Implementation of the {@link HiveMetastore} interface through Alluxio.
 */
public class AlluxioHiveMetastore
        implements HiveMetastore
{
    private final TableMasterClient client;

    public AlluxioHiveMetastore(TableMasterClient client, MetastoreConfig metastoreConfig)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(metastoreConfig, "metastoreConfig is null");
        checkArgument(!metastoreConfig.isHideDeltaLakeTables(), "Hiding Delta Lake tables is not supported"); // TODO
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getDatabase(databaseName)));
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return client.getAllDatabases();
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getTable(databaseName, tableName)));
        }
        catch (NotFoundException e) {
            return Optional.empty();
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    private Map<String, HiveColumnStatistics> groupStatisticsByColumn(List<ColumnStatisticsInfo> statistics, OptionalLong rowCount)
    {
        return statistics.stream()
                .collect(toImmutableMap(ColumnStatisticsInfo::getColName, statisticsObj -> ProtoUtils.fromProto(statisticsObj.getData(), rowCount)));
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        try {
            HiveBasicStatistics basicStats = ThriftMetastoreUtil.getHiveBasicStatistics(table.getParameters());
            List<Column> columns = new ArrayList<>(table.getPartitionColumns());
            columns.addAll(table.getDataColumns());
            List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            List<ColumnStatisticsInfo> colStatsList = client.getTableColumnStatistics(table.getDatabaseName(), table.getTableName(), columnNames);
            return new PartitionStatistics(basicStats, groupStatisticsByColumn(colStatsList, basicStats.getRowCount()));
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        try {
            List<String> dataColumns = table.getDataColumns().stream()
                    .map(Column::getName)
                    .collect(toImmutableList());
            List<String> partitionColumns = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(toImmutableList());

            Map<String, HiveBasicStatistics> partitionBasicStatistics = partitions.stream()
                    .collect(toImmutableMap(
                            partition -> makePartName(partitionColumns, partition.getValues()),
                            partition -> getHiveBasicStatistics(partition.getParameters())));
            Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));

            Map<String, List<ColumnStatisticsInfo>> colStatsMap = client.getPartitionColumnStatistics(table.getDatabaseName(), table.getTableName(),
                    partitionBasicStatistics.keySet().stream().collect(toImmutableList()), dataColumns);
            Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = colStatsMap.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
            ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
            for (String partitionName : partitionBasicStatistics.keySet()) {
                HiveBasicStatistics basicStatistics = partitionBasicStatistics.get(partitionName);
                Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStatistics.getOrDefault(partitionName, ImmutableMap.of());
                result.put(partitionName, new PartitionStatistics(basicStatistics, columnStatistics));
            }
            return result.buildOrThrow();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updateTableStatistics(
            String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateTableStatistics");
    }

    @Override
    public void updatePartitionStatistics(
            Table table,
            Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        throw new TrinoException(NOT_SUPPORTED, "updatePartitionStatistics");
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return client.getAllTables(databaseName);
        }
        catch (NotFoundException e) {
            return new ArrayList<>(0);
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getTablesWithParameter(
            String databaseName,
            String parameterKey,
            String parameterValue)
    {
        try {
            return client.getAllTables(databaseName).stream()
                    .filter(tableName -> {
                        // TODO Is there a way to do a bulk RPC?
                        try {
                            TableInfo table = client.getTable(databaseName, tableName);
                            if (table == null) {
                                return false;
                            }
                            String value = table.getParametersMap().get(parameterKey);
                            return value != null && value.equals(parameterValue);
                        }
                        catch (AlluxioStatusException e) {
                            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get info for table: " + tableName, e);
                        }
                    })
                    .collect(Collectors.toList());
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        // TODO: Add views on the server side
        return Collections.emptyList();
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new TrinoException(NOT_SUPPORTED, "createDatabase");
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropDatabase");
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameDatabase");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setDatabaseOwner");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new TrinoException(NOT_SUPPORTED, "createTable");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropTable");
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable,
            PrincipalPrivileges principalPrivileges)
    {
        throw new TrinoException(NOT_SUPPORTED, "replaceTable");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName,
            String newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameTable");
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "commentTable");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTableOwner");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "commentColumn");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName,
            HiveType columnType, String columnComment)
    {
        throw new TrinoException(NOT_SUPPORTED, "addColumn");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName,
            String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameColumn");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropColumn");
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        throw new TrinoException(NOT_SUPPORTED, "getPartition");
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        try {
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            List<String> partitionNames = partitionInfos.stream()
                    .map(PartitionInfo::getPartitionName)
                    .collect(Collectors.toList());
            return Optional.of(partitionNames);
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        if (partitionNames.isEmpty()) {
            return Collections.emptyMap();
        }
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableName();

        try {
            // Get all partitions
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            // Check that table name is correct
            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream()
                    .filter(partition -> partition.getTableName().equals(tableName))
                    .collect(Collectors.toList());
            Map<String, Optional<Partition>> result = partitionInfos.stream()
                    .filter(partitionName -> partitionNames.stream()
                            .anyMatch(partitionName.getPartitionName()::equals))
                    .collect(Collectors.toMap(
                            PartitionInfo::getPartitionName,
                            partitionInfo -> Optional.of(ProtoUtils.fromProto(partitionInfo))));
            return Collections.unmodifiableMap(result);
        }
        catch (AlluxioStatusException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName,
            List<PartitionWithStatistics> partitions)
    {
        throw new TrinoException(NOT_SUPPORTED, "addPartitions");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts,
            boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropPartition");
    }

    @Override
    public void alterPartition(String databaseName, String tableName,
            PartitionWithStatistics partition)
    {
        throw new TrinoException(NOT_SUPPORTED, "alterPartition");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "createRole");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropRole");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoles");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption,
            HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantRoles");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor,
            HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeRoles");
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoleGrants");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoleGrants");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeTablePrivileges");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "listTablePrivileges");
    }
}
