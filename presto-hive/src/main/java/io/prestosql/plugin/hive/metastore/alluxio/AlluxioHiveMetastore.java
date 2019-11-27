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
package io.prestosql.plugin.hive.metastore.alluxio;

import alluxio.client.table.TableMasterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.TableInfo;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import com.google.inject.Inject;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.function.Function.identity;

/**
 * Implementation of the {@link HiveMetastore} interface through Alluxio.
 */
public class AlluxioHiveMetastore
        implements HiveMetastore
{
    private TableMasterClient client;

    @Inject
    public AlluxioHiveMetastore(TableMasterClient client)
    {
        this.client = client;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getDatabase(databaseName)));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return client.getAllDatabases();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getTable(databaseName, tableName)));
        }
        catch (NotFoundException e) {
            return Optional.empty();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        throw new UnsupportedOperationException("getSupportedColumnStatistics");
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            Table table = getTable(identity, databaseName, tableName)
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR,
                            String.format("Could not retrieve table %s.%s", databaseName, tableName)));
            HiveBasicStatistics basicStats =
                    ThriftMetastoreUtil.getHiveBasicStatistics(table.getParameters());
            // TODO implement logic to populate Map<string, HiveColumnStatistics>
            return new PartitionStatistics(basicStats, Collections.emptyMap());
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, String databaseName, String tableName, Set<String> partitionNames)
    {
        // TODO implement partition statistics
        // currently returns a map of partitionName to empty statistics to satisfy presto requirements
        return Collections.unmodifiableMap(
                partitionNames.stream().collect(Collectors.toMap(identity(), (p) -> PartitionStatistics.empty())));
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updateTableStatistics");
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName,
            String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updatePartitionStatistics");
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return client.getAllTables(databaseName);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey,
            String parameterValue)
    {
        try {
            return client.getAllTables(databaseName).stream().filter(s -> {
                        // TODO Is there a way to do a bulk RPC?
                try {
                    TableInfo table = client.getTable(databaseName, s);
                    if (table == null) {
                        return false;
                    }
                    String value = table.getParametersMap().get(parameterKey);
                    return value != null && value.equals(parameterValue);
                }
                catch (AlluxioStatusException e) {
                    throw new RuntimeException("Failed to get info for table " + s, e);
                }
            }).collect(Collectors.toList());
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        // TODO: Add views on the server side
        return Collections.emptyList();
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        throw new UnsupportedOperationException("createDatabase");
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        throw new UnsupportedOperationException("dropDatabase");
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException("renameDatabase");
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("createTable");
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropTable");
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable,
            PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("replaceTable");
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName,
            String newTableName)
    {
        throw new UnsupportedOperationException("renameTable");
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("commentTable");
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName,
            HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("addColumn");
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName,
            String newColumnName)
    {
        throw new UnsupportedOperationException("renameColumn");
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("dropColumn");
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName,
            List<String> partitionValues)
    {
        throw new UnsupportedOperationException("getPartition");
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("getPartitionNames");
    }

    /**
     * return a list of partition names by which the values of each partition is at least
     * contained which the {@code parts} argument
     *
     * @param databaseName
     * @param tableName
     * @param parts        list of values which returned partitions should contain
     * @return optionally, a list of strings where each entry is in the form of {key}={value}
     */
    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        try {
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName))
                    // Filter out any partitions which have values that don't match
                    .filter(partition -> {
                        List<String> values = partition.getValuesList();
                        if (values.size() != parts.size()) {
                            return false;
                        }
                        for (int i = 0; i < values.size(); i++) {
                            String constraintPart = parts.get(i);
                            if (!constraintPart.isEmpty() && !values.get(i).equals(constraintPart)) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .collect(Collectors.toList());
            List<String> partitionNames = partitionInfos.stream().map(PartitionInfo::getPartitionName).collect(Collectors.toList());
            return Optional.of(partitionNames);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName,
            String tableName, List<String> partitionNames)
    {
        if (partitionNames.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            // Get all partitions
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            // Check that table name is correct
            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName))
                    .collect(Collectors.toList());
            Map<String, Optional<Partition>> result = partitionInfos.stream()
                    .filter(p -> partitionNames.stream().anyMatch(p.getPartitionName()::equals))
                    .collect(Collectors.toMap(
                            PartitionInfo::getPartitionName,
                            pi -> Optional.of(ProtoUtils.fromProto(pi))));
            return Collections.unmodifiableMap(result);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName,
            List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("addPartitions");
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts,
            boolean deleteData)
    {
        throw new UnsupportedOperationException("dropPartition");
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName,
            PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("alterPartition");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException("createRole");
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException("dropRole");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException("listRoles");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption,
            HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("grantRoles");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor,
            HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("revokeRoles");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new UnsupportedOperationException("listRoleGrants");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee,
            Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("grantTablePrivileges");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee,
            Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("revokeTablePrivileges");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal)
    {
        throw new UnsupportedOperationException("listTablePrivileges");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
