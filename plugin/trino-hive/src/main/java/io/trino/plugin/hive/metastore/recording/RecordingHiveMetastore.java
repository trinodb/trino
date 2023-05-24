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

import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TablesWithParameterCacheKey;
import io.trino.plugin.hive.metastore.UserTableKey;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.PartitionFilter.partitionFilter;
import static java.util.Objects.requireNonNull;

public class RecordingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore delegate;
    private final HiveMetastoreRecording recording;

    public RecordingHiveMetastore(HiveMetastore delegate, HiveMetastoreRecording recording)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.recording = requireNonNull(recording, "recording is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return recording.getDatabase(databaseName, () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        return recording.getAllDatabases(delegate::getAllDatabases);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return recording.getTable(hiveTableName(databaseName, tableName), () -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        // No need to record that, since it's a pure local operation.
        return delegate.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return recording.getTableStatistics(
                hiveTableName(table.getDatabaseName(), table.getTableName()),
                () -> delegate.getTableStatistics(table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return recording.getPartitionStatistics(
                partitions.stream()
                        .map(partition -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), makePartitionName(table, partition)))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionStatistics(table, partitions));
    }

    @Override
    public void updateTableStatistics(String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updateTableStatistics(databaseName, tableName, transaction, update);
    }

    @Override
    public void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(table, partitionName, update);
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(table, updates);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return recording.getAllTables(databaseName, () -> delegate.getAllTables(databaseName));
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        TablesWithParameterCacheKey key = new TablesWithParameterCacheKey(databaseName, parameterKey, parameterValue);
        return recording.getTablesWithParameter(key, () -> delegate.getTablesWithParameter(databaseName, parameterKey, parameterValue));
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return recording.getAllViews(databaseName, () -> delegate.getAllViews(databaseName));
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return recording.getAllTables(delegate::getAllTables);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return recording.getAllViews(delegate::getAllViews);
    }

    @Override
    public void createDatabase(Database database)
    {
        verifyRecordingMode();
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        verifyRecordingMode();
        delegate.renameDatabase(databaseName, newDatabaseName);
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        verifyRecordingMode();
        delegate.setDatabaseOwner(databaseName, principal);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        verifyRecordingMode();
        delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        verifyRecordingMode();
        delegate.commentTable(databaseName, tableName, comment);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        verifyRecordingMode();
        delegate.setTableOwner(databaseName, tableName, principal);
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        verifyRecordingMode();
        delegate.commentColumn(databaseName, tableName, columnName, comment);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        verifyRecordingMode();
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        verifyRecordingMode();
        delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyRecordingMode();
        delegate.dropColumn(databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return recording.getPartition(
                hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues),
                () -> delegate.getPartition(table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return recording.getPartitionNamesByFilter(
                partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter),
                () -> delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return recording.getPartitionsByNames(
                partitionNames.stream()
                        .map(partitionName -> hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionName))
                        .collect(toImmutableSet()),
                () -> delegate.getPartitionsByNames(table, partitionNames));
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        verifyRecordingMode();
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        verifyRecordingMode();
        delegate.alterPartition(databaseName, tableName, partition);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return recording.listTablePrivileges(
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
        return recording.listRoles(delegate::listRoles);
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
        return recording.listGrantedPrincipals(
                role,
                () -> delegate.listGrantedPrincipals(role));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return recording.listRoleGrants(
                principal,
                () -> delegate.listRoleGrants(principal));
    }

    private void verifyRecordingMode()
    {
        if (recording.isReplay()) {
            throw new IllegalStateException("Cannot perform Metastore updates in replay mode");
        }
    }
}
