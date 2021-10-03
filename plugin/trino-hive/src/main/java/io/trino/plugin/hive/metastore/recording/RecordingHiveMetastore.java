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

import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.trino.plugin.hive.metastore.HiveTableName.hiveTableName;
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
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return recording.getTable(hiveTableName(databaseName, tableName), () -> delegate.getTable(identity, databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return recording.getSupportedColumnStatistics(type.getTypeSignature().toString(), () -> delegate.getSupportedColumnStatistics(type));
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return recording.getTableStatistics(
                hiveTableName(table.getDatabaseName(), table.getTableName()),
                () -> delegate.getTableStatistics(identity, table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return recording.getPartitionStatistics(
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
        return recording.getPartition(
                hivePartitionName(hiveTableName(table.getDatabaseName(), table.getTableName()), partitionValues),
                () -> delegate.getPartition(identity, table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(HiveIdentity identity, String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return recording.getPartitionNamesByFilter(
                partitionFilter(databaseName, tableName, columnNames, partitionKeysFilter),
                () -> delegate.getPartitionNamesByFilter(identity, databaseName, tableName, columnNames, partitionKeysFilter));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        return recording.getPartitionsByNames(
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
