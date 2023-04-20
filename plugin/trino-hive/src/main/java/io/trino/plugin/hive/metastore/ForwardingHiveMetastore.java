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

import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore delegate;

    protected ForwardingHiveMetastore(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName);
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return delegate.getTableStatistics(table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return delegate.getPartitionStatistics(table, partitions);
    }

    @Override
    public void updateTableStatistics(
            String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updateTableStatistics(databaseName, tableName, transaction, update);
    }

    @Override
    public void updatePartitionStatistics(
            Table table,
            String partitionName,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updatePartitionStatistics(table, partitionName, update);
    }

    @Override
    public void updatePartitionStatistics(
            Table table,
            Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        delegate.updatePartitionStatistics(table, updates);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return delegate.getAllTables();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return delegate.getTablesWithParameter(databaseName, parameterKey, parameterValue);
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return delegate.getAllViews();
    }

    @Override
    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        delegate.renameDatabase(databaseName, newDatabaseName);
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        delegate.setDatabaseOwner(databaseName, principal);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(
            String databaseName,
            String tableName,
            Table newTable,
            PrincipalPrivileges principalPrivileges)
    {
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        delegate.commentTable(databaseName, tableName, comment);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        delegate.setTableOwner(databaseName, tableName, principal);
    }

    @Override
    public void commentColumn(
            String databaseName,
            String tableName,
            String columnName,
            Optional<String> comment)
    {
        delegate.commentColumn(databaseName, tableName, columnName, comment);
    }

    @Override
    public void addColumn(
            String databaseName,
            String tableName,
            String columnName,
            HiveType columnType,
            String columnComment)
    {
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        delegate.dropColumn(databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return delegate.getPartition(table, partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(
            Table table,
            List<String> partitionNames)
    {
        return delegate.getPartitionsByNames(table, partitionNames);
    }

    @Override
    public void addPartitions(
            String databaseName,
            String tableName,
            List<PartitionWithStatistics> partitions)
    {
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(
            String databaseName,
            String tableName,
            PartitionWithStatistics partition)
    {
        delegate.alterPartition(databaseName, tableName, partition);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return delegate.listGrantedPrincipals(role);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilege> privileges,
            boolean grantOption)
    {
        delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void revokeTablePrivileges(String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilege> privileges, boolean grantOption)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName,
            String tableName,
            Optional<String> tableOwner,
            Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
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

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return delegate.getConfigValue(name);
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
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(
            String dbName,
            String tableName,
            long transactionId,
            long writeId,
            OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public void alterPartitions(
            String dbName,
            String tableName,
            List<Partition> partitions,
            long writeId)
    {
        delegate.alterPartitions(dbName, tableName, partitions, writeId);
    }

    @Override
    public void addDynamicPartitions(
            String dbName,
            String tableName,
            List<String> partitionNames,
            long transactionId,
            long writeId,
            AcidOperation operation)
    {
        delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    @Override
    public void alterTransactionalTable(
            Table table,
            long transactionId,
            long writeId,
            PrincipalPrivileges principalPrivileges)
    {
        delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges);
    }
}
