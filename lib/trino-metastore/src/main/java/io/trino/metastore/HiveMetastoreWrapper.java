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
package io.trino.metastore;

import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveMetastoreWrapper
        implements HiveMetastore
{
    protected final HiveMetastore delegate;

    public HiveMetastoreWrapper(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        delegate.abortTransaction(transactionId);
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(dbName, tableName, transactionId);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        delegate.alterPartition(databaseName, tableName, partition);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges);
    }

    @Override
    public void checkSupportsTransactions()
    {
        delegate.checkSupportsTransactions();
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        delegate.commentColumn(databaseName, tableName, columnName, comment);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        delegate.commentTable(databaseName, tableName, comment);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        delegate.commitTransaction(transactionId);
    }

    @Override
    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        delegate.createFunction(databaseName, functionName, function);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        delegate.createRole(role, grantor);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        delegate.dropColumn(databaseName, tableName, columnName);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        delegate.dropFunction(databaseName, functionName, signatureToken);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void dropRole(String role)
    {
        delegate.dropRole(role);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return delegate.functionExists(databaseName, functionName, signatureToken);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return delegate.getAllFunctions(databaseName);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return delegate.getFunctions(databaseName, functionName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return delegate.getPartition(table, partitionValues);
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return delegate.getPartitionsByNames(table, partitionNames);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return delegate.getTableColumnStatistics(databaseName, tableName, columnNames);
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return delegate.getTableNamesWithParameters(databaseName, parameterKey, parameterValues);
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return delegate.getTables(databaseName);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return delegate.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    @Override
    public Set<String> listRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return delegate.openTransaction(transactionOwner);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        delegate.renameDatabase(databaseName, newDatabaseName);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        delegate.replaceFunction(databaseName, functionName, function);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges, environmentContext);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        delegate.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        delegate.setDatabaseOwner(databaseName, principal);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        delegate.setTableOwner(databaseName, tableName, principal);
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        delegate.updatePartitionStatistics(table, mode, partitionUpdates);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return delegate.useSparkTableStatistics();
    }
}
