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

import io.trino.plugin.base.metrics.ApiCallStats;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MeasuredHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore delegate;
    private final ApiCallStats apiCallStats = new ApiCallStats();

    public MeasuredHiveMetastore(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public static HiveMetastoreFactory factory(HiveMetastoreFactory metastoreFactory)
    {
        return new MeasuredMetastoreFactory(metastoreFactory);
    }

    @Override
    public Metrics getMetrics()
    {
        return apiCallStats.toMetrics("metastore");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return apiCallStats.measure("getDatabase", () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        return apiCallStats.measure("getAllDatabases", delegate::getAllDatabases);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return apiCallStats.measure("getTable", () -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return apiCallStats.measure("getTableColumnStatistics", () -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return apiCallStats.measure("getPartitionColumnStatistics", () -> delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames));
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return apiCallStats.measure("useSparkTableStatistics", delegate::useSparkTableStatistics);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        apiCallStats.measure("updateTableStatistics", () -> delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate));
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        apiCallStats.measure("updatePartitionStatistics", () -> delegate.updatePartitionStatistics(table, mode, partitionUpdates));
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return apiCallStats.measure("getTables", () -> delegate.getTables(databaseName));
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return apiCallStats.measure("getTableNamesWithParameters", () -> delegate.getTableNamesWithParameters(databaseName, parameterKey, parameterValues));
    }

    @Override
    public void createDatabase(Database database)
    {
        apiCallStats.measure("createDatabase", () -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        apiCallStats.measure("dropDatabase", () -> delegate.dropDatabase(databaseName, deleteData));
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        apiCallStats.measure("renameDatabase", () -> delegate.renameDatabase(databaseName, newDatabaseName));
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        apiCallStats.measure("setDatabaseOwner", () -> delegate.setDatabaseOwner(databaseName, principal));
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        apiCallStats.measure("createTable", () -> delegate.createTable(table, principalPrivileges));
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        apiCallStats.measure("dropTable", () -> delegate.dropTable(databaseName, tableName, deleteData));
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        apiCallStats.measure("replaceTable", () -> delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges, environmentContext));
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        apiCallStats.measure("renameTable", () -> delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName));
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        apiCallStats.measure("commentTable", () -> delegate.commentTable(databaseName, tableName, comment));
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        apiCallStats.measure("setTableOwner", () -> delegate.setTableOwner(databaseName, tableName, principal));
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        apiCallStats.measure("commentColumn", () -> delegate.commentColumn(databaseName, tableName, columnName, comment));
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        apiCallStats.measure("addColumn", () -> delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment));
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        apiCallStats.measure("renameColumn", () -> delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName));
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        apiCallStats.measure("dropColumn", () -> delegate.dropColumn(databaseName, tableName, columnName));
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return apiCallStats.measure("getPartition", () -> delegate.getPartition(table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return apiCallStats.measure("getPartitionNamesByFilter", () -> delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return apiCallStats.measure("getPartitionsByNames", () -> delegate.getPartitionsByNames(table, partitionNames));
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        apiCallStats.measure("addPartitions", () -> delegate.addPartitions(databaseName, tableName, partitions));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        apiCallStats.measure("dropPartition", () -> delegate.dropPartition(databaseName, tableName, parts, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        apiCallStats.measure("alterPartition", () -> delegate.alterPartition(databaseName, tableName, partition));
    }

    @Override
    public void createRole(String role, String grantor)
    {
        apiCallStats.measure("createRole", () -> delegate.createRole(role, grantor));
    }

    @Override
    public void dropRole(String role)
    {
        apiCallStats.measure("dropRole", () -> delegate.dropRole(role));
    }

    @Override
    public Set<String> listRoles()
    {
        return apiCallStats.measure("listRoles", delegate::listRoles);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        apiCallStats.measure("grantRoles", () -> delegate.grantRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        apiCallStats.measure("revokeRoles", () -> delegate.revokeRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return apiCallStats.measure("listRoleGrants", () -> delegate.listRoleGrants(principal));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        apiCallStats.measure("grantTablePrivileges", () -> delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        apiCallStats.measure("revokeTablePrivileges", () -> delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return apiCallStats.measure("listTablePrivileges", () -> delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal));
    }

    @Override
    public void checkSupportsTransactions()
    {
        apiCallStats.measure("checkSupportsTransactions", () -> delegate.checkSupportsTransactions());
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return apiCallStats.measure("openTransaction", () -> delegate.openTransaction(transactionOwner));
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        apiCallStats.measure("commitTransaction", () -> delegate.commitTransaction(transactionId));
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        apiCallStats.measure("abortTransaction", () -> delegate.abortTransaction(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        apiCallStats.measure("sendTransactionHeartbeat", () -> delegate.sendTransactionHeartbeat(transactionId));
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        apiCallStats.measure("acquireSharedReadLock", () -> delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions));
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return apiCallStats.measure("getValidWriteIds", () -> delegate.getValidWriteIds(tables, currentTransactionId));
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return apiCallStats.measure("getConfigValue", () -> delegate.getConfigValue(name));
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return apiCallStats.measure("allocateWriteId", () -> delegate.allocateWriteId(dbName, tableName, transactionId));
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        apiCallStats.measure("acquireTableWriteLock", () -> delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite));
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        apiCallStats.measure("updateTableWriteId", () -> delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange));
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        apiCallStats.measure("addDynamicPartitions", () -> delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation));
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        apiCallStats.measure("alterTransactionalTable", () -> delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges));
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return apiCallStats.measure("functionExists", () -> delegate.functionExists(databaseName, functionName, signatureToken));
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return apiCallStats.measure("getAllFunctions", () -> delegate.getAllFunctions(databaseName));
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return apiCallStats.measure("getFunctions", () -> delegate.getFunctions(databaseName, functionName));
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        apiCallStats.measure("createFunction", () -> delegate.createFunction(databaseName, functionName, function));
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        apiCallStats.measure("replaceFunction", () -> delegate.replaceFunction(databaseName, functionName, function));
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        apiCallStats.measure("dropFunction", () -> delegate.dropFunction(databaseName, functionName, signatureToken));
    }

    public static class MeasuredMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastoreFactory metastoreFactory;

        public MeasuredMetastoreFactory(HiveMetastoreFactory metastoreFactory)
        {
            this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return metastoreFactory.isImpersonationEnabled();
        }

        @Override
        public boolean hasBuiltInCaching()
        {
            return metastoreFactory.hasBuiltInCaching();
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            return new MeasuredHiveMetastore(metastoreFactory.createMetastore(identity));
        }
    }
}
