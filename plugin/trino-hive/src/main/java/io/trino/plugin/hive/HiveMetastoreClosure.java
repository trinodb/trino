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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.acid.AcidOperation;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.DataOperationType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreClosure
{
    private final HiveMetastore delegate;

    /**
     * Do not use this directly.  Instead, the closure should be fetched from the current SemiTransactionalHiveMetastore,
     * which can be fetched from the current HiveMetadata.
     */
    public HiveMetastoreClosure(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    private Table getExistingTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName);
    }

    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return delegate.getTableStatistics(getExistingTable(databaseName, tableName));
    }

    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getExistingTable(databaseName, tableName);
        List<Partition> partitions = getExistingPartitionsByNames(table, ImmutableList.copyOf(partitionNames));
        return delegate.getPartitionStatistics(table, partitions);
    }

    public void updateTableStatistics(String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        delegate.updateTableStatistics(databaseName, tableName, transaction, update);
    }

    public void updatePartitionStatistics(String databaseName,
            String tableName,
            String partitionName,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table table = getExistingTable(databaseName, tableName);
        delegate.updatePartitionStatistics(table, partitionName, update);
    }

    public void updatePartitionStatistics(String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        Table table = getExistingTable(databaseName, tableName);
        delegate.updatePartitionStatistics(table, updates);
    }

    public List<String> getAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return delegate.getTablesWithParameter(databaseName, parameterKey, parameterValue);
    }

    public List<String> getAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
    }

    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        delegate.renameDatabase(databaseName, newDatabaseName);
    }

    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        delegate.setDatabaseOwner(databaseName, principal);
    }

    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        delegate.setTableOwner(databaseName, tableName, principal);
    }

    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(table, principalPrivileges);
    }

    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
    }

    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
    }

    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        delegate.commentTable(databaseName, tableName, comment);
    }

    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        delegate.commentColumn(databaseName, tableName, columnName, comment);
    }

    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
    }

    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
    }

    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        delegate.dropColumn(databaseName, tableName, columnName);
    }

    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        return delegate.getTable(databaseName, tableName)
                .flatMap(table -> delegate.getPartition(table, partitionValues));
    }

    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    private List<Partition> getExistingPartitionsByNames(Table table, List<String> partitionNames)
    {
        Map<String, Partition> partitions = delegate.getPartitionsByNames(table, partitionNames).entrySet().stream()
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().orElseThrow(() ->
                        new PartitionNotFoundException(table.getSchemaTableName(), extractPartitionValues(entry.getKey())))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return partitionNames.stream()
                .map(partitions::get)
                .collect(toImmutableList());
    }

    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        return delegate.getTable(databaseName, tableName)
                .map(table -> delegate.getPartitionsByNames(table, partitionNames))
                .orElseGet(() -> partitionNames.stream()
                        .collect(toImmutableMap(name -> name, name -> Optional.empty())));
    }

    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        delegate.alterPartition(databaseName, tableName, partition);
    }

    public void createRole(String role, String grantor)
    {
        delegate.createRole(role, grantor);
    }

    public void dropRole(String role)
    {
        delegate.dropRole(role);
    }

    public Set<String> listRoles()
    {
        return delegate.listRoles();
    }

    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return delegate.listGrantedPrincipals(role);
    }

    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
    }

    public long openTransaction()
    {
        return delegate.openTransaction();
    }

    public void commitTransaction(long transactionId)
    {
        delegate.commitTransaction(transactionId);
    }

    public void abortTransaction(long transactionId)
    {
        delegate.abortTransaction(transactionId);
    }

    public void sendTransactionHeartbeat(long transactionId)
    {
        delegate.sendTransactionHeartbeat(transactionId);
    }

    public void acquireSharedReadLock(String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(queryId, transactionId, fullTables, partitions);
    }

    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return delegate.getValidWriteIds(tables, currentTransactionId);
    }

    public Optional<String> getConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(dbName, tableName, transactionId);
    }

    public void acquireTableWriteLock(String queryId, long transactionId, String dbName, String tableName, DataOperationType operation, boolean isPartitioned)
    {
        delegate.acquireTableWriteLock(queryId, transactionId, dbName, tableName, operation, isPartitioned);
    }

    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        delegate.alterPartitions(dbName, tableName, partitions, writeId);
    }

    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges);
    }
}
