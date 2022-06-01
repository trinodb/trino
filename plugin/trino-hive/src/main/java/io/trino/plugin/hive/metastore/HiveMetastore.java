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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.connector.SchemaTableName;
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

public interface HiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(Table table);

    Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions);

    void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update);

    default void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        updatePartitionStatistics(table, ImmutableMap.of(partitionName, update));
    }

    void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates);

    List<String> getAllTables(String databaseName);

    List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue);

    List<String> getAllViews(String databaseName);

    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void renameDatabase(String databaseName, String newDatabaseName);

    void setDatabaseOwner(String databaseName, HivePrincipal principal);

    void createTable(Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName);

    void commentTable(String databaseName, String tableName, Optional<String> comment);

    void setTableOwner(String databaseName, String tableName, HivePrincipal principal);

    void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment);

    void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(Table table, List<String> partitionValues);

    /**
     * Return a list of partition names, with optional filtering (hint to improve performance if possible).
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @param columnNames the list of partition column names
     * @param partitionKeysFilter optional filter for the partition column values
     * @return a list of partition names as created by {@link MetastoreUtil#toPartitionName}
     * @see TupleDomain
     */
    Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter);

    Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames);

    void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    Set<RoleGrant> listGrantedPrincipals(String role);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    /**
     * @param tableOwner
     * @param principal when empty, all table privileges are returned
     */
    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal);

    default long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw new UnsupportedOperationException();
    }

    default void commitTransaction(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void abortTransaction(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void sendTransactionHeartbeat(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        throw new UnsupportedOperationException();
    }

    default String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException();
    }

    default Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }

    default long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        throw new UnsupportedOperationException();
    }

    default void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        throw new UnsupportedOperationException();
    }

    default void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        throw new UnsupportedOperationException();
    }

    default void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        throw new UnsupportedOperationException();
    }

    default void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }
}
