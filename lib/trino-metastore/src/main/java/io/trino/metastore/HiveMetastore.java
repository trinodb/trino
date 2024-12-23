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

import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.TrinoException;
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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface HiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(String databaseName, String tableName);

    /**
     * @param columnNames Must not be empty.
     */
    Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames);

    /**
     * @param columnNames Must not be empty.
     */
    Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            Set<String> columnNames);

    /**
     * If true, callers should inspect table and partition parameters for spark stats.
     * This method really only exists for the ThriftHiveMetastore implementation. Spark mixes table and column statistics into the table parameters, and this breaks
     * the abstractions of the metastore interface.
     */
    default boolean useSparkTableStatistics()
    {
        return false;
    }

    void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate);

    void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates);

    List<TableInfo> getTables(String databaseName);

    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void renameDatabase(String databaseName, String newDatabaseName);

    void setDatabaseOwner(String databaseName, HivePrincipal principal);

    void createTable(Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is to drop and add. Trying to
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
     * @return a list of partition names as created by {@code MetastoreUtil#toPartitionName}
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

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    /**
     * @param principal when empty, all table privileges are returned
     */
    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal);

    default void checkSupportsTransactions()
    {
        throw new TrinoException(NOT_SUPPORTED, getClass().getSimpleName() + " does not support ACID tables");
    }

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
            AcidOperation operation,
            boolean isDynamicPartitionWrite)
    {
        throw new UnsupportedOperationException();
    }

    default void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
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

    boolean functionExists(String databaseName, String functionName, String signatureToken);

    Collection<LanguageFunction> getAllFunctions(String databaseName);

    Collection<LanguageFunction> getFunctions(String databaseName, String functionName);

    void createFunction(String databaseName, String functionName, LanguageFunction function);

    void replaceFunction(String databaseName, String functionName, LanguageFunction function);

    void dropFunction(String databaseName, String functionName, String signatureToken);
}
