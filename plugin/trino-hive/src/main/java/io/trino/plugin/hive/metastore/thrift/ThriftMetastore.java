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
package io.trino.plugin.hive.metastore.thrift;

import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.Table;
import io.trino.hive.thrift.metastore.TableMeta;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public sealed interface ThriftMetastore
        permits ThriftHiveMetastore
{
    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void alterDatabase(String databaseName, Database database);

    void createTable(Table table);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    void alterTable(String databaseName, String tableName, Table table);

    void alterTransactionalTable(Table table, long transactionId, long writeId);

    List<String> getAllDatabases();

    List<TableMeta> getTables(String databaseName);

    Optional<Database> getDatabase(String databaseName);

    void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition);

    Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter);

    Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(String databaseName, String tableName);

    Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames);

    Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames);

    boolean useSparkTableStatistics();

    void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate);

    void updatePartitionStatistics(Table table, String partitionName, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate);

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

    default Optional<List<FieldSchema>> getFields(String databaseName, String tableName)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        if (table.getSd() == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        return Optional.of(table.getSd().getCols());
    }

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
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        throw new UnsupportedOperationException();
    }

    default long acquireTableExclusiveLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            String dbName,
            String tableName)
    {
        throw new UnsupportedOperationException();
    }

    default void releaseTableLock(long lockId)
    {
        throw new UnsupportedOperationException();
    }

    default void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        throw new UnsupportedOperationException();
    }

    default void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, DataOperationType operation)
    {
        throw new UnsupportedOperationException();
    }

    Optional<io.trino.hive.thrift.metastore.Function> getFunction(String databaseName, String functionName);

    Collection<String> getFunctions(String databaseName, String functionNamePattern);

    void createFunction(io.trino.hive.thrift.metastore.Function function);

    void alterFunction(io.trino.hive.thrift.metastore.Function function);

    void dropFunction(String databaseName, String functionName);
}
