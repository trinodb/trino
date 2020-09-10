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
package io.prestosql.plugin.hive.metastore;

import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface HiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(HiveIdentity identity, Table table);

    Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions);

    void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    List<String> getAllTables(String databaseName);

    List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue);

    List<String> getAllViews(String databaseName);

    void createDatabase(HiveIdentity identity, Database database);

    void dropDatabase(HiveIdentity identity, String databaseName);

    void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName);

    void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal);

    void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName);

    void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment);

    void commentColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, Optional<String> comment);

    void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues);

    /**
     * return a list of partition names where partitionKeysFilter is used as a hint to each implementation.
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @param columnNames the list of partition column names
     * @param partitionKeysFilter map of filters (Domain) for each partition column
     * @return optionally, a list of strings where each entry is in the form of {key}={value}
     * @see TupleDomain
     */
    Optional<List<String>> getPartitionNamesByFilter(HiveIdentity identity, String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter);

    Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames);

    void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    Set<RoleGrant> listGrantedPrincipals(String role);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges);

    /**
     * @param principal when empty, all table privileges are returned
     */
    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal);

    boolean isImpersonationEnabled();

    default long openTransaction(HiveIdentity identity)
    {
        throw new UnsupportedOperationException();
    }

    default void commitTransaction(HiveIdentity identity, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void sendTransactionHeartbeat(HiveIdentity identity, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireSharedReadLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        throw new UnsupportedOperationException();
    }

    default String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException();
    }

    default Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }
}
