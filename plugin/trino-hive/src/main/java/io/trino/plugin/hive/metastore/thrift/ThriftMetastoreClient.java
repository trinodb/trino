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

import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.EnvironmentContext;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.HiveObjectPrivilege;
import io.trino.hive.thrift.metastore.HiveObjectRef;
import io.trino.hive.thrift.metastore.LockRequest;
import io.trino.hive.thrift.metastore.LockResponse;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.PrincipalType;
import io.trino.hive.thrift.metastore.PrivilegeBag;
import io.trino.hive.thrift.metastore.Role;
import io.trino.hive.thrift.metastore.RolePrincipalGrant;
import io.trino.hive.thrift.metastore.Table;
import io.trino.hive.thrift.metastore.TxnToWriteId;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.spi.connector.SchemaTableName;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ThriftMetastoreClient
        extends Closeable
{
    @Override
    void close();

    List<String> getAllDatabases()
            throws TException;

    Database getDatabase(String databaseName)
            throws TException;

    List<String> getAllTables(String databaseName)
            throws TException;

    Optional<List<SchemaTableName>> getAllTables()
            throws TException;

    List<String> getAllViews(String databaseName)
            throws TException;

    Optional<List<SchemaTableName>> getAllViews()
            throws TException;

    List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
            throws TException;

    void createDatabase(Database database)
            throws TException;

    void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException;

    void alterDatabase(String databaseName, Database database)
            throws TException;

    void createTable(Table table)
            throws TException;

    void dropTable(String databaseName, String name, boolean deleteData)
            throws TException;

    void alterTableWithEnvironmentContext(String databaseName, String tableName, Table newTable, EnvironmentContext context)
            throws TException;

    Table getTable(String databaseName, String tableName)
            throws TException;

    List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException;

    List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException;

    void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException;

    void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException;

    Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException;

    void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException;

    void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException;

    List<String> getPartitionNames(String databaseName, String tableName)
            throws TException;

    List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException;

    int addPartitions(List<Partition> newPartitions)
            throws TException;

    boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException;

    void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException;

    Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException;

    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException;

    List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException;

    List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException;

    List<String> getRoleNames()
            throws TException;

    void createRole(String role, String grantor)
            throws TException;

    void dropRole(String role)
            throws TException;

    boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException;

    boolean revokePrivileges(PrivilegeBag privilegeBag, boolean revokeGrantOption)
            throws TException;

    void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException;

    void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException;

    List<RolePrincipalGrant> listGrantedPrincipals(String role)
            throws TException;

    List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
            throws TException;

    void setUGI(String userName)
            throws TException;

    long openTransaction(String user)
            throws TException;

    void commitTransaction(long transactionId)
            throws TException;

    default void abortTransaction(long transactionId)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    void sendTransactionHeartbeat(long transactionId)
            throws TException;

    LockResponse acquireLock(LockRequest lockRequest)
            throws TException;

    LockResponse checkLock(long lockId)
            throws TException;

    void unlock(long lockId)
            throws TException;

    String getValidWriteIds(List<String> tableList, long currentTransactionId)
            throws TException;

    String getConfigValue(String name, String defaultValue)
            throws TException;

    String getDelegationToken(String userName)
            throws TException;

    default List<TxnToWriteId> allocateTableWriteIds(String database, String tableName, List<Long> transactionIds)
            throws TException
    {
        throw new UnsupportedOperationException();
    }

    void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
            throws TException;

    void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
            throws TException;

    void alterTransactionalTable(Table table, long transactionId, long writeId, EnvironmentContext context)
            throws TException;
}
