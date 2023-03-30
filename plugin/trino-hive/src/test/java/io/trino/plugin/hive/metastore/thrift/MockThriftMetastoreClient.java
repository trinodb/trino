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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.hive.thrift.metastore.ColumnStatisticsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.EnvironmentContext;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.HiveObjectPrivilege;
import io.trino.hive.thrift.metastore.HiveObjectRef;
import io.trino.hive.thrift.metastore.LockRequest;
import io.trino.hive.thrift.metastore.LockResponse;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.PrincipalType;
import io.trino.hive.thrift.metastore.PrivilegeBag;
import io.trino.hive.thrift.metastore.Role;
import io.trino.hive.thrift.metastore.RolePrincipalGrant;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.Table;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testng.services.ManageTestResources;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.thrift.metastore.PrincipalType.ROLE;
import static io.trino.hive.thrift.metastore.PrincipalType.USER;

@ManageTestResources.Suppress(because = "close() is no-op and instance's resources are negligible")
public class MockThriftMetastoreClient
        implements ThriftMetastoreClient
{
    public static final String TEST_DATABASE = "testdb";
    public static final String BAD_DATABASE = "baddb";
    public static final String TEST_TABLE = "testtbl";
    public static final String TEST_PARTITION1_VALUE = "testpartition1";
    public static final String TEST_PARTITION1 = "key=" + TEST_PARTITION1_VALUE;
    public static final String TEST_COLUMN = "column";
    public static final String TEST_PARTITION2 = "key=testpartition2";
    public static final String TEST_PARTITION3 = "key=testpartition3";
    public static final String BAD_PARTITION = "key=badpartition1";
    public static final List<String> TEST_PARTITION_VALUES1 = ImmutableList.of("testpartition1");
    public static final List<String> TEST_PARTITION_VALUES2 = ImmutableList.of("testpartition2");
    public static final List<String> TEST_PARTITION_VALUES3 = ImmutableList.of("testpartition3");
    public static final List<String> TEST_ROLES = ImmutableList.of("testrole");
    private static final List<RolePrincipalGrant> TEST_ROLE_GRANTS = ImmutableList.of(
            new RolePrincipalGrant("role1", "user", USER, false, 0, "grantor1", USER),
            new RolePrincipalGrant("role2", "role1", ROLE, true, 0, "grantor2", ROLE));
    public static final List<String> PARTITION_COLUMN_NAMES = ImmutableList.of(TEST_COLUMN);

    private static final StorageDescriptor DEFAULT_STORAGE_DESCRIPTOR =
            new StorageDescriptor(ImmutableList.of(new FieldSchema(TEST_COLUMN, "bigint", "")), "", null, null, false, 0, new SerDeInfo(TEST_TABLE, null, ImmutableMap.of()), null, null, ImmutableMap.of());

    private final AtomicInteger accessCount = new AtomicInteger();
    private final Map<SchemaTableName, Map<String, ColumnStatisticsObj>> columnStatistics = new HashMap<>();
    private final Map<SchemaTableName, Map<String, Map<String, ColumnStatisticsObj>>> databaseTablePartitionColumnStatistics = new HashMap<>();

    private boolean throwException;

    public MockThriftMetastoreClient()
    {
        mockColumnStats(TEST_DATABASE, TEST_TABLE, ImmutableMap.of(TEST_COLUMN, createLongColumnStats()));
        mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION1, ImmutableMap.of(TEST_COLUMN, createLongColumnStats()));
    }

    public void mockColumnStats(String database, String table, Map<String, ColumnStatisticsData> columnStatistics)
    {
        this.columnStatistics.compute(new SchemaTableName(database, table), (ignored, oldColumnStats) -> {
            if (oldColumnStats == null) {
                oldColumnStats = new HashMap<>();
            }
            oldColumnStats.putAll(Maps.transformEntries(columnStatistics, (columnName, stats) -> {
                ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
                statsObj.setColName(columnName);
                statsObj.setStatsData(stats);
                return statsObj;
            }));
            return oldColumnStats;
        });
    }

    public void mockPartitionColumnStats(String database, String table, String partitionName, Map<String, ColumnStatisticsData> columnStatistics)
    {
        Map<String, Map<String, ColumnStatisticsObj>> tablePartitionColumnStatistics = databaseTablePartitionColumnStatistics.computeIfAbsent(new SchemaTableName(database, table), key -> new HashMap<>());
        tablePartitionColumnStatistics.compute(partitionName, (ignored, oldColumnStats) -> {
            if (oldColumnStats == null) {
                oldColumnStats = new HashMap<>();
            }
            oldColumnStats.putAll(Maps.transformEntries(columnStatistics, (columnName, stats) -> {
                ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
                statsObj.setColName(columnName);
                statsObj.setStatsData(stats);
                return statsObj;
            }));
            return oldColumnStats;
        });
    }

    private static ColumnStatisticsData createLongColumnStats()
    {
        ColumnStatisticsData data = new ColumnStatisticsData();
        data.setLongStats(new LongColumnStatsData());
        return data;
    }

    private static ColumnStatisticsObj createTestStats()
    {
        ColumnStatisticsObj stats = new ColumnStatisticsObj();
        ColumnStatisticsData data = new ColumnStatisticsData();
        data.setLongStats(new LongColumnStatsData());
        stats.setStatsData(data);
        stats.setColName(TEST_COLUMN);
        return stats;
    }

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    @Override
    public List<String> getAllDatabases()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_DATABASE);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE)) {
            return ImmutableList.of(); // As specified by Hive specification
        }
        return ImmutableList.of(TEST_TABLE);
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Database getDatabase(String name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!name.equals(TEST_DATABASE)) {
            throw new NoSuchObjectException();
        }
        return new Database(TEST_DATABASE, null, null, null);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return new Table(
                TEST_TABLE,
                TEST_DATABASE,
                "",
                0,
                0,
                0,
                DEFAULT_STORAGE_DESCRIPTOR,
                ImmutableList.of(new FieldSchema("key", "string", null)),
                ImmutableMap.of(),
                "",
                "",
                TableType.MANAGED_TABLE.name());
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
    {
        return ImmutableList.of(new FieldSchema("key", "string", null));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }

        Map<String, ColumnStatisticsObj> columnStatistics = this.columnStatistics.get(new SchemaTableName(databaseName, tableName));

        if (columnStatistics == null || !columnStatistics.keySet().containsAll(columnNames)) {
            throw new NoSuchObjectException();
        }

        return columnNames.stream().map(columnStatistics::get).collect(toImmutableList());
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        ImmutableMap.Builder<String, List<ColumnStatisticsObj>> result = ImmutableMap.builder();
        Map<String, Map<String, ColumnStatisticsObj>> tablePartitionColumnStatistics = databaseTablePartitionColumnStatistics.get(new SchemaTableName(databaseName, tableName));
        if (tablePartitionColumnStatistics == null) {
            throw new NoSuchObjectException();
        }

        for (String partition : partitionNames) {
            Map<String, ColumnStatisticsObj> columnStatistics = tablePartitionColumnStatistics.get(partition);
            if (columnStatistics == null || !columnStatistics.keySet().containsAll(columnNames)) {
                throw new NoSuchObjectException();
            }
            result.put(partition, ImmutableList.copyOf(columnStatistics.values()));
        }

        return result.buildOrThrow();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        accessCount.incrementAndGet();
        // No-op
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tableName)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String dbName, String tableName, List<String> partValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) ||
                !ImmutableSet.of(TEST_PARTITION_VALUES1, TEST_PARTITION_VALUES2, TEST_PARTITION_VALUES3).contains(partitionValues)) {
            throw new NoSuchObjectException();
        }
        return new Partition(partitionValues, TEST_DATABASE, TEST_TABLE, 0, 0, DEFAULT_STORAGE_DESCRIPTOR, ImmutableMap.of());
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> names)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3).containsAll(names)) {
            throw new NoSuchObjectException();
        }
        return names.stream()
                .map(MockThriftMetastoreClient::getPartitionsByNamesUnchecked)
                .collect(toImmutableList());
    }

    private static Partition getPartitionsByNamesUnchecked(String name)
    {
        try {
            return new Partition(
                    ImmutableList.copyOf(Warehouse.getPartValuesFromPartName(name)),
                    TEST_DATABASE,
                    TEST_TABLE,
                    0,
                    0,
                    DEFAULT_STORAGE_DESCRIPTOR,
                    ImmutableMap.of());
        }
        catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Table table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
    {
        // No-op, make sure the cache invalidation logic in CachingHiveMetastore will be passed through
    }

    @Override
    public void alterTableWithEnvironmentContext(String databaseName, String tableName, Table newTable, EnvironmentContext context)
    {
        // No-op, accessCount already increased by getTable call
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        accessCount.incrementAndGet();
        // No-op
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRoleNames()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLES;
    }

    @Override
    public void createRole(String role, String grantor)
    {
        // No-op
    }

    @Override
    public void dropRole(String role)
    {
        // No-op
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag, boolean revokeGrantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
    {
        // No-op
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
    {
        // No-op
    }

    @Override
    public List<RolePrincipalGrant> listGrantedPrincipals(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLE_GRANTS;
    }

    @Override
    public void close()
    {
        // No-op
    }

    @Override
    public void setUGI(String userName)
    {
        // No-op
    }

    @Override
    public long openTransaction(String user)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResponse checkLock(long lockId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unlock(long lockId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, EnvironmentContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDelegationToken(String userName)
    {
        throw new UnsupportedOperationException();
    }
}
