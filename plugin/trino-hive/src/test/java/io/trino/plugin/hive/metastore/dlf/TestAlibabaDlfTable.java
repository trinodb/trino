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
package io.trino.plugin.hive.metastore.dlf;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.PrincipalType;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.acid.AcidOperation.NONE;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.createMetastoreColumnStatistics;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAlibabaDlfTable
{
    private static final String TEST_DB = TestingAlibabaDlfUtils.TEST_DB;
    private static HiveMetastore client;
    private static Database testDb;
    private static AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig;
    public static final AcidTransaction NO_ACID_TRANSACTION = new AcidTransaction(NONE, 0, 0, Optional.empty());

    @BeforeClass
    public static void setUp() throws TException, IOException
    {
        alibabaDlfMetaStoreConfig = new AlibabaDlfMetaStoreConfig();
        client = TestingAlibabaDlfUtils.getDlfClient(alibabaDlfMetaStoreConfig);
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(TEST_DB).toString();
        testDb = TestingAlibabaDlfUtils.getDatabase(TEST_DB, location);
        try {
            client.dropDatabase(TEST_DB, true);
        }
        catch (SchemaNotFoundException e) {
            client.createDatabase(testDb);
        }
    }

    @AfterMethod
    public static void cleanUpCase() throws TException, IOException
    {
        List<String> allTables = client.getAllTables(TEST_DB);
        allTables.forEach(t -> {
            try {
                client.dropTable(TEST_DB, t, true);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterClass
    public static void cleanUp() throws TException, IOException
    {
        try {
            client.dropDatabase(TEST_DB, true);
        }
        catch (SchemaNotFoundException e) {
        }
    }

    private static OptionalLong firstPresent(OptionalLong first, OptionalLong second)
    {
        return first.isPresent() ? first : second;
    }

    @Test
    public void testCreatTable() throws TException, IOException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
        // create a table
        client.createTable(table, null);

        // check the info about the created table
        Optional<Table> check = client.getTable(TEST_DB, tblName);
        assertTrue(check.isPresent());
        assertEquals(TEST_DB, check.get().getDatabaseName());
        assertEquals(tblName, check.get().getTableName());
    }

    @Test
    public void testTableCloseLog() throws IOException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
        // create a table
        ProxyLogUtils originProxyLogUtils = ProxyLogUtils.getProxyLogUtils();
        try {
            ProxyLogUtils.setProxyLogUtils(null);
            AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfigNew = new AlibabaDlfMetaStoreConfig();
            //close dlf log
            alibabaDlfMetaStoreConfigNew.setEnableRecordLog(false);
            HiveMetastore newClient = TestingAlibabaDlfUtils.getDlfClient(alibabaDlfMetaStoreConfigNew);
            // create a table
            System.out.println("close log start ++++++++++++++++++++++");
            assertFalse(ProxyLogUtils.getProxyLogUtils().isRecordLog());
            newClient.createTable(table, null);

            // check the info about the created table
            Optional<Table> check = newClient.getTable(TEST_DB, tblName);
            assertTrue(check.isPresent());
            assertEquals(TEST_DB, check.get().getDatabaseName());
            assertEquals(tblName, check.get().getTableName());

            newClient.dropTable(TEST_DB, tblName, true);
        }
        finally {
            ProxyLogUtils.setProxyLogUtils(originProxyLogUtils);
            System.out.println("close log end ++++++++++++++++++++++");
        }
    }

    @Test
    public void testTableCloseActionLog() throws IOException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
        // create a table
        ProxyLogUtils originProxyLogUtils = ProxyLogUtils.getProxyLogUtils();
        try {
            ProxyLogUtils.setProxyLogUtils(null);
            AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfigNew = new AlibabaDlfMetaStoreConfig();
            //open action parameter log
            alibabaDlfMetaStoreConfigNew.setEnableRecordActionLog(true);
            HiveMetastore newClient = TestingAlibabaDlfUtils.getDlfClient(alibabaDlfMetaStoreConfigNew);
            // create a table
            System.out.println("close log start ++++++++++++++++++++++");
            assertTrue(ProxyLogUtils.getProxyLogUtils().isRecordActionLog());
            newClient.createTable(table, null);

            // check the info about the created table
            Optional<Table> check = newClient.getTable(TEST_DB, tblName);
            assertTrue(check.isPresent());
            assertEquals(TEST_DB, check.get().getDatabaseName());
            assertEquals(tblName, check.get().getTableName());
            newClient.dropTable(TEST_DB, tblName, true);
        }
        finally {
            ProxyLogUtils.setProxyLogUtils(originProxyLogUtils);
            System.out.println("close log end ++++++++++++++++++++++");
        }
    }

    @Test
    public void testCreateTableRepeatedly() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partColumns);
        // create a table
        client.createTable(table, null);
        // repeatedly create table
        assertThatThrownBy(() -> client.createTable(table, null))
                .isInstanceOf(TableAlreadyExistsException.class);
    }

    @Test
    public void testCreatePartitionTable() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partColumns);
        // create a table
        client.createTable(table, null);

        // check the partition table
        Optional<Table> checkPartTable = client.getTable(TEST_DB, tblName);
        Map<String, HiveType> returned = checkPartTable.get().getPartitionColumns().stream()
                .collect(Collectors.toMap(Column::getName, Column::getType));
        Map<String, String> returned2 = ImmutableMap.of("ds", returned.get("ds").toString());
        assertEquals(partColumns, returned2);
    }

    @Test
    public void testDropNonExistTable() throws TException, IOException
    {
        // drop a non exist table
        String nonExist = "non_exist";
        try {
            client.dropTable(TEST_DB, nonExist, true);
        }
        catch (TableNotFoundException e) {
        }
    }

    @Test
    public void testDeleteDirWhenDropTable() throws TException, IOException
    {
        // create a table
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partColumns);
        // create a table
        client.createTable(table, null);

        // drop the created table and delete the data
        client.dropTable(TEST_DB, tblName, true);

        // assert the table has been dropped successfully
        assertFalse(client.getTable(TEST_DB, tblName).isPresent());
    }

    @Test
    public void testDropPartitionTable() throws TException
    {
        // create a partition table
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partColumns);
        // create a table
        client.createTable(table, null);

        // drop the created table and delete the data
        client.dropTable(TEST_DB, tblName, true);

        // assert the table has been dropped successfully
        assertFalse(client.getTable(TEST_DB, tblName).isPresent());
    }

    @Test
    public void testGetNonExistTable() throws TException
    {
        // get a non exist table
        assertFalse(client.getTable("non_exist_db", "non_exist").isPresent());
        assertFalse(client.getTable(TEST_DB, "non_exist").isPresent());
    }

    @Test
    public void testGetAllTables() throws TException
    {
        String tbl1 = TestingAlibabaDlfUtils.TEST_TABLE + "1";
        String tbl2 = TestingAlibabaDlfUtils.TEST_TABLE + "2";
        String tbl3 = TestingAlibabaDlfUtils.TEST_TABLE + "3";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table1 = TestingAlibabaDlfUtils.getTable(TEST_DB, tbl1, columns, partColumns);
        Table table2 = TestingAlibabaDlfUtils.getTable(TEST_DB, tbl2, columns, partColumns);
        Table table3 = TestingAlibabaDlfUtils.getTable(TEST_DB, tbl3, columns, partColumns);

        // create the tables
        client.createTable(table1, null);
        client.createTable(table2, null);
        client.createTable(table3, null);

        List<String> tables = client.getAllTables(TEST_DB);
        List<String> expected = ImmutableList.of(tbl1, tbl2, tbl3);
        tables.sort(String::compareTo);
        assertEquals(expected, tables);
    }

    @Test
    public void testAlterNonExistTable() throws TException
    {
        String nonExist = "non_exist";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, nonExist, columns, partColumns);

        // alter a non exist table
        assertThatThrownBy(() -> client.replaceTable(TEST_DB, nonExist, table, null))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    public void testAlterTableRenameTable() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        String newTblName = "new" + TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        // alter table: rename
        client.renameTable(TEST_DB, tblName, TEST_DB, newTblName);

        // check renamed table
        Optional<Table> checkRenamed = client.getTable(TEST_DB, newTblName);
        assertTrue(checkRenamed.isPresent());
        assertEquals(newTblName, checkRenamed.get().getTableName());
        assertTrue(checkRenamed.get().getParameters().get("key").equals("value"));
    }

    @Test
    public void testAlterTableCommentTable() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        // alter comment
        String comment = "test comment";
        client.commentTable(TEST_DB, tblName, Optional.of(comment));

        // check the comment
        Optional<Table> checkTable = client.getTable(TEST_DB, tblName);
        assertEquals(checkTable.get().getParameters().get("comment"), comment);
    }

    @Test
    public void testAlterTableCommentColumn() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        // alter comment
        String col = "id";
        String comment = "test comment";
        client.commentColumn(TEST_DB, tblName, col, Optional.of(comment));

        // check the comment
        Optional<Table> checkTable = client.getTable(TEST_DB, tblName);
        assertEquals(checkTable.get().getColumn(col).get().getComment().get(), comment);
    }

    @Test
    public void testAlterTableSetTableOwner() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        // alter table: set table owner
        String ownerName = "testPrincipal";
        // client.setTableOwner(TEST_DB, tblName, HivePrincipal.from(new TrinoPrincipal(PrincipalType.USER, ownerName)));
        client.setTableOwner(TEST_DB, tblName, new HivePrincipal(PrincipalType.USER, ownerName));

        // check the comment
        Optional<Table> checkTable = client.getTable(TEST_DB, tblName);
        System.out.println(checkTable.get());
        assertEquals(checkTable.get().getOwner().get(), ownerName);
    }

    @Test
    public void testAlterTableAddColumns() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        // alter table: add a new column
        Map<String, String> newColumns = ImmutableMap.of("id", "int", "name", "string", "phone", "string");
        Table colAdded = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, newColumns, partitionColumns);
        client.replaceTable(TEST_DB, tblName, colAdded, null);

        // check the new columns
        Optional<Table> checkColAdded = client.getTable(TEST_DB, tblName);
        List<Column> newCols = checkColAdded.get().getDataColumns();
        List<Column> expected = ImmutableList.of(new Column("id", HiveType.HIVE_INT, Optional.empty()), new Column("name", HiveType.HIVE_STRING, Optional.empty()), new Column("phone", HiveType.HIVE_STRING, Optional.empty()));
        assertEquals(expected, newCols);
    }

    @Test
    public void testGetViews() throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        String tblName2 = TestingAlibabaDlfUtils.TEST_TABLE + "2";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns, TableType.VIRTUAL_VIEW.name());
        Table table2 = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName2, columns, partitionColumns);

        // create the table
        client.createTable(table, null);
        client.createTable(table2, null);
        assertTrue(client.getTable(TEST_DB, tblName).get().getTableType().equals(TableType.VIRTUAL_VIEW.name()));
        assertTrue(client.getTable(TEST_DB, tblName2).get().getTableType().equals(table2.getTableType()));
        List<String> views = client.getAllViews(TEST_DB);
        assertTrue(views.size() == 1 && views.get(0).equals(tblName));
    }

    @Test
    public void testTablePartition()
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);

        // create the table
        client.createTable(table, null);

        List<String> values = ImmutableList.of("2020");
        Optional<Partition> partition = client.getPartition(table, values);
        assertFalse(partition.isPresent());
    }

    @Test
    public void testTableColumnStatisticsWithoutVector() throws TException
    {
        testTableColumnStatistics(alibabaDlfMetaStoreConfig);
    }

    private void testTableColumnStatistics(AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig) throws TException
    {
        // create table
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "int");
        columns.put("name", "string");
        columns.put("birth", "date");
        columns.put("height", "double");
        columns.put("weight", "decimal(10,3)");
        columns.put("is_male", "boolean");
        columns.put("shopping", "binary");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, ImmutableMap.of());
        // create the table
        client.createTable(table, null);
        // update statistics
        ColumnStatistics columnStatistics = new ColumnStatistics();
        ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
        columnStatisticsDesc.setIsTblLevel(true);
        columnStatisticsDesc.setTableName(tblName);
        columnStatisticsDesc.setDbName(TEST_DB);
        columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);
        List<ColumnStatisticsObj> columnStatisticsObjs = new ArrayList<>();
        ColumnStatisticsObj longColumnStatisticsObj = new ColumnStatisticsObj();
        longColumnStatisticsObj.setColName("id");
        longColumnStatisticsObj.setColType("int");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        byte[] longBitVectors = new byte[5];
        longBitVectors[0] = 11;
        longColumnStatsData.setBitVectors(longBitVectors);
        longColumnStatsData.setLowValue(1);
        longColumnStatsData.setHighValue(5);
        longColumnStatsData.setNumDVs(20);
        longColumnStatsData.setNumNulls(20);
        columnStatisticsData.setLongStats(longColumnStatsData);
        longColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(longColumnStatisticsObj);
        ColumnStatisticsObj stringColumnStatisticsObj = new ColumnStatisticsObj();
        stringColumnStatisticsObj.setColName("name");
        stringColumnStatisticsObj.setColType("string");
        columnStatisticsData = new ColumnStatisticsData();
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        byte[] stringBitVectors = new byte[5];
        stringBitVectors[0] = 22;
        stringColumnStatsData.setBitVectors(stringBitVectors);
        stringColumnStatsData.setMaxColLen(100);
        stringColumnStatsData.setAvgColLen(50.33);
        stringColumnStatsData.setNumDVs(21);
        stringColumnStatsData.setNumNulls(21);
        columnStatisticsData.setStringStats(stringColumnStatsData);
        stringColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(stringColumnStatisticsObj);
        ColumnStatisticsObj dateColumnStatisticsObj = new ColumnStatisticsObj();
        dateColumnStatisticsObj.setColName("birth");
        dateColumnStatisticsObj.setColType("date");
        columnStatisticsData = new ColumnStatisticsData();
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        byte[] dateBitVectors = new byte[5];
        dateBitVectors[0] = 33;
        dateColumnStatsData.setBitVectors(dateBitVectors);
        dateColumnStatsData.setLowValue(new Date(18590));
        dateColumnStatsData.setHighValue(new Date(18585));
        dateColumnStatsData.setNumDVs(22);
        dateColumnStatsData.setNumNulls(22);
        columnStatisticsData.setDateStats(dateColumnStatsData);
        dateColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(dateColumnStatisticsObj);
        ColumnStatisticsObj doubleColumnStatisticsObj = new ColumnStatisticsObj();
        doubleColumnStatisticsObj.setColName("height");
        doubleColumnStatisticsObj.setColType("double");
        columnStatisticsData = new ColumnStatisticsData();
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        byte[] doubleBitVectors = new byte[5];
        doubleBitVectors[0] = 44;
        doubleColumnStatsData.setBitVectors(doubleBitVectors);
        doubleColumnStatsData.setLowValue(170.15);
        doubleColumnStatsData.setHighValue(190.23);
        doubleColumnStatsData.setNumDVs(23);
        doubleColumnStatsData.setNumNulls(23);
        columnStatisticsData.setDoubleStats(doubleColumnStatsData);
        doubleColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(doubleColumnStatisticsObj);
        ColumnStatisticsObj decimalColumnStatisticsObj = new ColumnStatisticsObj();
        decimalColumnStatisticsObj.setColName("weight");
        decimalColumnStatisticsObj.setColType("decimal(10,3)");
        columnStatisticsData = new ColumnStatisticsData();
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        byte[] decimalBitVectors = new byte[5];
        decimalBitVectors[0] = 55;
        decimalColumnStatsData.setBitVectors(decimalBitVectors);
        decimalColumnStatsData.setLowValue(new Decimal((short) 3, ByteBuffer.wrap(new BigDecimal("128.888").unscaledValue().toByteArray())));
        decimalColumnStatsData.setHighValue(new Decimal((short) 3, ByteBuffer.wrap(new BigDecimal("178.888").unscaledValue().toByteArray())));
        decimalColumnStatsData.setNumDVs(24);
        decimalColumnStatsData.setNumNulls(24);
        columnStatisticsData.setDecimalStats(decimalColumnStatsData);
        decimalColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(decimalColumnStatisticsObj);
        ColumnStatisticsObj booleanColumnStatisticsObj = new ColumnStatisticsObj();
        booleanColumnStatisticsObj.setColName("is_male");
        booleanColumnStatisticsObj.setColType("boolean");
        columnStatisticsData = new ColumnStatisticsData();
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setNumFalses(100);
        booleanColumnStatsData.setNumTrues(50);
        booleanColumnStatsData.setNumNulls(20);
        columnStatisticsData.setBooleanStats(booleanColumnStatsData);
        booleanColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(booleanColumnStatisticsObj);
        ColumnStatisticsObj binaryColumnStatisticsObj = new ColumnStatisticsObj();
        binaryColumnStatisticsObj.setColName("shopping");
        binaryColumnStatisticsObj.setColType("binary");
        columnStatisticsData = new ColumnStatisticsData();
        BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
        binaryColumnStatsData.setMaxColLen(100);
        binaryColumnStatsData.setAvgColLen(50.33);
        binaryColumnStatsData.setNumNulls(26);
        columnStatisticsData.setBinaryStats(binaryColumnStatsData);
        binaryColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(binaryColumnStatisticsObj);
        columnStatistics.setStatsDesc(columnStatisticsDesc);
        columnStatistics.setStatsObj(columnStatisticsObjs);

        OptionalLong rowCount = OptionalLong.of(200L);
        HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(10, rowCount.getAsLong(), 1000, 5000);
        Map<String, HiveColumnStatistics> hiveColumnStatistics = columnStatisticsObjs.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, rowCount)));
        PartitionStatistics newPartitionStats = new PartitionStatistics(hiveBasicStatistics, hiveColumnStatistics);

        client.updateTableStatistics(TEST_DB, tblName, NO_ACID_TRANSACTION, oldPartitionStats -> updatePartitionStatistics(oldPartitionStats, newPartitionStats));
        // get statistics
        List<String> colNames = new ArrayList<>();
        colNames.add("id");
        colNames.add("name");
        colNames.add("birth");
        colNames.add("height");
        colNames.add("weight");
        colNames.add("is_male");
        colNames.add("shopping");

        Optional<Table> tableGet = client.getTable(TEST_DB, tblName);
        PartitionStatistics getPartitionStatistics = client.getTableStatistics(tableGet.get());

        HiveBasicStatistics basicStatistics = getPartitionStatistics.getBasicStatistics();

        OptionalLong rowCountGet = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = getPartitionStatistics.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), table.getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCountGet))
                .collect(toImmutableList());

        assertEquals(rowCount, rowCountGet, "rowcount:" + rowCount + ":" + rowCountGet);

        for (ColumnStatisticsObj obj : metastoreColumnStatistics) {
            if ("id".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(longColumnStatisticsObj.getColName()), "longColName");
                assertTrue(obj.getColType().equals(longColumnStatisticsObj.getColType()), "longColType:");
                assertTrue(obj.getStatsData().isSetLongStats(), "longDataType:");
                LongColumnStatsData getLongColumnStatsData = obj.getStatsData().getLongStats();
                if (!alibabaDlfMetaStoreConfig.getEnableBitVector()) {
                    assertTrue(getLongColumnStatsData.getBitVectors() == null, "longBitVectors");
                }
                else {
                    assertTrue(getLongColumnStatsData.bufferForBitVectors().equals(longColumnStatsData.bufferForBitVectors()), "longBitVectors");
                }
                assertTrue(getLongColumnStatsData.getHighValue() == longColumnStatsData.getHighValue(), "longHighValue");
                assertTrue(getLongColumnStatsData.getLowValue() == longColumnStatsData.getLowValue(), "longLowValue");
                assertTrue(getLongColumnStatsData.getNumDVs() == longColumnStatsData.getNumDVs(), "longNumDVs");
                assertTrue(getLongColumnStatsData.getNumNulls() == longColumnStatsData.getNumNulls(), "longNumNulls");
            }
            else if ("name".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(stringColumnStatisticsObj.getColName()), "stringColName");
                assertTrue(obj.getColType().equals(stringColumnStatisticsObj.getColType()), "stringColType:");
                assertTrue(obj.getStatsData().isSetStringStats(), "stringDataType:");
                StringColumnStatsData getStringColumnStatsData = obj.getStatsData().getStringStats();
                if (!alibabaDlfMetaStoreConfig.getEnableBitVector()) {
                    assertTrue(getStringColumnStatsData.getBitVectors() == null, "stringBitVector");
                }
                else {
                    assertTrue(getStringColumnStatsData.bufferForBitVectors().equals(stringColumnStatsData.bufferForBitVectors()), "stringBitVector");
                }
                assertTrue(getStringColumnStatsData.getMaxColLen() == stringColumnStatsData.getMaxColLen(), "stringMaxColLen");
                assertTrue(Math.abs(getStringColumnStatsData.getAvgColLen() - stringColumnStatsData.getAvgColLen()) <= 0.01, "stringAvgColLen:" + Math.abs(getStringColumnStatsData.getAvgColLen() - stringColumnStatsData.getAvgColLen()));
                assertTrue(getStringColumnStatsData.getNumDVs() == stringColumnStatsData.getNumDVs(), "stringNumDVs");
                assertTrue(getStringColumnStatsData.getNumNulls() == stringColumnStatsData.getNumNulls(), "stringNumNulls");
            }
            else if ("birth".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(dateColumnStatisticsObj.getColName()), "dateColName");
                assertTrue(obj.getColType().equals(dateColumnStatisticsObj.getColType()), "dateColType:");
                assertTrue(obj.getStatsData().isSetDateStats(), "dateDataType:");
                DateColumnStatsData getDateColumnStatsData = obj.getStatsData().getDateStats();
                if (!alibabaDlfMetaStoreConfig.getEnableBitVector()) {
                    assertTrue(getDateColumnStatsData.getBitVectors() == null);
                }
                else {
                    assertTrue(getDateColumnStatsData.bufferForBitVectors().equals(dateColumnStatsData.bufferForBitVectors()));
                }
                assertTrue(getDateColumnStatsData.getLowValue().equals(dateColumnStatsData.getLowValue()), "dateLowValue");
                assertTrue(getDateColumnStatsData.getHighValue().equals(dateColumnStatsData.getHighValue()), "dateHighValue");
                assertTrue(getDateColumnStatsData.getNumDVs() == dateColumnStatsData.getNumDVs(), "dateNumDVs");
                assertTrue(getDateColumnStatsData.getNumNulls() == dateColumnStatsData.getNumNulls(), "dateNumNulls");
            }
            else if ("height".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(doubleColumnStatisticsObj.getColName()), "doubleColName");
                assertTrue(obj.getColType().equals(doubleColumnStatisticsObj.getColType()), "doubleColType:");
                assertTrue(obj.getStatsData().isSetDoubleStats(), "doubleDataType:");
                DoubleColumnStatsData getDoubleColumnStatsData = obj.getStatsData().getDoubleStats();
                if (!alibabaDlfMetaStoreConfig.getEnableBitVector()) {
                    assertTrue(getDoubleColumnStatsData.getBitVectors() == null, "doubleBitVectors");
                }
                else {
                    assertTrue(getDoubleColumnStatsData.bufferForBitVectors().equals(doubleColumnStatsData.bufferForBitVectors()), "doubleBitVectors");
                }
                assertTrue(Math.abs(getDoubleColumnStatsData.getLowValue() - doubleColumnStatsData.getLowValue()) <= 0.01, "doubleLowValue");
                assertTrue(Math.abs(getDoubleColumnStatsData.getHighValue() - doubleColumnStatsData.getHighValue()) <= 0.01, "doubleHighValue");
                assertTrue(getDoubleColumnStatsData.getNumDVs() == doubleColumnStatsData.getNumDVs(), "doubleNumDVs");
                assertTrue(getDoubleColumnStatsData.getNumNulls() == doubleColumnStatsData.getNumNulls(), "doubleNumNulls");
            }
            else if ("weight".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(decimalColumnStatisticsObj.getColName()), "decimalColName");
                assertTrue(obj.getColType().equals(decimalColumnStatisticsObj.getColType()), "decimalColType:");
                assertTrue(obj.getStatsData().isSetDecimalStats(), "decimalDataType:");
                DecimalColumnStatsData getDecimalColumnStatsData = obj.getStatsData().getDecimalStats();
                if (!alibabaDlfMetaStoreConfig.getEnableBitVector()) {
                    assertTrue(getDecimalColumnStatsData.getBitVectors() == null, "decimalBitVectors");
                }
                else {
                    assertTrue(getDecimalColumnStatsData.bufferForBitVectors().equals(decimalColumnStatsData.bufferForBitVectors()), "decimalBitVectors");
                }
                assertTrue(getDecimalColumnStatsData.getLowValue().equals(decimalColumnStatsData.getLowValue()), "decimalLowValue");
                assertTrue(getDecimalColumnStatsData.getHighValue().equals(decimalColumnStatsData.getHighValue()), "decimalHighValue");
                assertTrue(getDecimalColumnStatsData.getNumDVs() == decimalColumnStatsData.getNumDVs(), "decimalNumDvs");
                assertTrue(getDecimalColumnStatsData.getNumNulls() == decimalColumnStatsData.getNumNulls(), "decimalNumNulls");
            }
            else if ("is_male".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(booleanColumnStatisticsObj.getColName()), "booleanColName");
                assertTrue(obj.getColType().equals(booleanColumnStatisticsObj.getColType()), "booleanColType:");
                assertTrue(obj.getStatsData().isSetBooleanStats(), "booleanDataType:");
                BooleanColumnStatsData getBooleanColumnStatsData = obj.getStatsData().getBooleanStats();
                assertTrue(getBooleanColumnStatsData.getBitVectors() == null, "booleanBitVectors");
                assertTrue(getBooleanColumnStatsData.getNumFalses() == booleanColumnStatsData.getNumFalses(), "booleanNumFalses");
                assertTrue(getBooleanColumnStatsData.getNumTrues() == booleanColumnStatsData.getNumTrues(), "booleanNumTrues");
                assertTrue(getBooleanColumnStatsData.getNumNulls() == booleanColumnStatsData.getNumNulls(), "booleanNumNulls");
            }
            else if ("shopping".equals(obj.getColName())) {
                assertTrue(obj.getColName().equals(binaryColumnStatisticsObj.getColName()), "binaryColName");
                assertTrue(obj.getColType().equals(binaryColumnStatisticsObj.getColType()), "binaryColType:");
                assertTrue(obj.getStatsData().isSetBinaryStats(), "binaryDataType:");
                BinaryColumnStatsData getBinaryColumnStatsData = obj.getStatsData().getBinaryStats();
                assertTrue(getBinaryColumnStatsData.getBitVectors() == null, "binaryBitVectors");
                assertTrue(getBinaryColumnStatsData.getMaxColLen() == binaryColumnStatsData.getMaxColLen(), "binaryMaxColLen");
                assertTrue(Math.abs(getBinaryColumnStatsData.getAvgColLen() - binaryColumnStatsData.getAvgColLen()) <= 0.01, "binaryAvgColLen");
                assertTrue(getBinaryColumnStatsData.getNumNulls() == binaryColumnStatsData.getNumNulls(), "binaryNumNulls");
            }
            else {
                assertTrue(1 == 0);
            }
        }
        // delete statistics
        PartitionStatistics partitionStatisticsForDelete = new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of());

        client.updateTableStatistics(TEST_DB, tblName, NO_ACID_TRANSACTION, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatisticsForDelete));

        // get statistics
        Optional<Table> tableGetAfterDelete = client.getTable(TEST_DB, tblName);
        PartitionStatistics getPartitionStatisticsAfterDelete = client.getTableStatistics(tableGetAfterDelete.get());

        HiveBasicStatistics basicStatisticsAfterDelete = getPartitionStatisticsAfterDelete.getBasicStatistics();
        OptionalLong rowCountGetAfterDelete = basicStatisticsAfterDelete.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatisticsAfterDelete = getPartitionStatisticsAfterDelete.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), tableGetAfterDelete.get().getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCountGetAfterDelete))
                .collect(toImmutableList());

        assertEquals(0, metastoreColumnStatisticsAfterDelete.size());
    }

    private PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
    {
        HiveBasicStatistics oldBasicStatistics = oldPartitionStats.getBasicStatistics();
        HiveBasicStatistics newBasicStatistics = newPartitionStats.getBasicStatistics();
        HiveBasicStatistics updatedBasicStatistics = new HiveBasicStatistics(
                firstPresent(newBasicStatistics.getFileCount(), oldBasicStatistics.getFileCount()),
                firstPresent(newBasicStatistics.getRowCount(), oldBasicStatistics.getRowCount()),
                firstPresent(newBasicStatistics.getInMemoryDataSizeInBytes(), oldBasicStatistics.getInMemoryDataSizeInBytes()),
                firstPresent(newBasicStatistics.getOnDiskDataSizeInBytes(), oldBasicStatistics.getOnDiskDataSizeInBytes()));
        Map<String, HiveColumnStatistics> updatedColumnStatistics =
                updateColumnStatistics(oldPartitionStats.getColumnStatistics(), newPartitionStats.getColumnStatistics());
        return new PartitionStatistics(updatedBasicStatistics, updatedColumnStatistics);
    }

    private PartitionStatistics updatePartitionStatisticsForDelete(PartitionStatistics newPartitionStats)
    {
        return newPartitionStats;
    }

    private Map<String, HiveColumnStatistics> updateColumnStatistics(Map<String, HiveColumnStatistics> oldColumnStats, Map<String, HiveColumnStatistics> newColumnStats)
    {
        Map<String, HiveColumnStatistics> result = new HashMap<>(oldColumnStats);
        result.putAll(newColumnStats);
        return ImmutableMap.copyOf(result);
    }
}
