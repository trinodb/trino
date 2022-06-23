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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.acid.AcidOperation.NONE;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.createMetastoreColumnStatistics;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestAlibabaDlfPartition
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
        catch (Exception e) {
        }
        client.createDatabase(testDb);
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
    public void testGetPartitions() throws TException, IOException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
        // create a table
        client.createTable(table, null);
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        Partition partition = new Partition(TEST_DB, tblName, Lists.newArrayList("20121210"), table.getStorage(), table.getDataColumns(), new HashMap<String, String>());

        HiveBasicStatistics basicStatistics = new HiveBasicStatistics(0, 0, 0, 0);
        Map<String, HiveColumnStatistics> columnStatistics = new HashMap<>();
        PartitionStatistics statistics = new PartitionStatistics(basicStatistics, columnStatistics);
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, "ds=20121210", statistics);
        partitions.add(partitionWithStatistics);
        client.addPartitions(TEST_DB, tblName, partitions);

        List<String> values = ImmutableList.of("20121210");
        Optional<Partition> partitionGet = client.getPartition(table, values);
        assertTrue(partitionGet.isPresent() && partitionGet.get().getTableName().equals(tblName));

        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        List<String> partsInput = new ArrayList<>();
        partsInput.add("20121210");
        Optional<List<String>> partsGets = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("20121210")))));
        assertTrue(partsGets.isPresent() && partsGets.get().size() == 1);

        List<String> partsInput2 = new ArrayList<>();
        partsInput2.add("");
        Optional<List<String>> partsGets2 = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("")))));
        assertTrue(partsGets2.isPresent() && partsGets2.get().size() == 1);
    }

    @Test
    public void testPartitionColumnStatisticsWithoutEnableVector() throws TException
    {
        testPartitionColumnStatistics(alibabaDlfMetaStoreConfig);
    }

    private void testPartitionColumnStatistics(AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig) throws TException
    {
        // create table
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        String partValName = "20121210 12:23++"; // if partvalname contains "/" it will throw Exception
        String partCol = "ds";
        String partColType = "string";
        String partName = partCol + "=" + partValName;
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "int");
        columns.put("name", "string");
        columns.put("birth", "date");
        columns.put("height", "double");
        columns.put("weight", "decimal(10,3)");
        columns.put("is_male", "boolean");
        columns.put("shopping", "binary");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, ImmutableMap.of(partCol, partColType));
        // create the table
        client.createTable(table, null);
        //add partition
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        Partition partition = new Partition(TEST_DB, tblName, Lists.newArrayList(partValName), table.getStorage(), table.getDataColumns(), new HashMap<String, String>());

        HiveBasicStatistics basicStatisticsHive = new HiveBasicStatistics(1, 1, 10, 10);
        Map<String, HiveColumnStatistics> columnStatisticsHive = new HashMap<>();
        PartitionStatistics statistics = new PartitionStatistics(basicStatisticsHive, columnStatisticsHive);
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, partName, statistics);
        partitions.add(partitionWithStatistics);
        client.addPartitions(TEST_DB, tblName, partitions);
        String realPartVals = client.getPartitionNamesByFilter(TEST_DB, tblName, ImmutableList.of(partCol),
                TupleDomain.withColumnDomains(ImmutableMap.of(partCol, singleValue(VARCHAR, utf8Slice(partValName))))).get().get(0);
        // update statistics
        ColumnStatistics columnStatistics = new ColumnStatistics();
        ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
        columnStatisticsDesc.setIsTblLevel(false);
        columnStatisticsDesc.setTableName(tblName);
        columnStatisticsDesc.setPartName(partName);
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
        dateColumnStatsData.setLowValue(new org.apache.hadoop.hive.metastore.api.Date(18590));
        dateColumnStatsData.setHighValue(new org.apache.hadoop.hive.metastore.api.Date(18585));
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

        //update statistics
        client.updatePartitionStatistics(table, realPartVals, oldPartitionStats -> updatePartitionStatistics(oldPartitionStats, newPartitionStats));
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
        List<Partition> partitionsGet = client.getPartitionsByNames(tableGet.get(), ImmutableList.of(partName)).entrySet().stream().map(entry -> entry.getValue().get()).collect(Collectors.toList());
        Map<String, PartitionStatistics> getPartitionStatisticsMap = client.getPartitionStatistics(tableGet.get(), partitionsGet);

        PartitionStatistics getPartitionStatistics = getPartitionStatisticsMap.get(realPartVals);

        HiveBasicStatistics basicStatistics = getPartitionStatistics.getBasicStatistics();

        OptionalLong rowCountGet = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = getPartitionStatistics.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), tableGet.get().getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCountGet))
                .collect(toImmutableList());

        assertEquals(rowCount, rowCountGet, "rowcount:" + rowCount + ":" + rowCountGet);
        assertEquals(hiveBasicStatistics.getFileCount(), basicStatistics.getFileCount());
        assertEquals(hiveBasicStatistics.getInMemoryDataSizeInBytes(), basicStatistics.getInMemoryDataSizeInBytes());
        assertEquals(hiveBasicStatistics.getOnDiskDataSizeInBytes(), basicStatistics.getOnDiskDataSizeInBytes());

        assertTrue(metastoreColumnStatistics.size() >= 1);
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
        Optional<Table> tableGetForDelete = client.getTable(TEST_DB, tblName);
        client.updatePartitionStatistics(tableGetForDelete.get(), realPartVals, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatisticsForDelete));

        List<Partition> partitionsGetAfterDelete = client.getPartitionsByNames(tableGet.get(), ImmutableList.of(partName)).entrySet().stream().map(entry -> entry.getValue().get()).collect(Collectors.toList());
        Map<String, PartitionStatistics> getPartitionStatisticsMapAfterdelete = client.getPartitionStatistics(tableGetForDelete.get(), partitionsGetAfterDelete);

        PartitionStatistics getPartitionStatisticsAfterDelete = getPartitionStatisticsMapAfterdelete.get(realPartVals);

        HiveBasicStatistics basicStatisticsAfterDelete = getPartitionStatisticsAfterDelete.getBasicStatistics();
        OptionalLong rowCountGetAfterDelete = basicStatisticsAfterDelete.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatisticsAfterDelete = getPartitionStatisticsAfterDelete.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), tableGetForDelete.get().getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCountGetAfterDelete))
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

    private PartitionStatistics updatePartitionStatisticsForDelete(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats, String colName)
    {
        Map<String, HiveColumnStatistics> result = new HashMap<>(oldPartitionStats.getColumnStatistics());
        if (result.get(colName) != null) {
            result.remove(colName);
        }
        return new PartitionStatistics(newPartitionStats.getBasicStatistics(), result);
    }

    private Map<String, HiveColumnStatistics> updateColumnStatistics(Map<String, HiveColumnStatistics> oldColumnStats, Map<String, HiveColumnStatistics> newColumnStats)
    {
        Map<String, HiveColumnStatistics> result = new HashMap<>(oldColumnStats);
        result.putAll(newColumnStats);
        return ImmutableMap.copyOf(result);
    }

    @Test
    public void testColumnStatistics() throws Throwable
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;

        try {
            Map<String, String> columns = ImmutableMap.of("name", "string", "income", "double");
            Map<String, String> partitionColumns = ImmutableMap.of("ds", "string", "hr", "int");
            Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
            // create a table
            client.createTable(table, null);

            // Create a ColumnStatistics Obj
            String[] colName = new String[]{"income", "name"};
            double lowValue = 50000.21;
            double highValue = 1200000.4525;
            long numNulls = 3;
            long numDVs = 22;
            double avgColLen = 50.30;
            long maxColLen = 102;
            String[] colType = new String[]{"double", "string"};
            boolean isTblLevel = true;
            String partName = null;
            List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();

            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setIsTblLevel(isTblLevel);
            statsDesc.setPartName(partName);

            ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[0]);
            statsObj.setColType(colType[0]);

            ColumnStatisticsData statsData = new ColumnStatisticsData();
            DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
            statsData.setDoubleStats(numericStats);

            statsData.getDoubleStats().setHighValue(highValue);
            statsData.getDoubleStats().setLowValue(lowValue);
            statsData.getDoubleStats().setNumDVs(numDVs);
            statsData.getDoubleStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[1]);
            statsObj.setColType(colType[1]);

            statsData = new ColumnStatisticsData();
            StringColumnStatsData stringStats = new StringColumnStatsData();
            statsData.setStringStats(stringStats);
            statsData.getStringStats().setAvgColLen(avgColLen);
            statsData.getStringStats().setMaxColLen(maxColLen);
            statsData.getStringStats().setNumDVs(numDVs);
            statsData.getStringStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            ColumnStatistics colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            // write stats objs persistently
            OptionalLong rowCount = OptionalLong.of(200L);
            HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(10, rowCount.getAsLong(), 1000, 5000);
            Map<String, HiveColumnStatistics> hiveColumnStatistics = statsObjs.stream()
                    .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, rowCount)));
            PartitionStatistics newPartitionStats = new PartitionStatistics(hiveBasicStatistics, hiveColumnStatistics);

            client.updateTableStatistics(TEST_DB, tblName, NO_ACID_TRANSACTION, oldPartitionStats -> updatePartitionStatistics(oldPartitionStats, newPartitionStats));

            // retrieve the stats obj that was just written
            Optional<Table> tableGet = client.getTable(TEST_DB, tblName);
            ColumnStatisticsObj colStats2 = getTableColumnStatistics(tableGet.get(), colName[0], hiveBasicStatistics);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats2.getColName(), colName[0]);
            assertEquals(colStats2.getStatsData().getDoubleStats().getLowValue(), lowValue, 0.01);
            assertEquals(colStats2.getStatsData().getDoubleStats().getHighValue(), highValue, 0.01);
            assertEquals(colStats2.getStatsData().getDoubleStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getDoubleStats().getNumDVs(), numDVs);

            // test delete column stats; if no col name is passed all column stats associated with the
            // table is deleted
            PartitionStatistics partitionStatisticsForDelete = new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of());

            client.updateTableStatistics(TEST_DB, tblName, NO_ACID_TRANSACTION, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatisticsForDelete));
            // try to query stats for a column for which stats doesn't exist
            tableGet = client.getTable(TEST_DB, tblName);
            assertTrue(isEmptyTableColumnStatistics(tableGet.get(), colName[0]));

            // partition level column statistics test
            // create a table with multiple partitions
            client.dropTable(TEST_DB, tblName, true);

            //recreate table
            client.createTable(table, null);

            List<List<String>> values = new ArrayList<List<String>>();
            values.add(ImmutableList.of("2008-07-01 14:13:12", "14"));
            values.add(ImmutableList.of("2008-07-01 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-02 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-03 14:13:12", "151"));

            List<PartitionWithStatistics> partitionsWithStats = new ArrayList<>();
            List<String> partitionNames = new ArrayList<>();
            for (List<String> partValValues : values) {
                String partNameTemp = MetastoreUtil.makePartitionName(table.getPartitionColumns(), partValValues);
                partitionNames.add(partNameTemp);
                partitionsWithStats.add(createPartitionWithStatistics(table, partNameTemp, partValValues));
            }

            List<String> partitionColumnNames = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(toImmutableList());
            client.addPartitions(TEST_DB, tblName, partitionsWithStats);
            List<String> partitions = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames, TupleDomain.all()).get();

            partName = partitions.get(0);
            isTblLevel = false;

            // create a new columnstatistics desc to represent partition level column stats
            statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setPartName(partName);
            statsDesc.setIsTblLevel(isTblLevel);

            colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            //update statistics
            client.updatePartitionStatistics(table, partName, oldPartitionStats -> updatePartitionStatistics(oldPartitionStats, newPartitionStats));

            //get partitions statistics
            tableGet = client.getTable(TEST_DB, tblName);
            colStats2 = getPartitionColumnStatistics(tableGet.get(), partName, colName[1], hiveBasicStatistics);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats2.getColName(), colName[1]);
            assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
            assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
            assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

            // test stats deletion at partition level
            PartitionStatistics partitionStatisticsForDelete2 = new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of());
            Optional<Table> tableGetForDelete = client.getTable(TEST_DB, tblName);
            client.updatePartitionStatistics(tableGetForDelete.get(), partName, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatsOld, partitionStatisticsForDelete2, colName[1]));

            colStats2 = getPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[0], hiveBasicStatistics);
            assertTrue(colStats2 != null && colStats2.getColName().equals(colName[0]));

            // test get stats on a column for which stats doesn't exist
            assertTrue(isEmptyPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[1]));
        }
        catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testColumnStatistics() failed.");
            throw e;
        }
    }

    private PartitionWithStatistics createPartitionWithStatistics(Table table, String partName, List<String> partValNames)
    {
        HiveBasicStatistics basicStatisticsHive = new HiveBasicStatistics(1, 100, 1000, 1000);
        Map<String, String> parameters = new HashMap<>();
        final String numFiles = "numFiles";
        final String numRows = "numRows";
        final String rawDataSize = "rawDataSize";
        final String totalSize = "totalSize";
        parameters.put(numFiles, "1");
        parameters.put(numRows, "100");
        parameters.put(rawDataSize, "1000");
        parameters.put(totalSize, "1000");

        Partition partition = new Partition(table.getDatabaseName(), table.getTableName(), partValNames, table.getStorage(), table.getDataColumns(), parameters);
        Map<String, HiveColumnStatistics> columnStatisticsHive = new HashMap<>();
        PartitionStatistics statistics = new PartitionStatistics(basicStatisticsHive, columnStatisticsHive);
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, partName, statistics);
        return partitionWithStatistics;
    }

    private PartitionWithStatistics createPartitionWithStatistics(Table table, String partName, List<String> partValNames, List<ColumnStatisticsObj> statsObjs)
    {
        HiveBasicStatistics basicStatisticsHive = new HiveBasicStatistics(1, 100, 1000, 1000);
        Map<String, String> parameters = new HashMap<>();

        Partition partition = new Partition(table.getDatabaseName(), table.getTableName(), partValNames, table.getStorage(), table.getDataColumns(), parameters);

        Map<String, HiveColumnStatistics> columnStatisticsHive = statsObjs.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, basicStatisticsHive.getRowCount())));
        PartitionStatistics statistics = new PartitionStatistics(basicStatisticsHive, columnStatisticsHive);
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, partName, statistics);
        return partitionWithStatistics;
    }

    private PartitionWithStatistics createPartitionWithStatistics(Table table, String partName, List<String> partValNames, List<ColumnStatisticsObj> statsObjs, Map<String, String> parameters)
    {
        HiveBasicStatistics basicStatisticsHive = new HiveBasicStatistics(1, 100, 1000, 1000);
        Partition partition = new Partition(table.getDatabaseName(), table.getTableName(), partValNames, table.getStorage(), table.getDataColumns(), parameters);

        Map<String, HiveColumnStatistics> columnStatisticsHive = statsObjs.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, basicStatisticsHive.getRowCount())));
        PartitionStatistics statistics = new PartitionStatistics(basicStatisticsHive, columnStatisticsHive);
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, partName, statistics);
        return partitionWithStatistics;
    }

    private ColumnStatisticsObj getPartitionColumnStatistics(Table tableGet, String partName, String colName, HiveBasicStatistics hiveBasicStatistics)
    {
        List<Partition> partitionsGet = client.getPartitionsByNames(tableGet, ImmutableList.of(partName)).entrySet().stream().map(entry -> entry.getValue().get()).collect(Collectors.toList());
        Map<String, PartitionStatistics> getPartitionStatisticsMap = client.getPartitionStatistics(tableGet, partitionsGet);

        PartitionStatistics getPartitionStatisticsPartition = getPartitionStatisticsMap.get(partName);

        HiveBasicStatistics basicStatisticsPartition = getPartitionStatisticsPartition.getBasicStatistics();
        assertEquals(hiveBasicStatistics.getFileCount(), basicStatisticsPartition.getFileCount());
        assertEquals(hiveBasicStatistics.getRowCount(), basicStatisticsPartition.getRowCount());
        assertEquals(hiveBasicStatistics.getOnDiskDataSizeInBytes(), basicStatisticsPartition.getOnDiskDataSizeInBytes());
        assertEquals(hiveBasicStatistics.getInMemoryDataSizeInBytes(), basicStatisticsPartition.getInMemoryDataSizeInBytes());

        OptionalLong rowCountGetPartition = basicStatisticsPartition.getRowCount();
        ColumnStatisticsObj colStats2 = createMetastoreColumnStatistics(colName, tableGet.getColumn(colName).get().getType(), getPartitionStatisticsPartition.getColumnStatistics().get(colName), rowCountGetPartition);
        return colStats2;
    }

    private ColumnStatisticsObj getTableColumnStatistics(Table tableGet, String colName, HiveBasicStatistics hiveBasicStatistics)
    {
        PartitionStatistics getPartitionStatistics = client.getTableStatistics(tableGet);

        HiveBasicStatistics basicStatisticsPartition = getPartitionStatistics.getBasicStatistics();
        assertEquals(hiveBasicStatistics.getFileCount(), basicStatisticsPartition.getFileCount());
        assertEquals(hiveBasicStatistics.getRowCount(), basicStatisticsPartition.getRowCount());
        assertEquals(hiveBasicStatistics.getOnDiskDataSizeInBytes(), basicStatisticsPartition.getOnDiskDataSizeInBytes());
        assertEquals(hiveBasicStatistics.getInMemoryDataSizeInBytes(), basicStatisticsPartition.getInMemoryDataSizeInBytes());

        OptionalLong rowCountGetPartition = basicStatisticsPartition.getRowCount();
        ColumnStatisticsObj colStats2 = createMetastoreColumnStatistics(colName, tableGet.getColumn(colName).get().getType(), getPartitionStatistics.getColumnStatistics().get(colName), rowCountGetPartition);
        return colStats2;
    }

    private boolean isEmptyTableColumnStatistics(Table tableGet, String colName)
    {
        PartitionStatistics getPartitionStatistics = client.getTableStatistics(tableGet);
        if (getPartitionStatistics.getColumnStatistics().get(colName) == null) {
            return true;
        }
        else {
            return false;
        }
    }

    private boolean isEmptyPartitionColumnStatistics(Table tableGet, String partName, String colName)
    {
        List<Partition> partitionsGet = client.getPartitionsByNames(tableGet, ImmutableList.of(partName)).entrySet().stream().map(entry -> entry.getValue().get()).collect(Collectors.toList());
        Map<String, PartitionStatistics> getPartitionStatisticsMap = client.getPartitionStatistics(tableGet, partitionsGet);
        PartitionStatistics getPartitionStatisticsPartition = getPartitionStatisticsMap.get(partName);

        if (getPartitionStatisticsPartition.getColumnStatistics().get(colName) == null) {
            return true;
        }
        else {
            return false;
        }
    }

    @Test
    public void testListPartitionNames()
            throws TException
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of("ds", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
        client.createTable(table, null);
        List<PartitionWithStatistics> partitions = new ArrayList<>();

        List<String> names = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            partitions.add(createPartitionWithStatistics(table, "ds=20121210" + i, Lists.newArrayList("20121210" + i)));
            names.add(table.getPartitionColumns().get(0).getName() + "=20121210" + i);
        }
        client.addPartitions(TEST_DB, tblName, partitions);

        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        List<String> ret = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames, TupleDomain.all()).get();
        Collections.sort(names);
        Collections.sort(ret);
        assertEquals(names, ret);
    }

    @Test
    public void testListPartitionNamesMultiLevel()
            throws TException
    {
        // create a partition table which has multi partition keys.
        String tblName = "multiLevelPartTable";
        Map<String, String> cols = ImmutableMap.of("id", "int");
        Map<String, String> partCols = ImmutableMap.of("yr", "string", "mo", "string", "dt", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, cols, partCols);
        client.createTable(table, null);

        // create partitions
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=07/dt=01", Lists.newArrayList("2020", "07", "01")));
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=07/dt=02", Lists.newArrayList("2020", "07", "02")));
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=08/dt=01", Lists.newArrayList("2020", "08", "01")));
        client.addPartitions(TEST_DB, tblName, partitions);

        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        // get partition names with partial partition spec
        List<String> ret = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "yr", Domain.singleValue(VARCHAR, utf8Slice("2020")),
                        "mo", Domain.singleValue(VARCHAR, utf8Slice("")),
                        "dt", Domain.singleValue(VARCHAR, utf8Slice(""))))).get();
        List<String> expected = Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02", "yr=2020/mo=08/dt=01");
        Collections.sort(ret);
        Collections.sort(expected);
        assertEquals(expected, ret);

        ret = client.getPartitionNamesByFilter(TEST_DB, tblName, partitionColumnNames,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "yr", Domain.singleValue(VARCHAR, utf8Slice("2020")),
                        "mo", Domain.singleValue(VARCHAR, utf8Slice("07")),
                        "dt", Domain.singleValue(VARCHAR, utf8Slice(""))))).get();
        expected = Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02");
        Collections.sort(ret);
        Collections.sort(expected);
        assertEquals(expected, ret);
    }

    @Test
    public void testListPartitionNamesMultiLevelByNames()
            throws TException
    {
        // create a partition table which has multi partition keys.
        String tblName = "multiLevelPartTable";
        Map<String, String> cols = ImmutableMap.of("id", "int");
        Map<String, String> partCols = ImmutableMap.of("yr", "string", "mo", "string", "dt", "string");
        Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, cols, partCols);
        client.createTable(table, null);

        // create partitions
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=07/dt=01", Lists.newArrayList("2020", "07", "01")));
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=07/dt=02", Lists.newArrayList("2020", "07", "02")));
        partitions.add(createPartitionWithStatistics(table, "yr=2020/mo=08/dt=01", Lists.newArrayList("2020", "08", "01")));
        client.addPartitions(TEST_DB, tblName, partitions);

        // get partition names with partial partition spec
        Map<String, Optional<Partition>> ret = client.getPartitionsByNames(table, Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02", "yr=2020/mo=08/dt=01"));
        List<String> expected = Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02", "yr=2020/mo=08/dt=01");
        List<String> retPartitionNames = new ArrayList<>(ret.keySet());
        Collections.sort(retPartitionNames);
        Collections.sort(expected);
        assertEquals(expected, retPartitionNames);

        List<String> partitionColumns1 = ret.get("yr=2020/mo=07/dt=01").get().getColumns().stream().map(col -> col.getName() + ":" + col.getType().getTypeInfo().getTypeName()).collect(Collectors.toList());
        List<String> expetedPartitionColumns1 = cols.entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.toList());
        Collections.sort(partitionColumns1);
        Collections.sort(expetedPartitionColumns1);
        assertEquals(expetedPartitionColumns1, partitionColumns1);
    }

    @Test
    public void testAddPartitionWithColumnStatistics()
            throws Throwable
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;

        try {
            Map<String, String> columns = ImmutableMap.of("name", "string", "income", "double");
            Map<String, String> partitionColumns = ImmutableMap.of("ds", "string", "hr", "int");
            Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
            // create a table
            client.createTable(table, null);

            // Create a ColumnStatistics Obj
            String[] colName = new String[]{"income", "name"};
            double lowValue = 50000.21;
            double highValue = 1200000.4525;
            long numNulls = 3;
            long numDVs = 22;
            double avgColLen = 50.30;
            long maxColLen = 102;
            String[] colType = new String[]{"double", "string"};
            boolean isTblLevel = true;
            String partName = null;
            List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();

            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setIsTblLevel(isTblLevel);
            statsDesc.setPartName(partName);

            ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[0]);
            statsObj.setColType(colType[0]);

            ColumnStatisticsData statsData = new ColumnStatisticsData();
            DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
            statsData.setDoubleStats(numericStats);

            statsData.getDoubleStats().setHighValue(highValue);
            statsData.getDoubleStats().setLowValue(lowValue);
            statsData.getDoubleStats().setNumDVs(numDVs);
            statsData.getDoubleStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[1]);
            statsObj.setColType(colType[1]);

            statsData = new ColumnStatisticsData();
            StringColumnStatsData stringStats = new StringColumnStatsData();
            statsData.setStringStats(stringStats);
            statsData.getStringStats().setAvgColLen(avgColLen);
            statsData.getStringStats().setMaxColLen(maxColLen);
            statsData.getStringStats().setNumDVs(numDVs);
            statsData.getStringStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            ColumnStatistics colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            //recreate table

            List<List<String>> values = new ArrayList<List<String>>();
            values.add(ImmutableList.of("2008-07-01 14:13:12", "14"));
            values.add(ImmutableList.of("2008-07-01 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-02 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-03 14:13:12", "151"));

            List<PartitionWithStatistics> partitionsWithStats = new ArrayList<>();
            isTblLevel = false;

            // create a new columnstatistics desc to represent partition level column stats
            statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setPartName(partName);
            statsDesc.setIsTblLevel(isTblLevel);

            colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            //add partition statistics
            List<String> partitionNames = new ArrayList<>();
            for (List<String> partValValues : values) {
                String partNameTemp = MetastoreUtil.makePartitionName(table.getPartitionColumns(), partValValues);
                partitionNames.add(partNameTemp);
                partitionsWithStats.add(createPartitionWithStatistics(table, partNameTemp, partValValues, statsObjs));
            }
            partName = partitionNames.get(0);
            client.addPartitions(TEST_DB, tblName, partitionsWithStats);

            //get partitions statistics
            HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(1, 100, 1000, 1000);
            Optional<Table> tableGet = client.getTable(TEST_DB, tblName);
            ColumnStatisticsObj colStats2 = getPartitionColumnStatistics(tableGet.get(), partName, colName[1], hiveBasicStatistics);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats2.getColName(), colName[1]);
            assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
            assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
            assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

            // test stats deletion at partition level
            PartitionStatistics partitionStatisticsForDelete2 = new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of());
            Optional<Table> tableGetForDelete = client.getTable(TEST_DB, tblName);
            client.updatePartitionStatistics(tableGetForDelete.get(), partName, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatsOld, partitionStatisticsForDelete2, colName[1]));

            colStats2 = getPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[0], hiveBasicStatistics);
            assertTrue(colStats2 != null && colStats2.getColName().equals(colName[0]));

            // test get stats on a column for which stats doesn't exist
            assertTrue(isEmptyPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[1]));
        }
        catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testColumnStatistics() failed.");
            throw e;
        }
    }

    @Test
    public void testAlterPartitionWithColumnStatistics()
            throws Throwable
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;

        try {
            Map<String, String> columns = ImmutableMap.of("name", "string", "income", "double");
            Map<String, String> partitionColumns = ImmutableMap.of("ds", "string", "hr", "int");
            Table table = TestingAlibabaDlfUtils.getTable(TEST_DB, tblName, columns, partitionColumns);
            // create a table
            client.createTable(table, null);

            // Create a ColumnStatistics Obj
            String[] colName = new String[]{"income", "name"};
            double lowValue = 50000.21;
            double highValue = 1200000.4525;
            long numNulls = 3;
            long numDVs = 22;
            double avgColLen = 50.30;
            long maxColLen = 102;
            String[] colType = new String[]{"double", "string"};
            boolean isTblLevel = true;
            String partName = null;
            List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();

            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setIsTblLevel(isTblLevel);
            statsDesc.setPartName(partName);

            ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[0]);
            statsObj.setColType(colType[0]);

            ColumnStatisticsData statsData = new ColumnStatisticsData();
            DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
            statsData.setDoubleStats(numericStats);

            statsData.getDoubleStats().setHighValue(highValue);
            statsData.getDoubleStats().setLowValue(lowValue);
            statsData.getDoubleStats().setNumDVs(numDVs);
            statsData.getDoubleStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[1]);
            statsObj.setColType(colType[1]);

            statsData = new ColumnStatisticsData();
            StringColumnStatsData stringStats = new StringColumnStatsData();
            statsData.setStringStats(stringStats);
            statsData.getStringStats().setAvgColLen(avgColLen);
            statsData.getStringStats().setMaxColLen(maxColLen);
            statsData.getStringStats().setNumDVs(numDVs);
            statsData.getStringStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            ColumnStatistics colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            //recreate table

            List<List<String>> values = new ArrayList<List<String>>();
            values.add(ImmutableList.of("2008-07-01 14:13:12", "14"));
            values.add(ImmutableList.of("2008-07-01 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-02 14:13:12", "15"));
            values.add(ImmutableList.of("2008-07-03 14:13:12", "151"));

            List<PartitionWithStatistics> partitionsWithStats = new ArrayList<>();
            isTblLevel = false;

            // create a new columnstatistics desc to represent partition level column stats
            statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(TEST_DB);
            statsDesc.setTableName(tblName);
            statsDesc.setPartName(partName);
            statsDesc.setIsTblLevel(isTblLevel);

            colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            //add partition statistics
            List<String> partitionNames = new ArrayList<>();
            for (List<String> partValValues : values) {
                String partNameTemp = MetastoreUtil.makePartitionName(table.getPartitionColumns(), partValValues);
                partitionNames.add(partNameTemp);
                partitionsWithStats.add(createPartitionWithStatistics(table, partNameTemp, partValValues, statsObjs));
            }
            partName = partitionNames.get(0);
            client.addPartitions(TEST_DB, tblName, partitionsWithStats);

            //get partitions statistics
            HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(1, 100, 1000, 1000);
            Optional<Table> tableGet = client.getTable(TEST_DB, tblName);
            ColumnStatisticsObj colStats2 = getPartitionColumnStatistics(tableGet.get(), partName, colName[1], hiveBasicStatistics);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats2.getColName(), colName[1]);
            assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
            assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
            assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

            //alter partition
            Map<String, String> parameters = new HashMap<>();
            parameters.put("alterpartition", "test");
            client.alterPartition(TEST_DB, tblName, createPartitionWithStatistics(table, partitionNames.get(2), values.get(2), statsObjs.subList(0, statsObjs.size() - 1), parameters));
            Optional<Table> tableGetForAlter = client.getTable(TEST_DB, tblName);
            Optional<Partition> partitionAlter = client.getPartition(tableGetForAlter.get(), values.get(2));
            assertTrue(partitionAlter.isPresent());
            assertTrue(partitionAlter.get().getParameters().get("alterpartition").equals("test"));

            colStats2 = getPartitionColumnStatistics(tableGetForAlter.get(), partitionNames.get(2), colName[0], hiveBasicStatistics);
            assertTrue(colStats2 != null && colStats2.getColName().equals(colName[0]));
            assertTrue(isEmptyTableColumnStatistics(tableGetForAlter.get(), colName[1]));

            // test stats deletion at partition level
            PartitionStatistics partitionStatisticsForDelete2 = new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of());
            Optional<Table> tableGetForDelete = client.getTable(TEST_DB, tblName);
            client.updatePartitionStatistics(tableGetForDelete.get(), partName, partitionStatsOld -> updatePartitionStatisticsForDelete(partitionStatsOld, partitionStatisticsForDelete2, colName[1]));

            colStats2 = getPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[0], hiveBasicStatistics);
            assertTrue(colStats2 != null && colStats2.getColName().equals(colName[0]));

            // test get stats on a column for which stats doesn't exist
            assertTrue(isEmptyPartitionColumnStatistics(tableGetForDelete.get(), partName, colName[1]));
        }
        catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testColumnStatistics() failed.");
            throw e;
        }
    }
}
