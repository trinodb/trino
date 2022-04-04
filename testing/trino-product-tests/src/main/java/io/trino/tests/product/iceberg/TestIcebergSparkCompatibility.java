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
package io.trino.tests.product.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.concurrent.MoreFutures;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.hive.Engine;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AND_INSERT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AS_SELECT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_WITH_NO_DATA_AND_INSERT;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertTrue;

/**
 * Tests compatibility between Iceberg connector and Spark Iceberg.
 */
public class TestIcebergSparkCompatibility
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "unsupportedStorageFormats")
    public void testTrinoWithUnsupportedFileFormat(StorageFormat storageFormat)
    {
        String tableName = "test_trino_unsupported_file_format_" + storageFormat;
        String trinoTableName = trinoTableName(tableName);
        String sparkTableName = sparkTableName(tableName);

        onSpark().executeQuery(format("CREATE TABLE %s (x bigint) USING ICEBERG TBLPROPERTIES ('write.format.default'='%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s VALUES (42)", sparkTableName));

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q File format not supported for Iceberg: " + storageFormat);
        assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s VALUES (42)", trinoTableName)))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q File format not supported for Iceberg: " + storageFormat);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_trino_reading_primitive_types_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  _string STRING" +
                        ", _bigint BIGINT" +
                        ", _integer INTEGER" +
                        ", _real REAL" +
                        ", _double DOUBLE" +
                        ", _short_decimal decimal(8,2)" +
                        ", _long_decimal decimal(38,19)" +
                        ", _boolean BOOLEAN" +
                        ", _timestamp TIMESTAMP" +
                        ", _date DATE" +
                        ", _binary BINARY" +
                        ") USING ICEBERG " +
                        "TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));

        // Validate queries on an empty table created by Spark
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName("\"" + baseTableName + "$snapshots\"")))).hasNoRows();
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName))).hasNoRows();

        onSpark().executeQuery(format(
                "INSERT INTO %s VALUES (" +
                        "'a_string'" +
                        ", 1000000000000000" +
                        ", 1000000000" +
                        ", 10000000.123" +
                        ", 100000000000.123" +
                        ", CAST('123456.78' AS decimal(8,2))" +
                        ", CAST('1234567890123456789.0123456789012345678' AS decimal(38,19))" +
                        ", true" +
                        ", TIMESTAMP '2020-06-28 14:16:00.456'" +
                        ", DATE '1950-06-28'" +
                        ", X'000102f0feff'" +
                        ")",
                sparkTableName));

        Row row = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                new BigDecimal("123456.78"),
                new BigDecimal("1234567890123456789.0123456789012345678"),
                true,
                Timestamp.valueOf("2020-06-28 14:16:00.456"),
                Date.valueOf("1950-06-28"),
                new byte[] {00, 01, 02, -16, -2, -1});

        assertThat(onSpark().executeQuery(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        ", _timestamp" +
                        ", _date" +
                        ", _binary" +
                        " FROM " + sparkTableName))
                .containsOnly(row);

        assertThat(onTrino().executeQuery(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        ", CAST(_timestamp AS TIMESTAMP)" + // TODO test the value without a CAST from timestamp with time zone to timestamp
                        ", _date" +
                        ", _binary" +
                        " FROM " + trinoTableName))
                .containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testSparkReadingTrinoDataDataProvider")
    public void testSparkReadingTrinoData(StorageFormat storageFormat, CreateMode createMode)
    {
        String baseTableName = "test_spark_reading_primitive_types_" + storageFormat + "_" + createMode;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        String namedValues = "SELECT " +
                "  VARCHAR 'a_string' _string " +
                ", 1000000000000000 _bigint " +
                ", 1000000000 _integer " +
                ", REAL '10000000.123' _real " +
                ", DOUBLE '100000000000.123' _double " +
                ", DECIMAL '123456.78' _short_decimal " +
                ", DECIMAL '1234567890123456789.0123456789012345678' _long_decimal " +
                ", true _boolean " +
                //", TIMESTAMP '2020-06-28 14:16:00.456' _timestamp " +
                ", TIMESTAMP '2021-08-03 08:32:21.123456 Europe/Warsaw' _timestamptz " +
                ", DATE '1950-06-28' _date " +
                ", X'000102f0feff' _binary " +
                //", TIME '01:23:45.123456' _time " +
                "";

        switch (createMode) {
            case CREATE_TABLE_AND_INSERT:
                onTrino().executeQuery(format(
                        "CREATE TABLE %s (" +
                                "  _string VARCHAR" +
                                ", _bigint BIGINT" +
                                ", _integer INTEGER" +
                                ", _real REAL" +
                                ", _double DOUBLE" +
                                ", _short_decimal decimal(8,2)" +
                                ", _long_decimal decimal(38,19)" +
                                ", _boolean BOOLEAN" +
                                //", _timestamp TIMESTAMP" -- per https://iceberg.apache.org/spark-writes/ Iceberg's timestamp is currently not supported with Spark
                                ", _timestamptz timestamp(6) with time zone" +
                                ", _date DATE" +
                                ", _binary VARBINARY" +
                                //", _time time(6)" + -- per https://iceberg.apache.org/spark-writes/ Iceberg's time is currently not supported with Spark
                                ") WITH (format = '%s')",
                        trinoTableName,
                        storageFormat));

                onTrino().executeQuery(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            case CREATE_TABLE_AS_SELECT:
                onTrino().executeQuery(format("CREATE TABLE %s AS %s", trinoTableName, namedValues));
                break;

            case CREATE_TABLE_WITH_NO_DATA_AND_INSERT:
                onTrino().executeQuery(format("CREATE TABLE %s AS %s WITH NO DATA", trinoTableName, namedValues));
                onTrino().executeQuery(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            default:
                throw new UnsupportedOperationException("Unsupported create mode: " + createMode);
        }

        Row row = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                new BigDecimal("123456.78"),
                new BigDecimal("1234567890123456789.0123456789012345678"),
                true,
                //"2020-06-28 14:16:00.456",
                "2021-08-03 06:32:21.123456 UTC", // Iceberg's timestamptz stores point in time, without zone
                "1950-06-28",
                new byte[] {00, 01, 02, -16, -2, -1}
                // "01:23:45.123456"
                /**/);
        assertThat(onTrino().executeQuery(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        // _timestamp OR CAST(_timestamp AS varchar)
                        ", CAST(_timestamptz AS varchar)" +
                        ", CAST(_date AS varchar)" +
                        ", _binary" +
                        //", CAST(_time AS varchar)" +
                        " FROM " + trinoTableName))
                .containsOnly(row);

        assertThat(onSpark().executeQuery(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        // _timestamp OR CAST(_timestamp AS string)
                        ", CAST(_timestamptz AS string) || ' UTC'" + // Iceberg timestamptz is mapped to Spark timestamp and gets represented without time zone
                        ", CAST(_date AS string)" +
                        ", _binary" +
                        // ", CAST(_time AS string)" +
                        " FROM " + sparkTableName))
                .containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @DataProvider
    public Object[][] testSparkReadingTrinoDataDataProvider()
    {
        return Stream.of(storageFormats())
                .map(array -> getOnlyElement(asList(array)))
                .flatMap(storageFormat -> Stream.of(
                        new Object[] {storageFormat, CREATE_TABLE_AND_INSERT},
                        new Object[] {storageFormat, CREATE_TABLE_AS_SELECT},
                        new Object[] {storageFormat, CREATE_TABLE_WITH_NO_DATA_AND_INSERT}))
                .toArray(Object[][]::new);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkReadTrinoUuid(StorageFormat storageFormat)
    {
        String tableName = "test_spark_read_trino_uuid_" + storageFormat;
        String trinoTableName = trinoTableName(tableName);
        String sparkTableName = sparkTableName(tableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s AS SELECT UUID '406caec7-68b9-4778-81b2-a12ece70c8b1' u",
                trinoTableName));

        assertQueryFailure(() -> onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                // TODO Iceberg Spark integration needs yet to gain support
                //  once this is supported, merge this test with testSparkReadingTrinoData()
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("org.apache.hive.service.cli.HiveSQLException: Error running query:.*\\Q java.lang.ClassCastException: class [B cannot be cast to class org.apache.spark.unsafe.types.UTF8String\\E(?s:.*)");

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "specVersions")
    public void testSparkCreatesTrinoDrops(int specVersion)
    {
        String baseTableName = "test_spark_creates_trino_drops";
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(baseTableName), specVersion));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoCreatesSparkDrops()
    {
        String baseTableName = "test_trino_creates_spark_drops";
        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT)", trinoTableName(baseTableName)));
        onSpark().executeQuery("DROP TABLE " + sparkTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkReadsTrinoPartitionedTable(StorageFormat storageFormat)
    {
        String baseTableName = "test_spark_reads_trino_partitioned_table_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _varbinary VARBINARY, _bigint BIGINT) WITH (partitioning = ARRAY['_string', '_varbinary'], format = '%s')", trinoTableName, storageFormat));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", trinoTableName));

        Row row1 = row("b", new byte[]{15, -15, 2, -16, -2, -2}, 1002);
        String selectByString = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row1);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row1);

        Row row2 = row("a", new byte[]{15, -15, 2, -16, -2, -1}, 1001);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102f0feff'";
        assertThat(onTrino().executeQuery(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(onSpark().executeQuery(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadsSparkPartitionedTable(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_trino_reads_spark_partitioned_table_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (_string STRING, _varbinary BINARY, _bigint BIGINT) USING ICEBERG PARTITIONED BY (_string, _varbinary) TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));
        onSpark().executeQuery(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", sparkTableName));

        Row row1 = row("a", new byte[]{15, -15, 2, -16, -2, -1}, 1001);
        String select = "SELECT * FROM %s WHERE _string = 'a'";
        assertThat(onSpark().executeQuery(format(select, sparkTableName)))
                .containsOnly(row1);
        assertThat(onTrino().executeQuery(format(select, trinoTableName)))
                .containsOnly(row1);

        Row row2 = row("c", new byte[]{15, -15, 2, -3, -2, -1}, 1003);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102fdfeff'";
        assertThat(onTrino().executeQuery(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(onSpark().executeQuery(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingCompositeSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_trino_reading_spark_composites_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("" +
                        "CREATE TABLE %s (" +
                        "  doc_id string,\n" +
                        "  info MAP<STRING, INT>,\n" +
                        "  pets ARRAY<STRING>,\n" +
                        "  user_info STRUCT<name:STRING, surname:STRING, age:INT, gender:STRING>)" +
                        "  USING ICEBERG" +
                        " TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName, storageFormat, specVersion));

        onSpark().executeQuery(format(
                "INSERT INTO TABLE %s SELECT 'Doc213', map('age', 28, 'children', 3), array('Dog', 'Cat', 'Pig'), \n" +
                        "named_struct('name', 'Santa', 'surname', 'Claus','age', 1000,'gender', 'MALE')",
                sparkTableName));

        assertThat(onTrino().executeQuery("SELECT doc_id, info['age'], pets[2], user_info.surname FROM " + trinoTableName))
                .containsOnly(row("Doc213", 28, "Cat", "Claus"));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkReadingCompositeTrinoData(StorageFormat storageFormat)
    {
        String baseTableName = "test_spark_reading_trino_composites_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  doc_id VARCHAR,\n" +
                        "  info MAP(VARCHAR, INTEGER),\n" +
                        "  pets ARRAY(VARCHAR),\n" +
                        "  user_info ROW(name VARCHAR, surname VARCHAR, age INTEGER, gender VARCHAR)) " +
                        "  WITH (format = '%s')",
                trinoTableName,
                storageFormat));

        onTrino().executeQuery(format(
                "INSERT INTO %s VALUES('Doc213', MAP(ARRAY['age', 'children'], ARRAY[28, 3]), ARRAY['Dog', 'Cat', 'Pig'], ROW('Santa', 'Claus', 1000, 'MALE'))",
                trinoTableName));

        assertThat(onSpark().executeQuery("SELECT doc_id, info['age'], pets[1], user_info.surname FROM " + sparkTableName))
                .containsOnly(row("Doc213", 28, "Cat", "Claus"));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingSparkIcebergTablePropertiesData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_trino_reading_spark_iceberg_table_properties_" + storageFormat;
        String propertiesTableName = "\"" + baseTableName + "$properties\"";
        String sparkTableName = sparkTableName(baseTableName);
        String trinoPropertiesTableName = trinoTableName(propertiesTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        onSpark().executeQuery(format(
                "CREATE TABLE %s (\n" +
                        " doc_id STRING)\n" +
                        " USING ICEBERG TBLPROPERTIES (" +
                        " 'write.format.default'='%s'," +
                        " 'format-version' = %s," +
                        " 'custom.table-property' = 'my_custom_value')",
                sparkTableName,
                storageFormat.toString(),
                specVersion));

        assertThat(onTrino().executeQuery("SELECT key, value FROM " + trinoPropertiesTableName))
                .containsOnly(
                        row("custom.table-property", "my_custom_value"),
                        row("write.format.default", storageFormat.name()),
                        row("owner", "hive"));
        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingNestedSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_trino_reading_nested_spark_data_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (\n" +
                        "  doc_id STRING\n" +
                        ", nested_map MAP<STRING, ARRAY<STRUCT<sname: STRING, snumber: INT>>>\n" +
                        ", nested_array ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>\n" +
                        ", nested_struct STRUCT<name:STRING, complicated: ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>>)\n" +
                        " USING ICEBERG TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));

        onSpark().executeQuery(format(
                "INSERT INTO TABLE %s SELECT" +
                        "  'Doc213'" +
                        ", map('s1', array(named_struct('sname', 'ASName1', 'snumber', 201), named_struct('sname', 'ASName2', 'snumber', 202)))" +
                        ", array(map('m1', array(named_struct('mname', 'MAS1Name1', 'mnumber', 301), named_struct('mname', 'MAS1Name2', 'mnumber', 302)))" +
                        "       ,map('m2', array(named_struct('mname', 'MAS2Name1', 'mnumber', 401), named_struct('mname', 'MAS2Name2', 'mnumber', 402))))" +
                        ", named_struct('name', 'S1'," +
                        "               'complicated', array(map('m1', array(named_struct('mname', 'SAMA1Name1', 'mnumber', 301), named_struct('mname', 'SAMA1Name2', 'mnumber', 302)))" +
                        "                                   ,map('m2', array(named_struct('mname', 'SAMA2Name1', 'mnumber', 401), named_struct('mname', 'SAMA2Name2', 'mnumber', 402)))))",
                sparkTableName));

        Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        assertThat(onSpark().executeQuery(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][1].sname" +
                        ", nested_map['s1'][0].snumber" +
                        ", nested_array[1]['m2'][0].mname" +
                        ", nested_array[0]['m1'][1].mnumber" +
                        ", nested_struct.complicated[0]['m1'][0].mname" +
                        ", nested_struct.complicated[1]['m2'][1].mnumber" +
                        "  FROM " + sparkTableName))
                .containsOnly(row);

        assertThat(onTrino().executeQuery("SELECT" +
                "  doc_id" +
                ", nested_map['s1'][2].sname" +
                ", nested_map['s1'][1].snumber" +
                ", nested_array[2]['m2'][1].mname" +
                ", nested_array[1]['m1'][2].mnumber" +
                ", nested_struct.complicated[1]['m1'][1].mname" +
                ", nested_struct.complicated[2]['m2'][2].mnumber" +
                "  FROM " + trinoTableName))
                .containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkReadingNestedTrinoData(StorageFormat storageFormat)
    {
        String baseTableName = "test_spark_reading_nested_trino_data_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s (\n" +
                        "  doc_id VARCHAR\n" +
                        ", nested_map MAP(VARCHAR, ARRAY(ROW(sname VARCHAR, snumber INT)))\n" +
                        ", nested_array ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))\n" +
                        ", nested_struct ROW(name VARCHAR, complicated ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))))" +
                        "  WITH (format = '%s')",
                trinoTableName,
                storageFormat));

        onTrino().executeQuery(format(
                "INSERT INTO %s SELECT" +
                        "  'Doc213'" +
                        ", map(array['s1'], array[array[row('ASName1', 201), row('ASName2', 202)]])" +
                        ", array[map(array['m1'], array[array[row('MAS1Name1', 301), row('MAS1Name2', 302)]])" +
                        "       ,map(array['m2'], array[array[row('MAS2Name1', 401), row('MAS2Name2', 402)]])]" +
                        ", row('S1'" +
                        "      ,array[map(array['m1'], array[array[row('SAMA1Name1', 301), row('SAMA1Name2', 302)]])" +
                        "            ,map(array['m2'], array[array[row('SAMA2Name1', 401), row('SAMA2Name2', 402)]])])",
                trinoTableName));

        Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        assertThat(onTrino().executeQuery(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][2].sname" +
                        ", nested_map['s1'][1].snumber" +
                        ", nested_array[2]['m2'][1].mname" +
                        ", nested_array[1]['m1'][2].mnumber" +
                        ", nested_struct.complicated[1]['m1'][1].mname" +
                        ", nested_struct.complicated[2]['m2'][2].mnumber" +
                        "  FROM " + trinoTableName))
                .containsOnly(row);

        QueryResult sparkResult = onSpark().executeQuery(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][1].sname" +
                        ", nested_map['s1'][0].snumber" +
                        ", nested_array[1]['m2'][0].mname" +
                        ", nested_array[0]['m1'][1].mnumber" +
                        ", nested_struct.complicated[0]['m1'][0].mname" +
                        ", nested_struct.complicated[1]['m2'][1].mnumber" +
                        "  FROM " + sparkTableName);
        assertThat(sparkResult).containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testIdBasedFieldMapping(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_schema_evolution_for_nested_fields_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "remove_col BIGINT, " +
                        "rename_col BIGINT, " +
                        "keep_col BIGINT, " +
                        "drop_and_add_col BIGINT, " +
                        "CaseSensitiveCol BIGINT, " +
                        "a_struct STRUCT<removed: BIGINT, rename:BIGINT, keep:BIGINT, drop_and_add:BIGINT, CaseSensitive:BIGINT>, " +
                        "a_partition BIGINT) "
                        + " USING ICEBERG"
                        + " PARTITIONED BY (a_partition)"
                        + " TBLPROPERTIES ('write.format.default' = '%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));

        onSpark().executeQuery(format(
                "INSERT INTO TABLE %s SELECT " +
                        "1, " + // remove_col
                        "2, " + // rename_col
                        "3, " + // keep_col
                        "4, " + // drop_and_add_col,
                        "5, " + // CaseSensitiveCol,
                        " named_struct('removed', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13, 'CaseSensitive', 14), " // a_struct
                        + "1001", // a_partition
                sparkTableName));

        onSpark().executeQuery(format("ALTER TABLE %s DROP COLUMN remove_col", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s RENAME COLUMN rename_col TO quite_renamed_col", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s DROP COLUMN drop_and_add_col", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD COLUMN drop_and_add_col BIGINT", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD COLUMN add_col BIGINT", sparkTableName));

        onSpark().executeQuery(format("ALTER TABLE %s DROP COLUMN a_struct.removed", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s RENAME COLUMN a_struct.rename TO renamed", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s DROP COLUMN a_struct.drop_and_add", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD COLUMN a_struct.drop_and_add BIGINT", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD COLUMN a_struct.added BIGINT", sparkTableName));

        assertThat(onTrino().executeQuery("DESCRIBE " + trinoTableName).project(1, 2))
                .containsOnly(
                        row("quite_renamed_col", "bigint"),
                        row("keep_col", "bigint"),
                        row("drop_and_add_col", "bigint"),
                        row("add_col", "bigint"),
                        row("casesensitivecol", "bigint"),
                        row("a_struct", "row(renamed bigint, keep bigint, CaseSensitive bigint, drop_and_add bigint, added bigint)"),
                        row("a_partition", "bigint"));

        assertThat(onTrino().executeQuery(format("SELECT quite_renamed_col, keep_col, drop_and_add_col, add_col, casesensitivecol, a_struct, a_partition FROM %s", trinoTableName)))
                .containsOnly(row(
                        2L, // quite_renamed_col
                        3L, // keep_col
                        null, // drop_and_add_col; dropping and re-adding changes id
                        null, // add_col
                        5L, // CaseSensitiveCol
                        rowBuilder()
                                // Rename does not change id
                                .addField("renamed", 11L)
                                .addField("keep", 12L)
                                .addField("CaseSensitive", 14L)
                                // Dropping and re-adding changes id
                                .addField("drop_and_add", null)
                                .addField("added", null)
                                .build(),
                        1001L));

        // smoke test for dereference
        assertThat(onTrino().executeQuery(format("SELECT a_struct.renamed FROM %s", trinoTableName))).containsOnly(row(11L));
        assertThat(onTrino().executeQuery(format("SELECT a_struct.keep FROM %s", trinoTableName))).containsOnly(row(12L));
        assertThat(onTrino().executeQuery(format("SELECT a_struct.casesensitive FROM %s", trinoTableName))).containsOnly(row(14L));
        assertThat(onTrino().executeQuery(format("SELECT a_struct.drop_and_add FROM %s", trinoTableName))).containsOnly(row((Object) null));
        assertThat(onTrino().executeQuery(format("SELECT a_struct.added FROM %s", trinoTableName))).containsOnly(row((Object) null));

        // smoke test for dereference in a predicate
        assertThat(onTrino().executeQuery(format("SELECT keep_col FROM %s WHERE a_struct.renamed = 11", trinoTableName))).containsOnly(row(3L));
        assertThat(onTrino().executeQuery(format("SELECT keep_col FROM %s WHERE a_struct.keep = 12", trinoTableName))).containsOnly(row(3L));
        assertThat(onTrino().executeQuery(format("SELECT keep_col FROM %s WHERE a_struct.casesensitive = 14", trinoTableName))).containsOnly(row(3L));
        assertThat(onTrino().executeQuery(format("SELECT keep_col FROM %s WHERE a_struct.drop_and_add IS NULL", trinoTableName))).containsOnly(row(3L));
        assertThat(onTrino().executeQuery(format("SELECT keep_col FROM %s WHERE a_struct.added IS NULL", trinoTableName))).containsOnly(row(3L));

        onSpark().executeQuery(format(
                "INSERT INTO TABLE %s SELECT " +
                        "12, " + // quite_renamed_col
                        "13, " + // keep_col
                        "15, " + // CaseSensitiveCol,
                        "named_struct('renamed', 111, 'keep', 112, 'CaseSensitive', 113, 'drop_and_add', 114, 'added', 115), " + // a_struct
                        "1001, " + // a_partition,
                        "14, " + // drop_and_add_col
                        "15", // add_col
                sparkTableName));

        assertThat(onTrino().executeQuery("SELECT DISTINCT a_struct.renamed, a_struct.added, a_struct.keep FROM " + trinoTableName)).containsOnly(
                row(11L, null, 12L),
                row(111L, 115L, 112L));
        assertThat(onTrino().executeQuery("SELECT DISTINCT a_struct.renamed, a_struct.keep FROM " + trinoTableName + " WHERE a_struct.added IS NULL")).containsOnly(
                row(11L, 12L));

        assertThat(onTrino().executeQuery("SELECT a_struct FROM " + trinoTableName + " WHERE a_struct.added IS NOT NULL")).containsOnly(
                row(rowBuilder()
                        .addField("renamed", 111L)
                        .addField("keep", 112L)
                        .addField("CaseSensitive", 113L)
                        .addField("drop_and_add", 114L)
                        .addField("added", 115L)
                        .build()));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testReadAfterPartitionEvolution(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_read_after_partition_evolution_" + storageFormat;
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "int_col BIGINT, " +
                        "struct_col STRUCT<field_one: INT, field_two: INT>, " +
                        "timestamp_col TIMESTAMP) "
                        + " USING ICEBERG"
                        + " TBLPROPERTIES ('write.format.default' = '%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, named_struct('field_one', 1, 'field_two', 1), TIMESTAMP '2021-06-28 14:16:00.456')");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, int_col)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (2, named_struct('field_one', 2, 'field_two', 2), TIMESTAMP '2022-06-28 14:16:00.456')");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD struct_col.field_one");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (3, named_struct('field_one', 3, 'field_two', 3), TIMESTAMP '2023-06-28 14:16:00.456')");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD struct_col.field_one");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD struct_col.field_two");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (4, named_struct('field_one', 4, 'field_two', 4), TIMESTAMP '2024-06-28 14:16:00.456')");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD bucket(3, int_col)");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD struct_col.field_two");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD days(timestamp_col)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (5, named_struct('field_one', 5, 'field_two', 5), TIMESTAMP '2025-06-28 14:16:00.456')");

        // The Iceberg documentation states it is not necessary to drop a day transform partition field in order to add an hourly one
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD hours(timestamp_col)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (6, named_struct('field_one', 6, 'field_two', 6), TIMESTAMP '2026-06-28 14:16:00.456')");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD int_col");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (7, named_struct('field_one', 7, 'field_two', 7), TIMESTAMP '2027-06-28 14:16:00.456')");

        Function<Integer, io.trino.jdbc.Row> buildStructColValue = intValue -> rowBuilder()
                .addField("field_one", intValue)
                .addField("field_two", intValue)
                .build();

        assertThat(onTrino().executeQuery("SELECT int_col, struct_col, CAST(timestamp_col AS TIMESTAMP) FROM " + trinoTableName))
                .containsOnly(
                        row(1, buildStructColValue.apply(1), Timestamp.valueOf("2021-06-28 14:16:00.456")),
                        row(2, buildStructColValue.apply(2), Timestamp.valueOf("2022-06-28 14:16:00.456")),
                        row(3, buildStructColValue.apply(3), Timestamp.valueOf("2023-06-28 14:16:00.456")),
                        row(4, buildStructColValue.apply(4), Timestamp.valueOf("2024-06-28 14:16:00.456")),
                        row(5, buildStructColValue.apply(5), Timestamp.valueOf("2025-06-28 14:16:00.456")),
                        row(6, buildStructColValue.apply(6), Timestamp.valueOf("2026-06-28 14:16:00.456")),
                        row(7, buildStructColValue.apply(7), Timestamp.valueOf("2027-06-28 14:16:00.456")));

        assertThat(onTrino().executeQuery("SELECT struct_col.field_two FROM " + trinoTableName))
                .containsOnly(row(1), row(2), row(3), row(4), row(5), row(6), row(7));
        assertThat(onTrino().executeQuery("SELECT CAST(timestamp_col AS TIMESTAMP) FROM " + trinoTableName + " WHERE struct_col.field_two = 2"))
                .containsOnly(row(Timestamp.valueOf("2022-06-28 14:16:00.456")));

        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE int_col = 2"))
                .containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE int_col % 2 = 0"))
                .containsOnly(row(3));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE struct_col.field_one = 2"))
                .containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE struct_col.field_one % 2 = 0"))
                .containsOnly(row(3));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE year(timestamp_col) = 2022"))
                .containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE year(timestamp_col) % 2 = 0"))
                .containsOnly(row(3));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "specVersions")
    public void testTrinoShowingSparkCreatedTables(int specVersion)
    {
        String sparkTable = "test_table_listing_for_spark";
        String trinoTable = "test_table_listing_for_trino";

        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(sparkTable), specVersion));
        onTrino().executeQuery(format("CREATE TABLE %s (_integer INTEGER )", trinoTableName(trinoTable)));

        assertThat(onTrino().executeQuery(format("SHOW TABLES FROM %s LIKE '%s'", TEST_SCHEMA_NAME, "test_table_listing_for_%")))
                .containsOnly(row(sparkTable), row(trinoTable));

        onSpark().executeQuery("DROP TABLE " + sparkTableName(sparkTable));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(trinoTable));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoWritingDataWithObjectStorageLocationProvider(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_object_storage_location_provider_" + storageFormat;
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_object_storage_location_provider/obj-data";

        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES (" +
                        "'write.object-storage.enabled'=true," +
                        "'write.object-storage.path'='%s'," +
                        "'write.format.default' = '%s'," +
                        "'format-version' = %s)",
                sparkTableName, dataPath, storageFormat, specVersion));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        Row result = row("a_string", 1000000000000000L);
        assertThat(onSpark().executeQuery(format("SELECT _string, _bigint FROM %s", sparkTableName))).containsOnly(result);
        assertThat(onTrino().executeQuery(format("SELECT _string, _bigint FROM %s", trinoTableName))).containsOnly(result);

        QueryResult queryResult = onTrino().executeQuery(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult).hasRowsCount(1).hasColumnsCount(1);
        assertTrue(((String) queryResult.row(0).get(0)).contains(dataPath));

        // TODO: support path override in Iceberg table creation: https://github.com/trinodb/trino/issues/8861
        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE " + trinoTableName))
                .hasMessageContaining("contains Iceberg path override properties and cannot be dropped from Trino");
        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoWritingDataWithWriterDataPathSet(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = "test_writer_data_path_" + storageFormat;
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_writer_data_path_/obj-data";

        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES (" +
                        "'write.data.path'='%s'," +
                        "'write.format.default' = '%s'," +
                        "'format-version' = %s)",
                sparkTableName, dataPath, storageFormat, specVersion));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        Row result = row("a_string", 1000000000000000L);
        assertThat(onSpark().executeQuery(format("SELECT _string, _bigint FROM %s", sparkTableName))).containsOnly(result);
        assertThat(onTrino().executeQuery(format("SELECT _string, _bigint FROM %s", trinoTableName))).containsOnly(result);

        QueryResult queryResult = onTrino().executeQuery(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult).hasRowsCount(1).hasColumnsCount(1);
        assertTrue(((String) queryResult.row(0).get(0)).contains(dataPath));

        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE " + trinoTableName))
                .hasMessageContaining("contains Iceberg path override properties and cannot be dropped from Trino");
        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private static final List<String> SPECIAL_CHARACTER_VALUES = ImmutableList.of(
            "with-hyphen",
            "with.dot",
            "with:colon",
            "with/slash",
            "with\\\\backslashes",
            "with\\backslash",
            "with=equal",
            "with?question",
            "with!exclamation",
            "with%percent",
            "with%%percents",
            "with$dollar",
            "with#hash",
            "with*star",
            "with=equals",
            "with\"quote",
            "with'apostrophe",
            "with space",
            " with space prefix",
            "with space suffix ",
            "with€euro",
            "with non-ascii ąęłóść Θ Φ Δ",
            "with👨‍🏭combining character",
            " 👨‍🏭",
            "👨‍🏭 ");

    private static final String TRINO_INSERTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), ((value, index) -> format("(%d, '%s')", index, escapeTrinoString(value))))
                    .collect(Collectors.joining(", "));

    private static final String SPARK_INSERTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), ((value, index) -> format("(%d, '%s')", index, escapeSparkString(value))))
                    .collect(Collectors.joining(", "));

    private static final List<Row> EXPECTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), ((value, index) -> row((int) index, value)))
                    .collect(toImmutableList());

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testStringPartitioningWithSpecialCharactersCtasInTrino()
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_trino";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        onTrino().executeQuery(format(
                "CREATE TABLE %s (id, part_col) " +
                        "WITH (partitioning = ARRAY['part_col']) " +
                        "AS VALUES %s",
                trinoTableName,
                TRINO_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName);
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testStringPartitioningWithSpecialCharactersInsertInTrino()
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_trino";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        onTrino().executeQuery(format(
                "CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioning = ARRAY['part_col'])",
                trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES %s", trinoTableName, TRINO_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName);
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testStringPartitioningWithSpecialCharactersInsertInSpark()
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_spark";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        onTrino().executeQuery(format(
                "CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioning = ARRAY['part_col'])",
                trinoTableName));
        onSpark().executeQuery(format("INSERT INTO %s VALUES %s", sparkTableName, SPARK_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName);
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testInsertReadingFromParquetTableWithNestedRowFieldNotPresentInDataFile()
    {
        // regression test for https://github.com/trinodb/trino/issues/9264
        String sourceTableNameBase = "test_nested_missing_row_field_source";
        String trinoSourceTableName = trinoTableName(sourceTableNameBase);
        String sparkSourceTableName = sparkTableName(sourceTableNameBase);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoSourceTableName);
        onTrino().executeQuery(
                "CREATE TABLE " + trinoSourceTableName + " WITH (format = 'PARQUET') AS " +
                        " SELECT CAST(" +
                        "    ROW(1, ROW(2, 3)) AS " +
                        "    ROW(foo BIGINT, a_sub_struct ROW(x BIGINT, y BIGINT)) " +
                        ") AS a_struct");

        onSpark().executeQuery("ALTER TABLE " + sparkSourceTableName + " ADD COLUMN a_struct.a_sub_struct_2 STRUCT<z: BIGINT>");

        onTrino().executeQuery(
                "INSERT INTO " + trinoSourceTableName +
                        " SELECT CAST(" +
                        "    ROW(1, ROW(2, 3), ROW(4)) AS " +
                        "    ROW(foo BIGINT,\n" +
                        "        a_sub_struct ROW(x BIGINT, y BIGINT), " +
                        "        a_sub_struct_2 ROW(z BIGINT)" +
                        "    )" +
                        ") AS a_struct");

        String trinoTargetTableName = trinoTableName("test_nested_missing_row_field_target");
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTargetTableName);
        onTrino().executeQuery("CREATE TABLE " + trinoTargetTableName + " WITH (format = 'PARQUET') AS SELECT * FROM " + trinoSourceTableName);

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTargetTableName))
                .containsOnly(
                        row(
                                rowBuilder()
                                        .addField("foo", 1L)
                                        .addField(
                                                "a_sub_struct",
                                                rowBuilder()
                                                        .addField("x", 2L)
                                                        .addField("y", 3L)
                                                        .build())
                                        .addField(
                                                "a_sub_struct_2",
                                                null)
                                        .build()),
                        row(
                                rowBuilder()
                                        .addField("foo", 1L)
                                        .addField(
                                                "a_sub_struct",
                                                rowBuilder()
                                                        .addField("x", 2L)
                                                        .addField("y", 3L)
                                                        .build())
                                        .addField(
                                                "a_sub_struct_2",
                                                rowBuilder()
                                                        .addField("z", 4L)
                                                        .build())
                                        .build()));
    }

    private void assertSelectsOnSpecialCharacters(String trinoTableName, String sparkTableName)
    {
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        for (String value : SPECIAL_CHARACTER_VALUES) {
            String trinoValue = escapeTrinoString(value);
            String sparkValue = escapeSparkString(value);
            // Ensure Trino written metadata is readable from Spark and vice versa
            assertThat(onSpark().executeQuery("SELECT count(*) FROM " + sparkTableName + " WHERE part_col = '" + sparkValue + "'"))
                    .withFailMessage("Spark query with predicate containing '" + value + "' contained no matches, expected one")
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE part_col = '" + trinoValue + "'"))
                    .withFailMessage("Trino query with predicate containing '" + value + "' contained no matches, expected one")
                    .containsOnly(row(1));
        }
    }

    /**
     * @see TestIcebergInsert#testIcebergConcurrentInsert()
     */
    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, timeOut = 60_000)
    public void testTrinoSparkConcurrentInsert()
            throws Exception
    {
        int insertsPerEngine = 7;

        String baseTableName = "trino_spark_insert_concurrent_" + randomTableSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(e varchar, a bigint)");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CyclicBarrier barrier = new CyclicBarrier(2);
            QueryExecutor onTrino = onTrino();
            QueryExecutor onSpark = onSpark();
            List<Row> allInserted = executor.invokeAll(
                            Stream.of(Engine.TRINO, Engine.SPARK)
                                    .map(engine -> (Callable<List<Row>>) () -> {
                                        List<Row> inserted = new ArrayList<>();
                                        for (int i = 0; i < insertsPerEngine; i++) {
                                            barrier.await(20, SECONDS);
                                            String engineName = engine.name().toLowerCase(ENGLISH);
                                            long value = i;
                                            switch (engine) {
                                                case TRINO:
                                                    try {
                                                        onTrino.executeQuery(format("INSERT INTO %s VALUES ('%s', %d)", trinoTableName, engineName, value));
                                                    }
                                                    catch (QueryExecutionException queryExecutionException) {
                                                        // failed to insert
                                                        continue; // next loop iteration
                                                    }
                                                    break;
                                                case SPARK:
                                                    onSpark.executeQuery(format("INSERT INTO %s VALUES ('%s', %d)", sparkTableName, engineName, value));
                                                    break;
                                                default:
                                                    throw new UnsupportedOperationException("Unexpected engine: " + engine);
                                            }

                                            inserted.add(row(engineName, value));
                                        }
                                        return inserted;
                                    })
                                    .collect(toImmutableList())).stream()
                    .map(MoreFutures::getDone)
                    .flatMap(List::stream)
                    .collect(toImmutableList());

            // At least one INSERT per round should succeed
            Assertions.assertThat(allInserted).hasSizeBetween(insertsPerEngine, insertsPerEngine * 2);

            // All Spark inserts should succeed (and not be obliterated)
            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE e = 'spark'"))
                    .containsOnly(row(insertsPerEngine));

            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                    .containsOnly(allInserted);

            onTrino().executeQuery("DROP TABLE " + trinoTableName);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsAndCompressionCodecs")
    public void testTrinoReadingSparkCompressedData(StorageFormat storageFormat, String compressionCodec)
    {
        String baseTableName = "test_spark_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomTableSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        List<Row> rows = IntStream.range(0, 555)
                .mapToObj(i -> row("a" + i, i))
                .collect(toImmutableList());

        switch (storageFormat) {
            case PARQUET:
                onSpark().executeQuery("SET spark.sql.parquet.compression.codec = " + compressionCodec);
                break;

            case ORC:
                if ("GZIP".equals(compressionCodec)) {
                    onSpark().executeQuery("SET spark.sql.orc.compression.codec = zlib");
                }
                else {
                    onSpark().executeQuery("SET spark.sql.orc.compression.codec = " + compressionCodec);
                }
                break;

            default:
                throw new UnsupportedOperationException("Unsupported storage format: " + storageFormat);
        }

        onSpark().executeQuery(
                "CREATE TABLE " + sparkTableName + " (a string, b bigint) " +
                        "USING ICEBERG TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')");
        onSpark().executeQuery(
                "INSERT INTO " + sparkTableName + " VALUES " +
                        rows.stream()
                                .map(row -> format("('%s', %s)", row.getValues().get(0), row.getValues().get(1)))
                                .collect(Collectors.joining(", ")));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(rows);
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                .containsOnly(rows);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormatsAndCompressionCodecs")
    public void testSparkReadingTrinoCompressedData(StorageFormat storageFormat, String compressionCodec)
    {
        String baseTableName = "test_trino_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomTableSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("SET SESSION iceberg.compression_codec = '" + compressionCodec + "'");

        String createTable = "CREATE TABLE " + trinoTableName + " WITH (format = '" + storageFormat + "') AS TABLE tpch.tiny.nation";
        if (storageFormat == StorageFormat.PARQUET && "LZ4".equals(compressionCodec)) {
            // TODO (https://github.com/trinodb/trino/issues/9142) LZ4 is not supported with native Parquet writer
            assertQueryFailure(() -> onTrino().executeQuery(createTable))
                    .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Unsupported codec: LZ4");
            return;
        }
        onTrino().executeQuery(createTable);

        List<Row> expected = onTrino().executeQuery("TABLE tpch.tiny.nation").rows().stream()
                .map(row -> row(row.toArray()))
                .collect(toImmutableList());
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                .containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(expected);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void verifyCompressionCodecsDataProvider()
    {
        assertThat(onTrino().executeQuery("SHOW SESSION LIKE 'iceberg.compression_codec'"))
                .containsOnly(row(
                        "iceberg.compression_codec",
                        "ZSTD",
                        "ZSTD",
                        "varchar",
                        "Compression codec to use when writing files. Possible values: " + compressionCodecs()));
    }

    @DataProvider
    public Object[][] storageFormatsAndCompressionCodecs()
    {
        List<String> compressionCodecs = compressionCodecs();
        return Stream.of(StorageFormat.values())
                .filter(StorageFormat::isSupportedInTrino)
                .flatMap(storageFormat -> compressionCodecs.stream()
                        .map(compressionCodec -> new Object[] {storageFormat, compressionCodec}))
                .toArray(Object[][]::new);
    }

    private List<String> compressionCodecs()
    {
        return List.of(
                "NONE",
                "SNAPPY",
                "LZ4",
                "ZSTD",
                "GZIP");
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testTrinoReadingMigratedNestedData(StorageFormat storageFormat)
    {
        String baseTableName = "test_trino_reading_migrated_nested_data_" + randomTableSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_map MAP<STRING, ARRAY<STRUCT<sName: STRING, sNumber: INT>>>\n" +
                ", nested_array ARRAY<MAP<STRING, ARRAY<STRUCT<mName: STRING, mNumber: INT>>>>\n" +
                ", nested_struct STRUCT<id:INT, name:STRING, address:STRUCT<street_number:INT, street_name:STRING>>)\n" +
                " USING %s";
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName, storageFormat.name()));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", map('s1', array(named_struct('sName', 'ASName1', 'sNumber', 201), named_struct('sName', 'ASName2', 'sNumber', 202)))" +
                ", array(map('m1', array(named_struct('mName', 'MAS1Name1', 'mNumber', 301), named_struct('mName', 'MAS1Name2', 'mNumber', 302)))" +
                "       ,map('m2', array(named_struct('mName', 'MAS2Name1', 'mNumber', 401), named_struct('mName', 'MAS2Name2', 'mNumber', 402))))" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('street_number', 42, 'street_name', 'Wallaby Way'))";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));

        String sparkTableName = sparkTableName(baseTableName);
        Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "P. Sherman", 42, "Wallaby Way");

        String sparkSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][1].sName" +
                ", nested_map['s1'][0].sNumber" +
                ", nested_array[1]['m2'][0].mName" +
                ", nested_array[0]['m1'][1].mNumber" +
                ", nested_struct.name" +
                ", nested_struct.address.street_number" +
                ", nested_struct.address.street_name" +
                "  FROM ";

        QueryResult sparkResult = onSpark().executeQuery(sparkSelect + sparkTableName);
        // The Spark behavior when the default name mapping does not exist is not consistent
        assertThat(sparkResult).containsOnly(row);

        String trinoSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][2].sName" +
                ", nested_map['s1'][1].sNumber" +
                ", nested_array[2]['m2'][1].mName" +
                ", nested_array[1]['m1'][2].mNumber" +
                ", nested_struct.name" +
                ", nested_struct.address.street_number" +
                ", nested_struct.address.street_name" +
                "  FROM ";

        String trinoTableName = trinoTableName(baseTableName);
        QueryResult trinoResult = onTrino().executeQuery(trinoSelect + trinoTableName);
        assertThat(trinoResult).containsOnly(row);

        // After removing the name mapping, columns from migrated files should be null since they are missing the Iceberg Field IDs
        onSpark().executeQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES ('schema.name-mapping.default')", sparkTableName));
        assertThat(onTrino().executeQuery(trinoSelect + trinoTableName)).containsOnly(row(null, null, null, null, null, null, null, null));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(null, null, null, null));
        assertThat(onTrino().executeQuery("SELECT nested_struct.address.street_number, nested_struct.address.street_name FROM " + trinoTableName)).containsOnly(row(null, null));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testMigratedDataWithAlteredSchema(StorageFormat storageFormat)
    {
        String baseTableName = "test_migrated_data_with_altered_schema_" + randomTableSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_struct STRUCT<id:INT, name:STRING, address:STRUCT<a:INT, b:STRING>>)\n" +
                " USING %s";
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName, storageFormat.name()));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('a', 42, 'b', 'Wallaby Way'))";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));

        String sparkTableName = sparkTableName(baseTableName);
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " RENAME COLUMN nested_struct TO nested_struct_moved");

        String select = "SELECT" +
                " nested_struct_moved.name" +
                ", nested_struct_moved.address.a" +
                ", nested_struct_moved.address.b" +
                "  FROM ";
        Row row = row("P. Sherman", 42, "Wallaby Way");

        QueryResult sparkResult = onSpark().executeQuery(select + sparkTableName);
        assertThat(sparkResult).containsOnly(ImmutableList.of(row));

        String trinoTableName = trinoTableName(baseTableName);
        assertThat(onTrino().executeQuery(select + trinoTableName)).containsOnly(ImmutableList.of(row));

        // After removing the name mapping, columns from migrated files should be null since they are missing the Iceberg Field IDs
        onSpark().executeQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES ('schema.name-mapping.default')", sparkTableName));
        assertThat(onTrino().executeQuery(select + trinoTableName)).containsOnly(row(null, null, null));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testMigratedDataWithPartialNameMapping(StorageFormat storageFormat)
    {
        String baseTableName = "test_migrated_data_with_partial_name_mapping_" + randomTableSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "CREATE TABLE %s (a INT, b INT) USING " + storageFormat.name();
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName));

        String insert = "INSERT INTO TABLE %s SELECT 1, 2";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));

        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        // Test missing entry for column 'b'
        onSpark().executeQuery(format(
                "ALTER TABLE %s SET TBLPROPERTIES ('schema.name-mapping.default'='[{\"field-id\": 1, \"names\": [\"a\"]}, {\"field-id\": 2, \"names\": [\"c\"]} ]')",
                sparkTableName));
        assertThat(onTrino().executeQuery("SELECT a, b FROM " + trinoTableName))
                .containsOnly(row(1, null));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPartialStats()
    {
        String tableName = "test_partial_stats_" + randomTableSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2)");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "1"), row("col1", null, null, 0.0, null, "2", "2"), row(null, null, null, null, 1.0, null, null));

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES (write.metadata.metrics.column.col1='none')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (3, 4)");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "3"), row("col1", null, null, null, null, null, null), row(null, null, null, null, 2.0, null, null));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testStatsAfterAddingPartitionField()
    {
        String tableName = "test_stats_after_adding_partition_field_" + randomTableSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2)");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "1"), row("col1", null, null, 0.0, null, "2", "2"), row(null, null, null, null, 1.0, null, null));

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col1");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (3, 4)");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "3"), row("col1", null, null, 0.0, null, "2", "4"), row(null, null, null, null, 2.0, null, null));

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col1");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, col1)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (5, 6)");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "5"), row("col1", null, null, 0.0, null, "2", "6"), row(null, null, null, null, 3.0, null, null));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "tableFormatWithDeleteFormat")
    public void testTrinoReadsSparkRowLevelDeletes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat)
    {
        String tableName = format("test_trino_reads_spark_row_level_deletes_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomTableSuffix());
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(a INT, b INT) " +
                "USING ICEBERG PARTITIONED BY (b) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        onSpark().executeQuery("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + tableName + "', options => map('min-input-files','1'))");
        // Delete one row in a file
        onSpark().executeQuery("DELETE FROM " + sparkTableName + " WHERE a = 13");
        // Delete an entire partition
        onSpark().executeQuery("DELETE FROM " + sparkTableName + " WHERE b = 2");

        List<Row> expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "tableFormatWithDeleteFormat")
    public void testTrinoReadsSparkRowLevelDeletesWithRowTypes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat)
    {
        String tableName = format("test_trino_reads_spark_row_level_deletes_row_types_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomTableSuffix());
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(part_key INT, int_t INT, row_t STRUCT<a:INT, b:INT>) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES " +
                "(1, 1, named_struct('a', 1, 'b', 2)), (1, 2, named_struct('a', 3, 'b', 4)), (1, 3, named_struct('a', 5, 'b', 6)), (2, 4, named_struct('a', 1, 'b',2))");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        onSpark().executeQuery("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + tableName + "', options => map('min-input-files','1'))");
        onSpark().executeQuery("DELETE FROM " + sparkTableName + " WHERE int_t = 2");

        List<Row> expected = ImmutableList.of(row(1, 2), row(1, 6), row(2, 2));
        assertThat(onTrino().executeQuery("SELECT part_key, row_t.b FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT part_key, row_t.b FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private static String escapeSparkString(String value)
    {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    private static String escapeTrinoString(String value)
    {
        return value.replace("'", "''");
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.%s.%s", SPARK_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private static String sparkDefaultCatalogTableName(String tableName)
    {
        return format("%s.%s", TEST_SCHEMA_NAME, tableName);
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }

    @DataProvider
    public static Object[][] specVersions()
    {
        return new Object[][] {{1}, {2}};
    }

    @DataProvider
    public static Object[][] storageFormats()
    {
        return Stream.of(StorageFormat.values())
                .filter(StorageFormat::isSupportedInTrino)
                .map(storageFormat -> new Object[] {storageFormat})
                .toArray(Object[][]::new);
    }

    // Provides each supported table formats paired with each delete file format.
    @DataProvider
    public static Object[][] tableFormatWithDeleteFormat()
    {
        return Stream.of(StorageFormat.values())
                .filter(StorageFormat::isSupportedInTrino)
                .flatMap(tableStorageFormat -> Arrays.stream(StorageFormat.values())
                        .map(deleteFileStorageFormat -> new Object[]{tableStorageFormat, deleteFileStorageFormat}))
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] storageFormatsWithSpecVersion()
    {
        List<StorageFormat> storageFormats = Stream.of(StorageFormat.values())
                .filter(StorageFormat::isSupportedInTrino)
                .collect(toImmutableList());
        List<Integer> specVersions = ImmutableList.of(1, 2);

        return storageFormats.stream()
                .flatMap(storageFormat -> specVersions.stream().map(specVersion -> new Object[] {storageFormat, specVersion}))
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] unsupportedStorageFormats()
    {
        return Stream.of(StorageFormat.values())
                .filter(storageFormat -> !storageFormat.isSupportedInTrino())
                .map(storageFormat -> new Object[] {storageFormat})
                .toArray(Object[][]::new);
    }

    public enum StorageFormat
    {
        PARQUET,
        ORC,
        AVRO,
        /**/;

        public boolean isSupportedInTrino()
        {
            // TODO (https://github.com/trinodb/trino/issues/1324) not supported in Trino yet
            //  - remove testTrinoWithUnsupportedFileFormat once all formats are supported
            return this != AVRO;
        }
    }

    public enum CreateMode
    {
        CREATE_TABLE_AND_INSERT,
        CREATE_TABLE_AS_SELECT,
        CREATE_TABLE_WITH_NO_DATA_AND_INSERT,
    }
}
