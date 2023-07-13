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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.hive.Engine;
import io.trino.tests.product.hive.TestHiveMetastoreClientFactory;
import org.apache.thrift.TException;
import org.assertj.core.api.Assertions;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.ICEBERG_JDBC;
import static io.trino.tests.product.TestGroups.ICEBERG_NESSIE;
import static io.trino.tests.product.TestGroups.ICEBERG_REST;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AND_INSERT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AS_SELECT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_WITH_NO_DATA_AND_INSERT;
import static io.trino.tests.product.iceberg.util.IcebergTestUtils.getTableLocation;
import static io.trino.tests.product.iceberg.util.IcebergTestUtils.stripNamenodeURI;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests compatibility between Iceberg connector and Spark Iceberg.
 */
public class TestIcebergSparkCompatibility
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;
    @Inject
    private TestHiveMetastoreClientFactory testHiveMetastoreClientFactory;
    private ThriftMetastoreClient metastoreClient;

    @BeforeMethodWithContext
    public void setup()
            throws TException
    {
        metastoreClient = testHiveMetastoreClientFactory.createMetastoreClient();
        // Create 'default' schema if it doesn't exist because JDBC catalog doesn't have such schema
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.default WITH (location = 'hdfs://hadoop-master:9000/user/hive/warehouse/default')");
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        metastoreClient.close();
        metastoreClient = null;
    }

    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @BeforeMethodWithContext
    public void setUp()
    {
        // we create default schema so that we can re-use the same test for the Iceberg REST catalog (since Iceberg itself doesn't support a default schema)
        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_trino_reading_primitive_types_" + storageFormat);
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
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s WHERE _integer > 0", trinoTableName))).hasNoRows();

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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "testSparkReadingTrinoDataDataProvider")
    public void testSparkReadingTrinoData(StorageFormat storageFormat, CreateMode createMode)
    {
        String baseTableName = toLowerCase("test_spark_reading_primitive_types_" + storageFormat + "_" + createMode);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormats")
    public void testSparkReadTrinoUuid(StorageFormat storageFormat)
    {
        String tableName = toLowerCase("test_spark_read_trino_uuid_" + storageFormat);
        String trinoTableName = trinoTableName(tableName);
        String sparkTableName = sparkTableName(tableName);

        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s AS SELECT UUID '406caec7-68b9-4778-81b2-a12ece70c8b1' u",
                trinoTableName));

        assertQueryFailure(() -> onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                // TODO Iceberg Spark integration needs yet to gain support
                //  once this is supported, merge this test with testSparkReadingTrinoData()
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("org.apache.hive.service.cli.HiveSQLException: Error running query:.*\\Q java.lang.ClassCastException\\E(?s:.*)");

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "specVersions")
    public void testSparkCreatesTrinoDrops(int specVersion)
    {
        String baseTableName = "test_spark_creates_trino_drops";
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(baseTableName), specVersion));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE})
    public void testTrinoCreatesSparkDrops()
    {
        String baseTableName = "test_trino_creates_spark_drops";
        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT)", trinoTableName(baseTableName)));
        onSpark().executeQuery("DROP TABLE " + sparkTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormats")
    public void testSparkReadsTrinoPartitionedTable(StorageFormat storageFormat)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _varbinary VARBINARY, _bigint BIGINT) WITH (partitioning = ARRAY['_string', '_varbinary'], format = '%s')", trinoTableName, storageFormat));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", trinoTableName));

        Row row1 = row("b", new byte[] {15, -15, 2, -16, -2, -2}, 1002);
        String selectByString = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row1);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row1);

        Row row2 = row("a", new byte[] {15, -15, 2, -16, -2, -1}, 1001);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102f0feff'";
        assertThat(onTrino().executeQuery(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(onSpark().executeQuery(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadsSparkPartitionedTable(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_trino_reads_spark_partitioned_table_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (_string STRING, _varbinary BINARY, _bigint BIGINT) USING ICEBERG PARTITIONED BY (_string, _varbinary) TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));
        onSpark().executeQuery(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", sparkTableName));

        Row row1 = row("a", new byte[] {15, -15, 2, -16, -2, -1}, 1001);
        String select = "SELECT * FROM %s WHERE _string = 'a'";
        assertThat(onSpark().executeQuery(format(select, sparkTableName)))
                .containsOnly(row1);
        assertThat(onTrino().executeQuery(format(select, trinoTableName)))
                .containsOnly(row1);

        Row row2 = row("c", new byte[] {15, -15, 2, -3, -2, -1}, 1003);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102fdfeff'";
        assertThat(onTrino().executeQuery(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(onSpark().executeQuery(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testTrinoPartitionedByRealWithNaN(StorageFormat storageFormat)
    {
        testTrinoPartitionedByNaN("REAL", storageFormat, Float.NaN);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testTrinoPartitionedByDoubleWithNaN(StorageFormat storageFormat)
    {
        testTrinoPartitionedByNaN("DOUBLE", storageFormat, Double.NaN);
    }

    private void testTrinoPartitionedByNaN(String typeName, StorageFormat storageFormat, Object expectedValue)
    {
        String baseTableName = "test_trino_partitioned_by_nan_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "', partitioning = ARRAY['col'])" +
                "AS SELECT " + typeName + " 'NaN' AS col");

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(expectedValue));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(expectedValue));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkPartitionedByRealWithNaN(StorageFormat storageFormat)
    {
        testSparkPartitionedByNaN("FLOAT", storageFormat, Float.NaN);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkPartitionedByDoubleWithNaN(StorageFormat storageFormat)
    {
        testSparkPartitionedByNaN("DOUBLE", storageFormat, Double.NaN);
    }

    private void testSparkPartitionedByNaN(String typeName, StorageFormat storageFormat, Object expectedValue)
    {
        String baseTableName = "test_spark_partitioned_by_nan_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + " " +
                "PARTITIONED BY (col) " +
                "TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT CAST('NaN' AS " + typeName + ") AS col");

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(expectedValue));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(expectedValue));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPartitionedByNestedField()
    {
        String baseTableName = "test_trino_nested_field_partition_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("" +
                        "CREATE TABLE %s (" +
                        "  id INT," +
                        "  parent STRUCT<nested:STRING, nested_another:STRING>)" +
                        "  USING ICEBERG" +
                        "  PARTITIONED BY (parent.nested)" +
                        "  TBLPROPERTIES ('format-version'=2)",
                sparkTableName));

        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (2, ROW('b'))"))
                .hasMessageContaining("Partitioning by nested field is unsupported: parent.nested");
        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + trinoTableName + " SET id = 2"))
                .hasMessageContaining("Partitioning by nested field is unsupported: parent.nested");
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + trinoTableName))
                .hasMessageContaining("Partitioning by nested field is unsupported: parent.nested");
        assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO " + trinoTableName + " t USING " + trinoTableName + " s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET id = 2"))
                .hasMessageContaining("Partitioning by nested field is unsupported: parent.nested");
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " EXECUTE OPTIMIZE"))
                .hasMessageContaining("Partitioning by nested field is unsupported: parent.nested");
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " DROP COLUMN parent.nested"))
                .hasMessageContaining("Cannot drop partition field: parent.nested");

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingCompositeSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_trino_reading_spark_composites_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormats")
    public void testSparkReadingCompositeTrinoData(StorageFormat storageFormat)
    {
        String baseTableName = toLowerCase("test_spark_reading_trino_composites_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingSparkIcebergTablePropertiesData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_trino_reading_spark_iceberg_table_properties_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadingNestedSparkData(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_trino_reading_nested_spark_data_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormats")
    public void testSparkReadingNestedTrinoData(StorageFormat storageFormat)
    {
        String baseTableName = toLowerCase("test_spark_reading_nested_trino_data_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testIdBasedFieldMapping(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_schema_evolution_for_nested_fields_" + storageFormat);
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testReadAfterPartitionEvolution(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_read_after_partition_evolution_" + storageFormat);
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

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS}, dataProvider = "specVersions")
    public void testTrinoShowingSparkCreatedTables(int specVersion)
    {
        String sparkTable = "test_table_listing_for_spark";
        String trinoTable = "test_table_listing_for_trino";

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTable);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTable);

        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(sparkTable), specVersion));
        onTrino().executeQuery(format("CREATE TABLE %s (_integer INTEGER )", trinoTableName(trinoTable)));

        assertThat(onTrino().executeQuery(format("SHOW TABLES FROM %s.%s LIKE '%s'", TRINO_CATALOG, TEST_SCHEMA_NAME, "test_table_listing_for_%")))
                .containsOnly(row(sparkTable), row(trinoTable));

        onSpark().executeQuery("DROP TABLE " + sparkTableName(sparkTable));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(trinoTable));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "specVersions")
    public void testCreateAndDropTableWithSameLocationWorksOnSpark(int specVersion)
    {
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_create_table_same_location/obj-data";
        String tableSameLocation1 = "test_same_location_spark_1_" + randomNameSuffix();
        String tableSameLocation2 = "test_same_location_spark_2_" + randomNameSuffix();

        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation1), dataPath, specVersion));
        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation2), dataPath, specVersion));

        onSpark().executeQuery(format("DROP TABLE IF EXISTS %s", sparkTableName(tableSameLocation1)));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName(tableSameLocation2)))).hasNoRows();

        onSpark().executeQuery(format("DROP TABLE %s", sparkTableName(tableSameLocation2)));
    }

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS}, dataProvider = "specVersions")
    public void testCreateAndDropTableWithSameLocationFailsOnTrino(int specVersion)
    {
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_create_table_same_location/obj-data";
        String tableSameLocation1 = "test_same_location_trino_1_" + randomNameSuffix();
        String tableSameLocation2 = "test_same_location_trino_2_" + randomNameSuffix();

        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation1), dataPath, specVersion));
        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation2), dataPath, specVersion));

        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName(tableSameLocation1)));

        assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName(tableSameLocation2))))
                .hasMessageMatching(".*Metadata not found in metadata location for table default." + tableSameLocation2);

        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName(tableSameLocation2)));
    }

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoWritingDataWithObjectStorageLocationProvider(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_object_storage_location_provider_" + storageFormat);
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
        assertTrue(((String) queryResult.getOnlyValue()).contains(dataPath));

        // TODO: support path override in Iceberg table creation: https://github.com/trinodb/trino/issues/8861
        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE " + trinoTableName))
                .hasMessageContaining("contains Iceberg path override properties and cannot be dropped from Trino");
        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS, ICEBERG_NESSIE}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoWritingDataWithWriterDataPathSet(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_writer_data_path_" + storageFormat);
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
        assertTrue(((String) queryResult.getOnlyValue()).contains(dataPath));

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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE})
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE})
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE})
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testPartitionedByNonLowercaseColumn()
    {
        String baseTableName = "test_partitioned_by_non_lowercase_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + " USING ICEBERG PARTITIONED BY (`PART`) TBLPROPERTIES ('format-version'='2') AS SELECT 1 AS data, 2 AS `PART`");
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).contains(row(1, 2));

            onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (3, 4)");
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).contains(row(1, 2), row(3, 4));

            onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE data = 3");
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).contains(row(1, 2));

            onTrino().executeQuery("UPDATE " + trinoTableName + " SET part = 20");
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).contains(row(1, 20));

            onTrino().executeQuery("MERGE INTO " + trinoTableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE");
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).hasNoRows();
        }
        finally {
            onSpark().executeQuery("DROP TABLE " + sparkTableName);
        }
    }

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS})
    public void testPartitioningWithMixedCaseColumnUnsupportedInTrino()
    {
        String baseTableName = "test_partitioning_with_mixed_case_column_in_spark";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        onSpark().executeQuery(format(
                "CREATE TABLE %s (id INTEGER, `mIxEd_COL` STRING) USING ICEBERG",
                sparkTableName));
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY['mIxEd_COL']"))
                .hasMessageContaining("Unable to parse partitioning value");
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY['\"mIxEd_COL\"']"))
                .hasMessageContaining("Unable to parse partitioning value");
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadsSparkSortOrder()
    {
        String sourceTableNameBase = "test_insert_into_sorted_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(sourceTableNameBase);
        String sparkTableName = sparkTableName(sourceTableNameBase);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + " (a INT, b INT, c INT) USING ICEBERG");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " WRITE ORDERED BY b, c DESC NULLS LAST");

        Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName).getOnlyValue())
                .contains("sorted_by = ARRAY['b ASC NULLS FIRST','c DESC NULLS LAST']");

        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (3, 2, 1), (1, 2, 3), (NULL, NULL, NULL)");
        assertThat(onSpark().executeQuery("SELECT _pos, a, b, c FROM " + sparkTableName))
                .contains(
                        row(0, null, null, null),
                        row(1, 1, 2, 3),
                        row(2, 3, 2, 1));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoIgnoresUnsupportedSparkSortOrder()
    {
        String sourceTableNameBase = "test_insert_into_sorted_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(sourceTableNameBase);
        String sparkTableName = sparkTableName(sourceTableNameBase);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + " (a INT, b INT, c INT) USING ICEBERG");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " WRITE ORDERED BY truncate(b, 3), a NULLS LAST");

        Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName).getOnlyValue())
                .doesNotContain("sorted_by");

        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (3, 2333, 1), (NULL, NULL, NULL), (1, 2222, 3)");
        assertThat(onSpark().executeQuery("SELECT _pos, a, b, c FROM " + sparkTableName))
                .contains(
                        row(0, 1, 2222, 3),
                        row(1, 3, 2333, 1),
                        row(2, null, null, null));
    }

    /**
     * @see TestIcebergInsert#testIcebergConcurrentInsert()
     */
    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, timeOut = 60_000)
    public void testTrinoSparkConcurrentInsert()
            throws Exception
    {
        int insertsPerEngine = 7;

        String baseTableName = "trino_spark_insert_concurrent_" + randomNameSuffix();
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsAndCompressionCodecs")
    public void testTrinoReadingSparkCompressedData(StorageFormat storageFormat, String compressionCodec)
    {
        String baseTableName = toLowerCase("test_spark_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomNameSuffix());
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

            case AVRO:
                if ("NONE".equals(compressionCodec)) {
                    onSpark().executeQuery("SET spark.sql.avro.compression.codec = uncompressed");
                }
                else if ("SNAPPY".equals(compressionCodec)) {
                    onSpark().executeQuery("SET spark.sql.avro.compression.codec = snappy");
                }
                else if ("ZSTD".equals(compressionCodec)) {
                    onSpark().executeQuery("SET spark.sql.avro.compression.codec = zstandard");
                }
                else {
                    assertQueryFailure(() -> onSpark().executeQuery("SET spark.sql.avro.compression.codec = " + compressionCodec))
                            .hasMessageContaining("The value of spark.sql.avro.compression.codec should be one of bzip2, deflate, uncompressed, xz, snappy, zstandard");
                    throw new SkipException("Unsupported compression codec");
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC, ICEBERG_NESSIE}, dataProvider = "storageFormatsAndCompressionCodecs")
    public void testSparkReadingTrinoCompressedData(StorageFormat storageFormat, String compressionCodec)
    {
        String baseTableName = toLowerCase("test_trino_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomNameSuffix());
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
        if (storageFormat == StorageFormat.AVRO && (compressionCodec.equals("LZ4"))) {
            assertQueryFailure(() -> onTrino().executeQuery(createTable))
                    .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Unsupported compression codec: " + compressionCodec);
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

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS})
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

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testTrinoReadingMigratedNestedData(StorageFormat storageFormat)
    {
        String baseTableName = "test_trino_reading_migrated_nested_data_" + randomNameSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_map MAP<STRING, ARRAY<STRUCT<sName: STRING, sNumber: INT>>>\n" +
                ", nested_array ARRAY<MAP<STRING, ARRAY<STRUCT<mName: STRING, mNumber: INT>>>>\n" +
                ", nested_struct STRUCT<id:INT, name:STRING, address:STRUCT<street_number:INT, street_name:STRING>>)\n" +
                " USING %s";
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName, storageFormat.name().toLowerCase(ENGLISH)));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", map('s1', array(named_struct('sName', 'ASName1', 'sNumber', 201), named_struct('sName', 'ASName2', 'sNumber', 202)))" +
                ", array(map('m1', array(named_struct('mName', 'MAS1Name1', 'mNumber', 301), named_struct('mName', 'MAS1Name2', 'mNumber', 302)))" +
                "       ,map('m2', array(named_struct('mName', 'MAS2Name1', 'mNumber', 401), named_struct('mName', 'MAS2Name2', 'mNumber', 402))))" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('street_number', 42, 'street_name', 'Wallaby Way'))";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        try {
            onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (QueryExecutionException e) {
            if (e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                throw new SkipException("This catalog doesn't support calling system.migrate procedure");
            }
        }

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

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testMigratedDataWithAlteredSchema(StorageFormat storageFormat)
    {
        String baseTableName = "test_migrated_data_with_altered_schema_" + randomNameSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_struct STRUCT<id:INT, name:STRING, address:STRUCT<a:INT, b:STRING>>)\n" +
                " USING %s";
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName, storageFormat));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('a', 42, 'b', 'Wallaby Way'))";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        try {
            onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (QueryExecutionException e) {
            if (e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                throw new SkipException("This catalog doesn't support calling system.migrate procedure");
            }
        }

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

    @Test(groups = {ICEBERG, ICEBERG_JDBC, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testMigratedDataWithPartialNameMapping(StorageFormat storageFormat)
    {
        String baseTableName = "test_migrated_data_with_partial_name_mapping_" + randomNameSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "CREATE TABLE %s (a INT, b INT) USING " + storageFormat.name().toLowerCase(ENGLISH);
        onSpark().executeQuery(format(sparkTableDefinition, defaultCatalogTableName));

        String insert = "INSERT INTO TABLE %s SELECT 1, 2";
        onSpark().executeQuery(format(insert, defaultCatalogTableName));
        try {
            onSpark().executeQuery(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (QueryExecutionException e) {
            if (e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                throw new SkipException("This catalog doesn't support calling system.migrate procedure");
            }
        }

        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        // Test missing entry for column 'b'
        onSpark().executeQuery(format(
                "ALTER TABLE %s SET TBLPROPERTIES ('schema.name-mapping.default'='[{\"field-id\": 1, \"names\": [\"a\"]}, {\"field-id\": 2, \"names\": [\"c\"]} ]')",
                sparkTableName));
        assertThat(onTrino().executeQuery("SELECT a, b FROM " + trinoTableName))
                .containsOnly(row(1, null));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testPartialStats()
    {
        String tableName = "test_partial_stats_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT, col2 STRING, col3 BINARY)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2, 'col2Value0', X'000102f0feff')");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(
                        row("col0", null, null, 0.0, null, "1", "1"),
                        row("col1", null, null, 0.0, null, "2", "2"),
                        row("col2", 151.0, null, 0.0, null, null, null),
                        row("col3", 72.0, null, 0.0, null, null, null),
                        row(null, null, null, null, 1.0, null, null));

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES (write.metadata.metrics.column.col1='none')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (3, 4, 'col2Value1', X'000102f0feee')");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + trinoTableName))
                .containsOnly(
                        row("col0", null, null, 0.0, null, "1", "3"),
                        row("col1", null, null, null, null, null, null),
                        row("col2", 305.0, null, 0.0, null, null, null),
                        row("col3", 145.0, null, 0.0, null, null, null),
                        row(null, null, null, null, 2.0, null, null));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testStatsAfterAddingPartitionField()
    {
        String tableName = "test_stats_after_adding_partition_field_" + randomNameSuffix();
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "tableFormatWithDeleteFormat")
    public void testTrinoReadsSparkRowLevelDeletes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat)
    {
        String tableName = toLowerCase(format("test_trino_reads_spark_row_level_deletes_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomNameSuffix()));
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

        // Delete to a file that already has deleted rows
        onSpark().executeQuery("DELETE FROM " + sparkTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "tableFormatWithDeleteFormat")
    public void testTrinoReadsSparkRowLevelDeletesWithRowTypes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat)
    {
        String tableName = toLowerCase(format("test_trino_reads_spark_row_level_deletes_row_types_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomNameSuffix()));
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

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormats")
    public void testSparkReadsTrinoRowLevelDeletes(StorageFormat storageFormat)
    {
        String tableName = toLowerCase(format("test_spark_reads_trino_row_level_deletes_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(a INT, b INT) WITH(partitioning = ARRAY['b'], format_version = 2, format = '" + storageFormat.name() + "')");
        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Delete one row in a file
        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE a = 13");
        // Delete an entire partition
        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE b = 2");

        List<Row> expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete to a file that already has deleted rows
        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormats")
    public void testSparkReadsTrinoRowLevelDeletesWithRowTypes(StorageFormat storageFormat)
    {
        String tableName = toLowerCase(format("test_spark_reads_trino_row_level_deletes_row_types_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(part_key INT, int_t INT, row_t ROW(a INT, b INT)) " +
                "WITH(partitioning = ARRAY['part_key'], format_version = 2, format = '" + storageFormat.name() + "') ");
        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (1, 1, row(1, 2)), (1, 2, row(3, 4)), (1, 3, row(5, 6)), (2, 4, row(1, 2))");
        onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE int_t = 2");

        List<Row> expected = ImmutableList.of(row(1, 2), row(1, 6), row(2, 2));
        assertThat(onTrino().executeQuery("SELECT part_key, row_t.b FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT part_key, row_t.b FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormats")
    public void testDeleteAfterPartitionEvolution(StorageFormat storageFormat)
    {
        String baseTableName = toLowerCase("test_delete_after_partition_evolution_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);
        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "col0 BIGINT, " +
                        "col1 BIGINT, " +
                        "col2 BIGINT) "
                        + " USING ICEBERG"
                        + " TBLPROPERTIES ('write.format.default' = '%s', 'format-version' = 2, 'write.delete.mode' = 'merge-on-read')",
                sparkTableName,
                storageFormat));
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 11, 21)");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, col0)");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (2, 12, 22)");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col1");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (3, 13, 23)");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col1");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col2");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (4, 14, 24)");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD bucket(3, col0)");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col2");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col0");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (5, 15, 25)");

        List<Row> expected = new ArrayList<>();
        expected.add(row(1, 11, 21));
        expected.add(row(2, 12, 22));
        expected.add(row(3, 13, 23));
        expected.add(row(4, 14, 24));
        expected.add(row(5, 15, 25));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);

        for (int columnValue = 1; columnValue <= 5; columnValue++) {
            onTrino().executeQuery("DELETE FROM " + trinoTableName + " WHERE col0 = " + columnValue);
            // Rows are in order so removing the first one always matches columnValue
            expected.remove(0);
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
            assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        }

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST})
    public void testMissingMetrics()
    {
        String tableName = "test_missing_metrics_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        onSpark().executeQuery("CREATE TABLE " + sparkTableName + " (name STRING, country STRING) USING ICEBERG " +
                "PARTITIONED BY (country) TBLPROPERTIES ('write.metadata.metrics.default'='none')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES ('Christoph', 'AT'), (NULL, 'RO')");
        assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s.%s.\"%s$partitions\" WHERE data IS NOT NULL", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName)))
                .containsOnly(row(0));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testOptimizeOnV2IcebergTable()
    {
        String tableName = format("test_optimize_on_v2_iceberg_table_%s", randomNameSuffix());
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);
        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(a INT, b INT) " +
                "USING ICEBERG PARTITIONED BY (b) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));

        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, 2), row(2, 2), row(3, 2), row(11, 12), row(12, 12), row(13, 12));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableExecuteProceduresOnEmptyTable()
    {
        String baseTableName = "test_alter_table_execute_procedures_on_empty_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  _string STRING" +
                        ", _bigint BIGINT" +
                        ", _integer INTEGER" +
                        ") USING ICEBERG",
                sparkTableName));

        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " EXECUTE optimize");
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " EXECUTE expire_snapshots(retention_threshold => '7d')");
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " EXECUTE remove_orphan_files(retention_threshold => '7d')");

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).hasNoRows();
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
                .map(storageFormat -> new Object[] {storageFormat})
                .toArray(Object[][]::new);
    }

    // Provides each supported table formats paired with each delete file format.
    @DataProvider
    public static Object[][] tableFormatWithDeleteFormat()
    {
        return Stream.of(StorageFormat.values())
                .flatMap(tableStorageFormat -> Arrays.stream(StorageFormat.values())
                        .map(deleteFileStorageFormat -> new Object[] {tableStorageFormat, deleteFileStorageFormat}))
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] storageFormatsWithSpecVersion()
    {
        List<StorageFormat> storageFormats = Stream.of(StorageFormat.values())
                .collect(toImmutableList());
        List<Integer> specVersions = ImmutableList.of(1, 2);

        return storageFormats.stream()
                .flatMap(storageFormat -> specVersions.stream().map(specVersion -> new Object[] {storageFormat, specVersion}))
                .toArray(Object[][]::new);
    }

    public enum StorageFormat
    {
        PARQUET,
        ORC,
        AVRO,
        /**/;
    }

    public enum CreateMode
    {
        CREATE_TABLE_AND_INSERT,
        CREATE_TABLE_AS_SELECT,
        CREATE_TABLE_WITH_NO_DATA_AND_INSERT
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormats")
    public void testSparkReadsTrinoTableAfterCleaningUp(StorageFormat storageFormat)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_after_expiring_snapshots" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s')", trinoTableName, storageFormat));
        // separate inserts give us snapshot per insert
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1004)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1005)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1006)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1007)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1008)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1009)", trinoTableName));
        onTrino().executeQuery((format("DELETE FROM %s WHERE _string = '%s'", trinoTableName, 'b')));
        int initialNumberOfMetadataFiles = calculateMetadataFilesForPartitionedTable(baseTableName);

        onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
        onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));

        int updatedNumberOfMetadataFiles = calculateMetadataFilesForPartitionedTable(baseTableName);
        Assertions.assertThat(updatedNumberOfMetadataFiles).isLessThan(initialNumberOfMetadataFiles);

        Row row = row(3006);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormatsWithSpecVersion")
    public void testSparkReadsTrinoTableAfterOptimizeAndCleaningUp(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_after_expiring_snapshots_after_optimize" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s', format_version = %s)", trinoTableName, storageFormat, specVersion));
        // separate inserts give us snapshot per insert
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1004)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1005)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('b', 1006)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1007)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1008)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('c', 1009)", trinoTableName));
        onTrino().executeQuery((format("DELETE FROM %s WHERE _string = '%s'", trinoTableName, 'b')));
        int initialNumberOfFiles = onTrino().executeQuery(format("SELECT * FROM iceberg.default.\"%s$files\"", baseTableName)).getRowsCount();
        int initialNumberOfMetadataFiles = calculateMetadataFilesForPartitionedTable(baseTableName);

        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));
        onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
        onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));

        int updatedNumberOfFiles = onTrino().executeQuery(format("SELECT * FROM iceberg.default.\"%s$files\"", baseTableName)).getRowsCount();
        Assertions.assertThat(updatedNumberOfFiles).isLessThan(initialNumberOfFiles);
        int updatedNumberOfMetadataFiles = calculateMetadataFilesForPartitionedTable(baseTableName);
        Assertions.assertThat(updatedNumberOfMetadataFiles).isLessThan(initialNumberOfMetadataFiles);

        Row row = row(3006);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "storageFormatsWithSpecVersion")
    public void testTrinoReadsTrinoTableWithSparkDeletesAfterOptimizeAndCleanUp(StorageFormat storageFormat, int specVersion)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_with_deletes_after_expiring_snapshots_after_optimize" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s', format_version = %s)", trinoTableName, storageFormat, specVersion));
        // separate inserts give us snapshot per insert
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        onSpark().executeQuery(format("DELETE FROM %s WHERE _bigint = 1002", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1004)", trinoTableName));

        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));
        onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
        onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));

        Row row = row(3008);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC}, dataProvider = "tableFormatWithDeleteFormat")
    public void testCleaningUpIcebergTableWithRowLevelDeletes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat)
    {
        String baseTableName = toLowerCase("test_cleaning_up_iceberg_table_fails_for_table_v2" + tableStorageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(part_key INT, int_t INT, row_t STRUCT<a:INT, b:INT>) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES " +
                "(1, 1, named_struct('a', 1, 'b', 2)), (1, 2, named_struct('a', 3, 'b', 4)), (1, 3, named_struct('a', 5, 'b', 6)), (2, 4, named_struct('a', 1, 'b', 2)), (2, 2, named_struct('a', 2, 'b', 3))");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        onSpark().executeQuery("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + baseTableName + "', options => map('min-input-files','1'))");
        onSpark().executeQuery("DELETE FROM " + sparkTableName + " WHERE int_t = 2");

        Row row = row(4);
        String selectByString = "SELECT SUM(int_t) FROM %s WHERE part_key = 1";
        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row);

        onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
        onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
        onTrino().executeQuery(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));

        assertThat(onTrino().executeQuery(format(selectByString, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(selectByString, sparkTableName)))
                .containsOnly(row);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testUpdateAfterSchemaEvolution()
    {
        String baseTableName = "test_update_after_schema_evolution_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(part_key INT, a INT, b INT, c INT) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 2, 3, 4), (11, 12, 13, 14)");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD part_key");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD a");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP COLUMN b");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP COLUMN c");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD COLUMN c INT");

        List<Row> expected = ImmutableList.of(row(1, 2, null), row(11, 12, null));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Because of the DROP/ADD on column c these two should be no-op updates
        onTrino().executeQuery("UPDATE " + trinoTableName + " SET c = c + 1");
        onTrino().executeQuery("UPDATE " + trinoTableName + " SET a = a + 1 WHERE c = 4");
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Check the new data files are using the updated partition scheme
        List<Object> filePaths = onTrino().executeQuery("SELECT DISTINCT file_path FROM " + TRINO_CATALOG + "." + TEST_SCHEMA_NAME + ".\"" + baseTableName + "$files\"").column(1);
        assertEquals(
                filePaths.stream()
                        .map(String::valueOf)
                        .filter(path -> path.contains("/a=") && !path.contains("/part_key="))
                        .count(),
                2);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testUpdateOnPartitionColumn()
    {
        String baseTableName = "test_update_on_partition_column" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(a INT, b STRING) " +
                "USING ICEBERG PARTITIONED BY (a) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (1, 'first'), (1, 'second'), (2, 'third'), (2, 'forth'), (2, 'fifth')");

        onTrino().executeQuery("UPDATE " + trinoTableName + " SET a = a + 1");
        List<Row> expected = ImmutableList.of(row(2, "first"), row(2, "second"), row(3, "third"), row(3, "forth"), row(3, "fifth"));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onTrino().executeQuery("UPDATE " + trinoTableName + " SET a = a + (CASE b WHEN 'first' THEN 1 ELSE 0 END)");
        expected = ImmutableList.of(row(3, "first"), row(2, "second"), row(3, "third"), row(3, "forth"), row(3, "fifth"));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Test moving rows from one file into different partitions, compact first
        onSpark().executeQuery("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + baseTableName + "', options => map('min-input-files','1'))");
        onTrino().executeQuery("UPDATE " + trinoTableName + " SET a = a + (CASE b WHEN 'forth' THEN -1 ELSE 1 END)");
        expected = ImmutableList.of(row(4, "first"), row(3, "second"), row(4, "third"), row(2, "forth"), row(4, "fifth"));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testAddNotNullColumn()
    {
        String baseTableName = "test_add_not_null_column_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " AS SELECT 1 col");
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1));

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ADD COLUMN new_col INT NOT NULL"))
                .hasMessageMatching(".*This connector does not support adding not null columns");
        assertQueryFailure(() -> onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD COLUMN new_col INT NOT NULL"))
                .hasMessageMatching("(?s).*Unsupported table change: Incompatible change: cannot add required column.*");

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1));
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testDropNestedField()
    {
        String baseTableName = "test_drop_nested_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " AS SELECT CAST(row(1, 2, row(10, 20)) AS row(a integer, b integer, c row(x integer, y integer))) AS col");

        // Drop a nested field
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " DROP COLUMN col.b");
        assertThat(onTrino().executeQuery("SELECT col.a, col.c.x, col.c.y FROM " + trinoTableName)).containsOnly(row(1, 10, 20));
        assertThat(onSpark().executeQuery("SELECT col.a, col.c.x, col.c.y FROM " + sparkTableName)).containsOnly(row(1, 10, 20));

        // Drop a row type having fields
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " DROP COLUMN col.c");
        assertThat(onTrino().executeQuery("SELECT col.a FROM " + trinoTableName)).containsOnly(row(1));
        assertThat(onSpark().executeQuery("SELECT col.a FROM " + sparkTableName)).containsOnly(row(1));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testDropPastPartitionedField()
    {
        String baseTableName = "test_drop_past_partitioned_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(id INTEGER, parent ROW(nested VARCHAR, nested_another VARCHAR))");
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD parent.nested");
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY[]");

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + trinoTableName + " DROP COLUMN parent.nested"))
                .hasMessageContaining("Cannot drop column which is used by an old partition spec: parent.nested");

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS, ICEBERG_REST, ICEBERG_JDBC})
    public void testHandlingPartitionSchemaEvolutionInPartitionMetadata()
    {
        String baseTableName = "test_handling_partition_schema_evolution_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (old_partition_key INT, new_partition_key INT, value date) WITH (PARTITIONING = array['old_partition_key'])", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 10, date '2022-04-10'), (2, 20, date '2022-05-11'), (3, 30, date '2022-06-12'), (2, 20, date '2022-06-13')", trinoTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1"),
                ImmutableMap.of("old_partition_key", "2"),
                ImmutableMap.of("old_partition_key", "3")));

        onSpark().executeQuery(format("ALTER TABLE %s DROP PARTITION FIELD old_partition_key", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD PARTITION FIELD new_partition_key", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")));

        onTrino().executeQuery(format("INSERT INTO %s VALUES (4, 40, date '2022-08-15')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")));

        onSpark().executeQuery(format("ALTER TABLE %s DROP PARTITION FIELD new_partition_key", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD PARTITION FIELD old_partition_key", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")));

        onTrino().executeQuery(format("INSERT INTO %s VALUES (5, 50, date '2022-08-15')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")));

        onSpark().executeQuery(format("ALTER TABLE %s DROP PARTITION FIELD old_partition_key", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD PARTITION FIELD days(value)", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null")));

        onTrino().executeQuery(format("INSERT INTO %s VALUES (6, 60, date '2022-08-16')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null")));

        onSpark().executeQuery(format("ALTER TABLE %s DROP PARTITION FIELD value_day", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD PARTITION FIELD months(value)", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null", "value_month", "null")));

        onTrino().executeQuery(format("INSERT INTO %s VALUES (7, 70, date '2022-08-17')", trinoTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "null", "value_month", "631"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null", "value_month", "null")));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testMetadataCompressionCodecGzip()
    {
        // Verify that Trino can read and write to a table created by Spark
        String baseTableName = "test_metadata_compression_codec_gzip" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(col int) USING iceberg TBLPROPERTIES ('write.metadata.compression-codec'='gzip')");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1)");
        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (2)");

        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2));

        // Verify that all metadata file is compressed as Gzip
        String tableLocation = stripNamenodeURI(getTableLocation(trinoTableName));
        List<String> metadataFiles = hdfsClient.listDirectory(tableLocation + "/metadata").stream()
                .filter(file -> file.endsWith("metadata.json"))
                .collect(toImmutableList());
        Assertions.assertThat(metadataFiles)
                .isNotEmpty()
                .filteredOn(file -> file.endsWith("gz.metadata.json"))
                .isEqualTo(metadataFiles);

        // Change 'write.metadata.compression-codec' to none and insert and select the table in Trino
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES ('write.metadata.compression-codec'='none')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2));

        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (3)");
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2), row(3));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private void validatePartitioning(String baseTableName, String sparkTableName, List<Map<String, String>> expectedValues)
    {
        List<String> trinoResult = expectedValues.stream().map(m ->
                m.entrySet().stream()
                        .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(", ", "{", "}")))
                .collect(toImmutableList());
        List<Object> partitioning = onTrino().executeQuery(format("SELECT partition, record_count FROM iceberg.default.\"%s$partitions\"", baseTableName))
                .column(1);
        Set<String> partitions = partitioning.stream().map(String::valueOf).collect(toUnmodifiableSet());
        Assertions.assertThat(partitions.size()).isEqualTo(expectedValues.size());
        Assertions.assertThat(partitions).containsAll(trinoResult);
        List<String> sparkResult = expectedValues.stream().map(m ->
                m.entrySet().stream()
                        .map(entry -> format("\"%s\":%s", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(",", "{", "}")))
                .collect(toImmutableList());
        partitioning = onSpark().executeQuery(format("SELECT partition from %s.files", sparkTableName)).column(1);
        partitions = partitioning.stream().map(String::valueOf).collect(toUnmodifiableSet());
        Assertions.assertThat(partitions.size()).isEqualTo(expectedValues.size());
        Assertions.assertThat(partitions).containsAll(sparkResult);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoAnalyze()
    {
        String baseTableName = "test_trino_analyze_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " AS SELECT regionkey, name FROM tpch.tiny.region");
        onTrino().executeQuery("ANALYZE " + trinoTableName);

        // We're not verifying results of ANALYZE (covered by non-product tests), but we're verifying table is readable.
        List<Row> expected = List.of(row(0, "AFRICA"), row(1, "AMERICA"), row(2, "ASIA"), row(3, "EUROPE"), row(4, "MIDDLE EAST"));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoAnalyzeWithNonLowercaseColumnName()
    {
        String baseTableName = "test_trino_analyze_with_uppercase_field" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName + "(col1 INT, COL2 INT) USING ICEBERG");
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (1, 1)");
        onTrino().executeQuery("ANALYZE " + trinoTableName);

        // We're not verifying results of ANALYZE (covered by non-product tests), but we're verifying table is readable.
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(1, 1));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(1, 1));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithTableLocation(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_table_location_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        List<Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false));
        String tableLocation = getTableLocation(trinoTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(onSpark().executeQuery(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithComments(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_comments_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));
        onTrino().executeQuery(format("COMMENT ON TABLE %s is 'my-table-comment'", trinoTableName));
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.a is 'a-comment'", trinoTableName));
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.b is 'b-comment'", trinoTableName));
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.c is 'c-comment'", trinoTableName));

        String tableLocation = getTableLocation(trinoTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation));

        Assertions.assertThat(getTableComment(baseTableName)).isEqualTo("my-table-comment");
        Assertions.assertThat(getColumnComment(baseTableName, "a")).isEqualTo("a-comment");
        Assertions.assertThat(getColumnComment(baseTableName, "b")).isEqualTo("b-comment");
        Assertions.assertThat(getColumnComment(baseTableName, "c")).isEqualTo("c-comment");
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithShowCreateTable(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_show_create_table_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        QueryResult expectedDescribeTable = onSpark().executeQuery("DESCRIBE TABLE EXTENDED " + sparkTableName);
        List<Row> expectedDescribeTableRows = expectedDescribeTable.rows().stream().map(columns -> row(columns.toArray())).collect(toImmutableList());

        QueryResult expectedShowCreateTable = onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName);
        List<Row> expectedShowCreateTableRows = expectedShowCreateTable.rows().stream().map(columns -> row(columns.toArray())).collect(toImmutableList());

        String tableLocation = getTableLocation(trinoTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation));

        QueryResult actualDescribeTable = onSpark().executeQuery("DESCRIBE TABLE EXTENDED " + sparkTableName);
        QueryResult actualShowCreateTable = onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName);

        assertThat(actualDescribeTable).hasColumns(expectedDescribeTable.getColumnTypes()).containsExactlyInOrder(expectedDescribeTableRows);
        assertThat(actualShowCreateTable).hasColumns(expectedShowCreateTable.getColumnTypes()).containsExactlyInOrder(expectedShowCreateTableRows);
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithReInsert(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_re_insert_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = getTableLocation(trinoTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation));
        onSpark().executeQuery(format("INSERT INTO %s values(3, 'POLAND', true)", sparkTableName));

        List<Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(onSpark().executeQuery(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithDroppedTable(StorageFormat storageFormat)
    {
        String baseTableName = "test_register_table_with_dropped_table_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = getTableLocation(trinoTableName);
        String baseTableNameNew = baseTableName + "_new";

        // Drop table to verify register_table call fails when no metadata can be found (table doesn't exist)
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));

        assertQueryFailure(() -> onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableNameNew, tableLocation)))
                .hasMessageMatching(".*No versioned metadata file exists at location.*");
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithDifferentTableName(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_different_table_name_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = getTableLocation(trinoTableName);
        String baseTableNameNew = baseTableName + "_new";
        String trinoTableNameNew = trinoTableName(baseTableNameNew);
        String sparkTableNameNew = sparkTableName(baseTableNameNew);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableNameNew, tableLocation));
        onSpark().executeQuery(format("INSERT INTO %s values(3, 'POLAND', true)", sparkTableNameNew));
        List<Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableNameNew))).containsOnly(expected);
        assertThat(onSpark().executeQuery(format("SELECT * FROM %s", sparkTableNameNew))).containsOnly(expected);
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableNameNew));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testRegisterTableWithMetadataFile(StorageFormat storageFormat)
            throws TException
    {
        String baseTableName = "test_register_table_with_metadata_file_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        onSpark().executeQuery(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        onTrino().executeQuery(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = getTableLocation(trinoTableName);
        String metadataLocation = metastoreClient.getTable(TEST_SCHEMA_NAME, baseTableName).getParameters().get("metadata_location");
        String metadataFileName = metadataLocation.substring(metadataLocation.lastIndexOf("/") + 1);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(baseTableName);

        onTrino().executeQuery(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation, metadataFileName));
        onTrino().executeQuery(format("INSERT INTO %s values(3, 'POLAND', true)", trinoTableName));
        List<Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(onSpark().executeQuery(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        onTrino().executeQuery(format("DROP TABLE %s", trinoTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testUnregisterNotIcebergTable()
    {
        String baseTableName = "test_unregister_not_iceberg_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String hiveTableName = TEST_SCHEMA_NAME + "." + baseTableName;

        onHive().executeQuery("CREATE TABLE " + hiveTableName + " AS SELECT 1 a");

        assertThatThrownBy(() -> onTrino().executeQuery("CALL iceberg.system.unregister_table('default', '" + baseTableName + "')"))
                .hasMessageContaining("Not an Iceberg table");

        assertThat(onSpark().executeQuery("SELECT * FROM " + hiveTableName)).containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT * FROM hive.default." + baseTableName)).containsOnly(row(1));
        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                .hasMessageContaining("Not an Iceberg table");

        onHive().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testSetColumnTypeDataProvider")
    public void testTrinoSetColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        testTrinoSetColumnType(false, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testSetColumnTypeDataProvider")
    public void testTrinoSetPartitionedColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        testTrinoSetColumnType(true, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue);
    }

    private void testTrinoSetColumnType(boolean partitioned, StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        String baseTableName = "test_set_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "'" + (partitioned ? ", partitioning = ARRAY['col']" : "") + ")" +
                "AS SELECT CAST(" + sourceValueLiteral + " AS " + sourceColumnType + ") AS col");

        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE " + newColumnType);

        assertEquals(getColumnType(baseTableName, "col"), newColumnType);
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(newValue));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(newValue));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @DataProvider
    public static Object[][] testSetColumnTypeDataProvider()
    {
        return cartesianProduct(
                Stream.of(StorageFormat.values())
                        .collect(toDataProvider()),
                new Object[][] {
                        {"integer", "2147483647", "bigint", 2147483647L},
                        {"real", "10.3", "double", 10.3},
                        {"real", "'NaN'", "double", Double.NaN},
                        {"decimal(5,3)", "'12.345'", "decimal(10,3)", BigDecimal.valueOf(12.345)}
                });
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testTrinoAlterStructColumnType(StorageFormat storageFormat)
    {
        String baseTableName = "test_trino_alter_row_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "')" +
                "AS SELECT CAST(row(1, 2) AS row(a integer, b integer)) AS col");

        // Add a nested field
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, b integer, c integer)");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, b integer, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Update a nested field
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, b bigint, c integer)");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, b bigint, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Drop a nested field
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, c integer)");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.c FROM " + sparkTableName)).containsOnly(row(1, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.c FROM " + trinoTableName)).containsOnly(row(1, null));

        // Adding a nested field with the same name doesn't restore the old data
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, c integer, b bigint)");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, c integer, b bigint)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.c, col.b FROM " + sparkTableName)).containsOnly(row(1, null, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.c, col.b FROM " + trinoTableName)).containsOnly(row(1, null, null));

        // Reorder fields
        onTrino().executeQuery("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(c integer, b bigint, a integer)");
        assertEquals(getColumnType(baseTableName, "col"), "row(c integer, b bigint, a integer)");
        assertThat(onSpark().executeQuery("SELECT col.b, col.c, col.a FROM " + sparkTableName)).containsOnly(row(null, null, 1));
        assertThat(onTrino().executeQuery("SELECT col.b, col.c, col.a FROM " + trinoTableName)).containsOnly(row(null, null, 1));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testSparkAlterColumnType")
    public void testSparkAlterColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        testSparkAlterColumnType(false, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testSparkAlterColumnType")
    public void testSparkAlterPartitionedColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        testSparkAlterColumnType(true, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue);
    }

    private void testSparkAlterColumnType(boolean partitioned, StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue)
    {
        String baseTableName = "test_spark_alter_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName +
                (partitioned ? " PARTITIONED BY (col)" : "") +
                " TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT CAST(" + sourceValueLiteral + " AS " + sourceColumnType + ") AS col");

        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ALTER COLUMN col TYPE " + newColumnType);

        assertEquals(getColumnType(baseTableName, "col"), newColumnType);
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(row(newValue));
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(newValue));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @DataProvider
    public static Object[][] testSparkAlterColumnType()
    {
        return cartesianProduct(
                Stream.of(StorageFormat.values())
                        .collect(toDataProvider()),
                new Object[][] {
                        {"integer", "2147483647", "bigint", 2147483647L},
                        {"float", "10.3", "double", 10.3},
                        {"float", "'NaN'", "double", Double.NaN},
                        {"decimal(5,3)", "'12.345'", "decimal(10,3)", BigDecimal.valueOf(12.345)}
                });
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "storageFormats")
    public void testSparkAlterStructColumnType(StorageFormat storageFormat)
    {
        String baseTableName = "test_spark_alter_struct_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("CREATE TABLE " + sparkTableName +
                " TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT named_struct('a', 1, 'b', 2) AS col");

        // Add a nested field
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD COLUMN col.c integer");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, b integer, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Update a nested field
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ALTER COLUMN col.b TYPE bigint");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, b bigint, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Drop a nested field
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " DROP COLUMN col.b");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, c integer)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.c FROM " + sparkTableName)).containsOnly(row(1, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.c FROM " + trinoTableName)).containsOnly(row(1, null));

        // Adding a nested field with the same name doesn't restore the old data
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ADD COLUMN col.b bigint");
        assertEquals(getColumnType(baseTableName, "col"), "row(a integer, c integer, b bigint)");
        assertThat(onSpark().executeQuery("SELECT col.a, col.c, col.b FROM " + sparkTableName)).containsOnly(row(1, null, null));
        assertThat(onTrino().executeQuery("SELECT col.a, col.c, col.b FROM " + trinoTableName)).containsOnly(row(1, null, null));

        // Reorder fields
        onSpark().executeQuery("ALTER TABLE " + sparkTableName + " ALTER COLUMN col.a AFTER b");
        assertEquals(getColumnType(baseTableName, "col"), "row(c integer, b bigint, a integer)");
        assertThat(onSpark().executeQuery("SELECT col.b, col.c, col.a FROM " + sparkTableName)).containsOnly(row(null, null, 1));
        assertThat(onTrino().executeQuery("SELECT col.b, col.c, col.a FROM " + trinoTableName)).containsOnly(row(null, null, 1));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private String getColumnType(String tableName, String columnName)
    {
        return (String) onTrino().executeQuery("SELECT data_type FROM " + TRINO_CATALOG + ".information_schema.columns " +
                        "WHERE table_schema = '" + TEST_SCHEMA_NAME + "' AND " +
                        "table_name = '" + tableName + "' AND " +
                        "column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    private int calculateMetadataFilesForPartitionedTable(String tableName)
    {
        String dataFilePath = (String) onTrino().executeQuery(format("SELECT file_path FROM iceberg.default.\"%s$files\" limit 1", tableName)).getOnlyValue();
        String partitionPath = dataFilePath.substring(0, dataFilePath.lastIndexOf("/"));
        String dataFolderPath = partitionPath.substring(0, partitionPath.lastIndexOf("/"));
        String tableFolderPath = dataFolderPath.substring(0, dataFolderPath.lastIndexOf("/"));
        String metadataFolderPath = tableFolderPath + "/metadata";
        return hdfsClient.listDirectory(URI.create(metadataFolderPath).getPath()).size();
    }

    private void dropTableFromMetastore(String tableName)
            throws TException
    {
        metastoreClient.dropTable(TEST_SCHEMA_NAME, tableName, false);
        assertThatThrownBy(() -> metastoreClient.getTable(TEST_SCHEMA_NAME, tableName)).hasMessageContaining("table not found");
    }

    private String getTableComment(String tableName)
    {
        return (String) onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '" + TRINO_CATALOG + "' AND schema_name = '" + TEST_SCHEMA_NAME + "' AND table_name = '" + tableName + "'").getOnlyValue();
    }

    private String getColumnComment(String tableName, String columnName)
    {
        return (String) onTrino().executeQuery("SELECT comment FROM " + TRINO_CATALOG + ".information_schema.columns WHERE table_schema = '" + TEST_SCHEMA_NAME + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'").getOnlyValue();
    }

    private static String toLowerCase(String name)
    {
        return name.toLowerCase(ENGLISH);
    }
}
