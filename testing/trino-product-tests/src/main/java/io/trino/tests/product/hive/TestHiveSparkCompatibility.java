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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.HIVE_SPARK_NO_STATS_FALLBACK;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveSparkCompatibility
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String TRINO_CATALOG = "hive";

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS}, dataProvider = "testReadSparkCreatedTableDataProvider")
    public void testReadSparkCreatedTable(String sparkTableFormat, String expectedTrinoTableFormat)
    {
        String sparkTableName = "spark_created_table_" + sparkTableFormat.replaceAll("[^a-zA-Z]", "").toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(
                "CREATE TABLE default." + sparkTableName + "( " +
                        "  a_boolean boolean, " +
                        "  a_tinyint tinyint, " +
                        "  a_smallint smallint, " +
                        "  an_integer int, " +
                        "  a_bigint bigint, " +
                        "  a_real float, " +
                        "  a_double double, " +
                        "  a_short_decimal decimal(11, 4), " +
                        "  a_long_decimal decimal(26, 7), " +
                        "  a_string string, " +
                        // TODO "  a_binary binary, " +
                        "  a_date date, " +
                        "  a_timestamp_seconds timestamp, " +
                        "  a_timestamp_millis timestamp, " +
                        "  a_timestamp_micros timestamp, " +
                        "  a_timestamp_nanos timestamp, " +
                        // TODO interval
                        // TODO array
                        // TODO struct
                        // TODO map
                        "  a_dummy string) " +
                        sparkTableFormat + " " +
                        // By default Spark creates table as "transactional=true", but doesn't conform to Hive transactional format,
                        // nor file naming convention, so such table cannot be read. As a workaround, force table to be marked
                        // non-transactional.
                        "TBLPROPERTIES ('transactional'='false')");

        // nulls
        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (" + join(",", nCopies(16, "NULL")) + ")");
        // positive values
        onSpark().executeQuery(
                "INSERT INTO " + sparkTableName + " VALUES (" +
                        "true, " + // a_boolean
                        "127, " + // a_tinyint
                        "32767, " + // a_smallint
                        "1000000000, " + // an_integer
                        "1000000000000000, " + // a_bigint
                        "10000000.123, " + // a_real
                        "100000000000.123, " + // a_double
                        "CAST('1234567.8901' AS decimal(11, 4)), " + // a_short_decimal
                        "CAST('1234567890123456789.0123456' AS decimal(26, 7)), " + // a_short_decimal
                        "'some string', " + // a_string
                        "DATE '2005-09-10', " +  // a_date
                        "TIMESTAMP '2005-09-10 13:00:00', " + // a_timestamp_seconds
                        "TIMESTAMP '2005-09-10 13:00:00.123', " + // a_timestamp_millis
                        "TIMESTAMP '2005-09-10 13:00:00.123456', " + // a_timestamp_micros
                        "TIMESTAMP '2005-09-10 13:00:00.123456789', " + // a_timestamp_nanos
                        "'dummy')");
        // negative values
        onSpark().executeQuery(
                "INSERT INTO " + sparkTableName + " VALUES (" +
                        "false, " + // a_boolean
                        "-128, " + // a_tinyint
                        "-32768, " + // a_smallint
                        "-1000000012, " + // an_integer
                        "-1000000000000012, " + // a_bigint
                        "-10000000.123, " + // a_real
                        "-100000000000.123, " + // a_double
                        "CAST('-1234567.8901' AS decimal(11, 4)), " + // a_short_decimal
                        "CAST('-1234567890123456789.0123456' AS decimal(26, 7)), " + // a_short_decimal
                        "'', " + // a_string
                        "DATE '1965-09-10', " + // a_date
                        "TIMESTAMP '1965-09-10 13:00:00', " + // a_timestamp_seconds
                        "TIMESTAMP '1965-09-10 13:00:00.123', " + // a_timestamp_millis
                        "TIMESTAMP '1965-09-10 13:00:00.123456', " + // a_timestamp_micros
                        "TIMESTAMP '1965-09-10 13:00:00.123456789', " + // a_timestamp_nanos
                        "'dummy')");

        List<Row> expected = List.of(
                row(nCopies(16, null).toArray()),
                row(
                        true, // a_boolean≥
                        (byte) 127, // a_tinyint
                        (short) 32767, // a_smallint
                        1000000000, // an_integer
                        1000000000000000L, // a_bigint
                        10000000.123F, // a_real
                        100000000000.123, // a_double
                        new BigDecimal("1234567.8901"), // a_short_decimal
                        new BigDecimal("1234567890123456789.0123456"), // a_long_decimal
                        "some string", // a_string
                        java.sql.Date.valueOf(LocalDate.of(2005, 9, 10)), // a_date
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0)), // a_timestamp_seconds
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0, 123_000_000)), // a_timestamp_millis
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_micros
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_nanos; note that Spark timestamp has microsecond precision
                        "dummy"),
                row(
                        false, // a_bigint
                        (byte) -128, // a_tinyint
                        (short) -32768, // a_smallint
                        -1000000012, // an_integer
                        -1000000000000012L, // a_bigint
                        -10000000.123F, // a_real
                        -100000000000.123, // a_double
                        new BigDecimal("-1234567.8901"), // a_short_decimal
                        new BigDecimal("-1234567890123456789.0123456"), // a_long_decimal
                        "", // a_string
                        java.sql.Date.valueOf(LocalDate.of(1965, 9, 10)), // a_date
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0)), // a_timestamp_seconds
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0, 123_000_000)), // a_timestamp_millis
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_micros
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_nanos; note that Spark timestamp has microsecond precision
                        "dummy"));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        onTrino().executeQuery("SET SESSION hive.timestamp_precision = 'NANOSECONDS'");
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);

        assertThat(onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName))
                .containsOnly(row(format(
                        "CREATE TABLE %s (\n" +
                                "   a_boolean boolean,\n" +
                                "   a_tinyint tinyint,\n" +
                                "   a_smallint smallint,\n" +
                                "   an_integer integer,\n" +
                                "   a_bigint bigint,\n" +
                                "   a_real real,\n" +
                                "   a_double double,\n" +
                                "   a_short_decimal decimal(11, 4),\n" +
                                "   a_long_decimal decimal(26, 7),\n" +
                                "   a_string varchar,\n" +
                                "   a_date date,\n" +
                                "   a_timestamp_seconds timestamp(9),\n" +
                                "   a_timestamp_millis timestamp(9),\n" +
                                "   a_timestamp_micros timestamp(9),\n" +
                                "   a_timestamp_nanos timestamp(9),\n" +
                                "   a_dummy varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = '%s'\n" +
                                ")",
                        trinoTableName,
                        expectedTrinoTableFormat)));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS}, dataProvider = "sparkParquetTimestampFormats")
    public void testSparkParquetTimestampCompatibility(String sparkTimestampFormat, String sparkTimestamp, String[] expectedValues)
    {
        String sparkTableName = "test_spark_parquet_timestamp_compatibility_" + sparkTimestampFormat.toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery("SET spark.sql.parquet.outputTimestampType = " + sparkTimestampFormat);
        onSpark().executeQuery(
                "CREATE TABLE default." + sparkTableName + "(a_timestamp timestamp) " +
                        "USING PARQUET " +
                        "TBLPROPERTIES ('transactional'='false')");

        onSpark().executeQuery("INSERT INTO " + sparkTableName + " VALUES (TIMESTAMP '" + sparkTimestamp + "')");

        for (int i = 0; i < HIVE_TIMESTAMP_PRECISIONS.length; i++) {
            String trinoTimestampPrecision = HIVE_TIMESTAMP_PRECISIONS[i];
            String expected = expectedValues[i];
            onTrino().executeQuery("SET SESSION hive.timestamp_precision = '" + trinoTimestampPrecision + "'");
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(row(java.sql.Timestamp.valueOf(expected)));
            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE a_timestamp = TIMESTAMP '" + expected + "'")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + trinoTableName + " WHERE a_timestamp != TIMESTAMP '" + expected + "'")).containsOnly(row(0));
        }

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testSparkClusteringCaseSensitiveCompatibility()
    {
        String sparkTableNameWithClusteringDifferentCase = "test_spark_clustering_case_sensitive_" + randomNameSuffix();
        onSpark().executeQuery(
                String.format("CREATE TABLE %s (row_id int, `segment_id` int, value long) ", sparkTableNameWithClusteringDifferentCase) +
                        "USING PARQUET " +
                        "PARTITIONED BY (`part` string) " +
                        "CLUSTERED BY (`SEGMENT_ID`) " +
                        "  SORTED BY (`SEGMENT_ID`) " +
                        "  INTO 10 BUCKETS");

        onSpark().executeQuery(format("INSERT INTO %s ", sparkTableNameWithClusteringDifferentCase) +
                "VALUES " +
                "  (1, 1, 100, 'part1')," +
                "  (100, 1, 123, 'part2')," +
                "  (101, 2, 202, 'part2')");

        // Ensure that Trino can successfully read from the Spark bucketed table even though the clustering
        // column `SEGMENT_ID` is in a different case than the data column `segment_id`
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s.default.%s", TRINO_CATALOG, sparkTableNameWithClusteringDifferentCase)))
                .containsOnly(List.of(
                        row(1, 1, 100, "part1"),
                        row(100, 1, 123, "part2"),
                        row(101, 2, 202, "part2")));
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testSparkParquetBloomFilterCompatibility()
    {
        String sparkTableNameWithBloomFilter = "test_spark_parquet_bloom_filter_compatibility_enabled_" + randomNameSuffix();
        String sparkTableNameNoBloomFilter = "test_spark_parquet_bloom_filter_compatibility_disabled_" + randomNameSuffix();

        // disable dictionary predicate when testing bloom filter predicate
        onSpark().executeQuery(
                String.format("CREATE TABLE %s (testInteger INT, testLong Long, testString STRING, testDouble DOUBLE, testFloat FLOAT) ", sparkTableNameWithBloomFilter) +
                        "USING PARQUET OPTIONS (" +
                        "'parquet.bloom.filter.enabled'='true'," +
                        "'parquet.enable.dictionary'='false'" +
                        ")");
        onSpark().executeQuery(
                String.format("CREATE TABLE %s (testInteger INT, testLong Long, testString STRING, testDouble DOUBLE, testFloat FLOAT) ", sparkTableNameNoBloomFilter) +
                        "USING PARQUET OPTIONS (" +
                        "'parquet.bloom.filter.enabled'='false'," +
                        "'parquet.enable.dictionary'='false'" +
                        ")");
        String[] sparkTables = new String[] {sparkTableNameWithBloomFilter, sparkTableNameNoBloomFilter};
        String[] trinoTables = new String[] {
                format("%s.default.%s", TRINO_CATALOG, sparkTableNameWithBloomFilter),
                format("%s.default.%s", TRINO_CATALOG, sparkTableNameNoBloomFilter)};

        // control number of spark output files via hint: https://issues.apache.org/jira/browse/SPARK-24940
        // contain values such as aaaaaaaaaaa and zzzzzzzzzzz, this made sure file level statistics: min and max won't take effect
        for (String sparkTable : sparkTables) {
            onSpark().executeQuery(format(
                    "INSERT INTO %s " +
                            "SELECT /*+ REPARTITION(1) */  testInteger, testLong, testString, testDouble, testFloat FROM VALUES " +
                            "  (-999999, -999999, 'aaaaaaaaaaa', -9999999999.99D, -9999999.9999F)" +
                            ", (3, 30, 'fdsvxxbv33cb', 97662.2D, 98862.2F)" +
                            ", (5324, 2466, 'refgfdfrexx', 8796.1D, -65496.1F)" +
                            ", (999999, 9999999999999, 'zzzzzzzzzzz', 9999999999.99D, -9999999.9999F)" +
                            ", (9444, 4132455, 'ff34322vxff', 32137758.7892D, 9978.129887F) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                    sparkTable));
        }

        // explicitly make sure using bloom filter statistics
        onTrino().executeQuery("set session hive.parquet_use_bloom_filter=true");
        assertTrinoBloomFilterTableSelectResult(trinoTables);

        // explicitly make sure not using bloom filter statistics
        onTrino().executeQuery("set session hive.parquet_use_bloom_filter=false");
        assertTrinoBloomFilterTableSelectResult(trinoTables);

        for (String sparkTable : sparkTables) {
            onSpark().executeQuery("DROP TABLE " + sparkTable);
        }
    }

    private static void assertTrinoBloomFilterTableSelectResult(String[] trinoTables)
    {
        for (String trinoTable : trinoTables) {
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + trinoTable + " WHERE testInteger IN (9444, -88777, 6711111)")).containsOnly(List.of(row(1)));
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + trinoTable + " WHERE testLong IN (4132455, 321324, 312321321322)")).containsOnly(List.of(row(1)));
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + trinoTable + " WHERE testString IN ('fdsvxxbv33cb', 'cxxx322', 'cxxx323')")).containsOnly(List.of(row(1)));
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + trinoTable + " WHERE testDouble IN (DOUBLE '97662.2', DOUBLE '-97221.2', DOUBLE '-88777.22233')")).containsOnly(List.of(row(1)));
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + trinoTable + " WHERE testFloat IN (REAL '-65496.1', REAL '98211862.2', REAL '6761111555.1222')")).containsOnly(List.of(row(1)));
        }
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testInsertFailsOnBucketedTableCreatedBySpark()
    {
        String hiveTableName = "spark_insert_bucketed_table_" + randomNameSuffix();

        onSpark().executeQuery(
                "CREATE TABLE default." + hiveTableName + "(a_key integer, a_value integer) " +
                        "USING PARQUET " +
                        "CLUSTERED BY (a_key) INTO 3 BUCKETS");

        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO default." + hiveTableName + " VALUES (1, 100)"))
                .hasMessageContaining("Inserting into Spark bucketed tables is not supported");

        onSpark().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testUpdateFailsOnBucketedTableCreatedBySpark()
    {
        String hiveTableName = "spark_update_bucketed_table_" + randomNameSuffix();

        onSpark().executeQuery(
                "CREATE TABLE default." + hiveTableName + "(a_key integer, a_value integer) " +
                        "USING ORC " +
                        "CLUSTERED BY (a_key) INTO 3 BUCKETS");

        assertQueryFailure(() -> onTrino().executeQuery("UPDATE default." + hiveTableName + " SET a_value = 100 WHERE a_key = 1"))
                .hasMessageContaining("Merging into Spark bucketed tables is not supported");

        onSpark().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testDeleteFailsOnBucketedTableCreatedBySpark()
    {
        String hiveTableName = "spark_delete_bucketed_table_" + randomNameSuffix();

        onSpark().executeQuery(
                "CREATE TABLE default." + hiveTableName + "(a_key integer, a_value integer) " +
                        "USING ORC " +
                        "CLUSTERED BY (a_key) INTO 3 BUCKETS");

        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM default." + hiveTableName + " WHERE a_key = 1"))
                .hasMessageContaining("Merging into Spark bucketed tables is not supported");

        onSpark().executeQuery("DROP TABLE " + hiveTableName);
    }

    private static final String[] HIVE_TIMESTAMP_PRECISIONS = new String[]{"MILLISECONDS", "MICROSECONDS", "NANOSECONDS"};

    @DataProvider
    public static Object[][] sparkParquetTimestampFormats()
    {
        String millisTimestamp = "2005-09-10 13:00:00.123";
        String microsTimestamp = "2005-09-10 13:00:00.123456";
        String nanosTimestamp = "2005-09-10 13:00:00.123456789";

        // Ordering of expected values matches the ordering in HIVE_TIMESTAMP_PRECISIONS
        return new Object[][] {
                {"TIMESTAMP_MILLIS", millisTimestamp, new String[]{millisTimestamp, millisTimestamp, millisTimestamp}},
                {"TIMESTAMP_MICROS", microsTimestamp, new String[]{millisTimestamp, microsTimestamp, microsTimestamp}},
                // note that Spark timestamp has microsecond precision
                {"INT96", nanosTimestamp, new String[]{millisTimestamp, microsTimestamp, microsTimestamp}},
        };
    }

    @DataProvider
    public static Object[][] testReadSparkCreatedTableDataProvider()
    {
        return new Object[][] {
                {"USING ORC", "ORC"},
                {"USING PARQUET", "PARQUET"},
                // TODO add Avro
        };
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadTrinoCreatedOrcTable()
    {
        testReadTrinoCreatedTable("using_orc", "ORC");
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadTrinoCreatedParquetTable()
    {
        testReadTrinoCreatedTable("using_parquet", "PARQUET");
    }

    private void testReadTrinoCreatedTable(String tableName, String tableFormat)
    {
        String sparkTableName = "trino_created_table_" + tableName + "_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        // Spark timestamps are in microsecond precision
        onTrino().executeQuery("SET SESSION hive.timestamp_precision = 'MICROSECONDS'");
        onTrino().executeQuery(format(
                "CREATE TABLE %s ( " +
                        "   a_boolean boolean, " +
                        "   a_tinyint tinyint, " +
                        "   a_smallint smallint, " +
                        "   an_integer integer, " +
                        "   a_bigint bigint, " +
                        "   a_real real, " +
                        "   a_double double, " +
                        "   a_short_decimal decimal(11, 4), " +
                        "   a_long_decimal decimal(26, 7), " +
                        "   a_string varchar, " +
                        // TODO binary
                        "   a_date date, " +
                        "   a_timestamp_seconds timestamp(6), " +
                        "   a_timestamp_millis timestamp(6), " +
                        "   a_timestamp_micros timestamp(6), " +
                        // TODO interval
                        // TODO array
                        // TODO struct
                        // TODO map
                        "   a_dummy varchar " +
                        ") " +
                        "WITH ( " +
                        "   format = '%s' " +
                        ")",
                trinoTableName,
                tableFormat));

        // nulls
        onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (" + join(",", nCopies(15, "NULL")) + ")");
        // positive values
        onTrino().executeQuery(
                "INSERT INTO " + trinoTableName + " VALUES (" +
                        "true, " + // a_boolean
                        "127, " + // a_tinyint
                        "32767, " + // a_smallint
                        "1000000000, " + // an_integer
                        "1000000000000000, " + // a_bigint
                        "10000000.123, " + // a_real
                        "100000000000.123, " + // a_double
                        "CAST('1234567.8901' AS decimal(11, 4)), " + // a_short_decimal
                        "CAST('1234567890123456789.0123456' AS decimal(26, 7)), " + // a_short_decimal
                        "'some string', " + // a_string
                        "DATE '2005-09-10', " +  // a_date
                        "TIMESTAMP '2005-09-10 13:00:00', " + // a_timestamp_seconds
                        "TIMESTAMP '2005-09-10 13:00:00.123', " + // a_timestamp_millis
                        "TIMESTAMP '2005-09-10 13:00:00.123456', " + // a_timestamp_micros
                        "'dummy')");
        // negative values
        onTrino().executeQuery(
                "INSERT INTO " + trinoTableName + " VALUES (" +
                        "false, " + // a_boolean
                        "-128, " + // a_tinyint
                        "-32768, " + // a_smallint
                        "-1000000012, " + // an_integer
                        "-1000000000000012, " + // a_bigint
                        "-10000000.123, " + // a_real
                        "-100000000000.123, " + // a_double
                        "CAST('-1234567.8901' AS decimal(11, 4)), " + // a_short_decimal
                        "CAST('-1234567890123456789.0123456' AS decimal(26, 7)), " + // a_short_decimal
                        "'', " + // a_string
                        "DATE '1965-09-10', " + // a_date
                        "TIMESTAMP '1965-09-10 13:00:00', " + // a_timestamp_seconds
                        "TIMESTAMP '1965-09-10 13:00:00.123', " + // a_timestamp_millis
                        "TIMESTAMP '1965-09-10 13:00:00.123456', " + // a_timestamp_micros
                        "'dummy')");

        List<Row> expected = List.of(
                row(nCopies(15, null).toArray()),
                row(
                        true, // a_boolean
                        (byte) 127, // a_tinyint
                        (short) 32767, // a_smallint
                        1000000000, // an_integer
                        1000000000000000L, // a_bigint
                        10000000.123F, // a_real
                        100000000000.123, // a_double
                        new BigDecimal("1234567.8901"), // a_short_decimal
                        new BigDecimal("1234567890123456789.0123456"), // a_long_decimal
                        "some string", // a_string
                        java.sql.Date.valueOf(LocalDate.of(2005, 9, 10)), // a_date
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0)), // a_timestamp_seconds
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0, 123_000_000)), // a_timestamp_millis
                        java.sql.Timestamp.valueOf(LocalDateTime.of(2005, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_micros
                        "dummy"),
                row(
                        false, // a_bigint
                        (byte) -128, // a_tinyint
                        (short) -32768, // a_smallint
                        -1000000012, // an_integer
                        -1000000000000012L, // a_bigint
                        -10000000.123F, // a_real
                        -100000000000.123, // a_double
                        new BigDecimal("-1234567.8901"), // a_short_decimal
                        new BigDecimal("-1234567890123456789.0123456"), // a_long_decimal
                        "", // a_string
                        java.sql.Date.valueOf(LocalDate.of(1965, 9, 10)), // a_date
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0)), // a_timestamp_seconds
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0, 123_000_000)), // a_timestamp_millis
                        java.sql.Timestamp.valueOf(LocalDateTime.of(1965, 9, 10, 13, 0, 0, 123_456_000)), // a_timestamp_micros
                        "dummy"));
        assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadSparkdDateAndTimePartitionName()
    {
        String sparkTableName = "test_trino_reading_spark_date_and_time_type_partitioned_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(format("CREATE TABLE default.%s (value integer) PARTITIONED BY (dt date)", sparkTableName));

        // Spark allows creating partition with time unit
        // Hive denies creating such partitions, but allows reading
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00:00.000000000') VALUES (1)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00:00') VALUES (2)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00') VALUES (3)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='12345-06-07') VALUES (4)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='123-04-05') VALUES (5)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='-0001-01-01') VALUES (6)", sparkTableName));

        assertThat(onTrino().executeQuery("SELECT \"$partition\" FROM " + trinoTableName))
                .containsOnly(List.of(
                        row("dt=2022-04-13 00%3A00%3A00.000000000"),
                        row("dt=2022-04-13 00%3A00%3A00"),
                        row("dt=2022-04-13 00%3A00"),
                        row("dt=12345-06-07"),
                        row("dt=123-04-05"),
                        row("dt=-0001-01-01")));

        // Use date_format function to avoid exception due to java.sql.Date.valueOf() with 5 digit year
        assertThat(onSpark().executeQuery("SELECT value, date_format(dt, 'yyyy-MM-dd') FROM " + sparkTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, "+12345-06-07"),
                        row(5, null),
                        row(6, "-0001-01-01")));

        // Use date_format function to avoid exception due to java.sql.Date.valueOf() with 5 digit year
        assertThat(onHive().executeQuery("SELECT value, date_format(dt, 'yyyy-MM-dd') FROM " + sparkTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, "12345-06-07"),
                        row(5, "0123-04-06"),
                        row(6, "0002-01-03")));

        // Cast to varchar so that we can compare with Spark & Hive easily
        assertThat(onTrino().executeQuery("SELECT value, CAST(dt AS VARCHAR) FROM " + trinoTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, "12345-06-07"),
                        row(5, "0123-04-05"),
                        row(6, "-0001-01-01")));

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS}, dataProvider = "unsupportedPartitionDates")
    public void testReadSparkInvalidDatePartitionName(String inputDate, java.sql.Date outputDate)
    {
        String sparkTableName = "test_trino_reading_spark_invalid_date_type_partitioned_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(format("CREATE TABLE default.%s (value integer) PARTITIONED BY (dt date)", sparkTableName));

        // Spark allows creating partition with invalid date format
        // Hive denies creating such partitions, but allows reading
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='%s') VALUES (1)", sparkTableName, inputDate));

        // Hive ignores time unit, and return null for invalid dates
        assertThat(onHive().executeQuery("SELECT value, dt FROM " + sparkTableName))
                .containsOnly(List.of(row(1, outputDate)));

        // Trino throws an exception if the date is invalid format or not a whole round date
        assertQueryFailure(() -> onTrino().executeQuery("SELECT value, dt FROM " + trinoTableName))
                .hasMessageContaining("Invalid partition value");

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @DataProvider
    public static Object[][] unsupportedPartitionDates()
    {
        return new Object[][] {
                {"1965-09-10 23:59:59.999999999", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 23:59:59", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 23:59", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 00", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"2021-02-30", java.sql.Date.valueOf(LocalDate.of(2021, 3, 2))},
                {"1965-09-10 invalid", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"invalid date", null},
        };
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadSparkBucketedTable()
    {
        // Spark tables can be created using native Spark code or by going through Hive code
        // This tests the native Spark path.
        String sparkTableName = "test_trino_reading_spark_native_buckets_" + randomNameSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE `default`.`%s` (\n" +
                        "  `a_string` STRING,\n" +
                        "  `a_bigint` BIGINT,\n" +
                        "  `an_integer` INT,\n" +
                        "  `a_real` FLOAT,\n" +
                        "  `a_double` DOUBLE,\n" +
                        "  `a_boolean` BOOLEAN)\n" +
                        "USING ORC\n" +
                        "CLUSTERED BY (a_string)\n" +
                        "INTO 4 BUCKETS\n" +
                        // By default Spark creates table as "transactional=true", but doesn't conform to Hive transactional format,
                        // nor file naming convention, so such table cannot be read. As a workaround, force table to be marked
                        // non-transactional.
                        "TBLPROPERTIES ('transactional'='false')",
                sparkTableName));

        onSpark().executeQuery(format(
                "INSERT INTO %s VALUES " +
                        "('one', 1000000000000000, 1000000000, 10000000.123, 100000000000.123, true)" +
                        ", ('two', -1000000000000000, -1000000000, -10000000.123, -100000000000.123, false)" +
                        ", ('three', 2000000000000000, 2000000000, 20000000.123, 200000000000.123, true)" +
                        ", ('four', -2000000000000000, -2000000000, -20000000.123, -200000000000.123, false)",
                sparkTableName));

        List<Row> expected = List.of(
                row("one", 1000000000000000L, 1000000000, 10000000.123F, 100000000000.123, true),
                row("two", -1000000000000000L, -1000000000, -10000000.123F, -100000000000.123, false),
                row("three", 2000000000000000L, 2000000000, 20000000.123F, 200000000000.123, true),
                row("four", -2000000000000000L, -2000000000, -20000000.123F, -200000000000.123, false));
        assertThat(onSpark().executeQuery("SELECT a_string, a_bigint, an_integer, a_real, a_double, a_boolean FROM " + sparkTableName))
                .containsOnly(expected);
        assertThat(onTrino().executeQuery("SELECT a_string, a_bigint, an_integer, a_real, a_double, a_boolean FROM " + trinoTableName))
                .containsOnly(expected);

        assertThat(onTrino().executeQuery("SHOW CREATE TABLE " + trinoTableName))
                .containsOnly(row(format(
                        "CREATE TABLE %s (\n" +
                                "   a_string varchar,\n" +
                                "   a_bigint bigint,\n" +
                                "   an_integer integer,\n" +
                                "   a_real real,\n" +
                                "   a_double double,\n" +
                                "   a_boolean boolean\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC'\n" +
                                ")",
                        trinoTableName)));

        assertQueryFailure(() -> onTrino().executeQuery("SELECT a_string, a_bigint, an_integer, a_real, a_double, a_boolean, \"$bucket\" FROM " + trinoTableName))
                .hasMessageContaining("Column '$bucket' cannot be resolved");

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadSparkStatisticsUnpartitionedTable()
    {
        String tableName1 = "test_trino_reading_spark_statistics_table_" + randomNameSuffix();
        String tableName2 = "test_trino_reading_spark_statistics_table_" + randomNameSuffix();
        try {
            onSpark().executeQuery(format("CREATE TABLE %s(" +
                    "c_tinyint            BYTE, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             STRING, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             BINARY" +
                    ")", tableName1));

            onSpark().executeQuery(format("INSERT INTO %s VALUES " +
                    "(120, 32760, 2147483640, 9223372036854775800, 123.340, 234.560, CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as BINARY))," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)", tableName1));
            // Copy table to avoid hive metastore cache
            onSpark().executeQuery(format("CREATE TABLE %s as SELECT * FROM %s", tableName2, tableName1));
            // test table statistics only
            onSpark().executeQuery(format("ANALYZE TABLE %s COMPUTE STATISTICS", tableName2));
            assertThat(onTrino().executeQuery(format("SHOW STATS FOR %s", tableName2))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));
            // test table and column statistics
            onSpark().executeQuery(format("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", tableName1));
            assertThat(onTrino().executeQuery(format("SHOW STATS FOR %s", tableName1))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 9.0, 1.0, 0.5, null, null, null),
                    row("c_char", 10.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, null, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName1));
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName2));
        }
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadSparkStatisticsPartitionedTable()
    {
        String tableName = "test_trino_reading_spark_statistics_table_" + randomNameSuffix();
        try {
            onSpark().executeQuery(format("CREATE TABLE %s(" +
                    "c_tinyint            BYTE, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             STRING, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             BINARY," +
                    "p_bigint             BIGINT, " +
                    "p_varchar            VARCHAR(15) " +
                    ") partitioned by (p_bigint, p_varchar)", tableName));

            onSpark().executeQuery("set hive.exec.dynamic.partition.mode=nonstrict");
            onSpark().executeQuery(format("INSERT INTO %s VALUES " +
                    "(120, 32760, 2147483640, 9223372036854775800, 123.340, 234.560, CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as BINARY), 1, 'partition 1')," +
                    "(120, 32760, 2147483640, 9223372036854775800, 123.340, 234.560, CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as BINARY), 2, 'partition 2')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 2, 'partition 2')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1, 'partition 1')", tableName));
            onSpark().executeQuery(format("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", tableName));
            onSpark().executeQuery(format("ANALYZE TABLE %s PARTITION(p_bigint, p_varchar) COMPUTE STATISTICS", tableName));
            // test statistics for whole table
            assertThat(onTrino().executeQuery(format("SHOW STATS FOR %s", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row("p_bigint", null, 2.0, 0.0, null, "1", "2"),
                    row("p_varchar", 44.0, 2.0, 0.0, null, null, null),
                    row(null, null, null, null, 4.0, null, null)));
            // test statistics for single partition
            assertThat(onTrino().executeQuery(format("SHOW STATS FOR (SELECT * FROM %s  where p_bigint = 1)", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 22.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test(groups = {HIVE_SPARK_NO_STATS_FALLBACK, PROFILE_SPECIFIC_TESTS})
    public void testIgnoringSparkStatisticsWithDisabledFallback()
    {
        String tableName = "test_trino_reading_spark_statistics_table_" + randomNameSuffix();
        try {
            onSpark().executeQuery(format("CREATE TABLE %s(" +
                    "c_tinyint            BYTE, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             STRING, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             BINARY" +
                    ")", tableName));

            onSpark().executeQuery(format("INSERT INTO %s VALUES " +
                    "(120, 32760, 2147483640, 9223372036854775800, 123.340, 234.560, CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as BINARY))," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)", tableName));
            onSpark().executeQuery(format("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", tableName));
            assertThat(onTrino().executeQuery(format("SHOW STATS FOR %s", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null)));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
