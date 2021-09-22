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

import io.trino.tempto.ProductTest;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveSparkCompatibility
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String TRINO_CATALOG = "hive";

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS}, dataProvider = "testReadSparkCreatedTableDataProvider")
    public void testReadSparkCreatedTable(String sparkTableFormat, String expectedTrinoTableFormat)
    {
        String sparkTableName = "spark_created_table_" + sparkTableFormat.replaceAll("[^a-zA-Z]", "").toLowerCase(ENGLISH) + "_" + randomTableSuffix();
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

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadTrinoCreatedParquetTableWithNativeWriter()
    {
        onTrino().executeQuery("SET SESSION " + TRINO_CATALOG + ".experimental_parquet_optimized_writer_enabled = true");
        // TODO (https://github.com/trinodb/trino/issues/6377) Native Parquet Writer writes Parquet V2 files that are not compatible with Spark's vectorized reader, see https://github.com/trinodb/trino/issues/7953 for more details
        assertThatThrownBy(() -> testReadTrinoCreatedTable("using_native_parquet", "PARQUET"))
                .hasStackTraceContaining("at org.apache.hive.jdbc.HiveStatement.execute")
                .extracting(Throwable::toString, InstanceOfAssertFactories.STRING)
                .matches("\\Qio.trino.tempto.query.QueryExecutionException: java.sql.SQLException: Error running query: java.lang.UnsupportedOperationException: Unsupported encoding: RLE\\E");
    }

    private void testReadTrinoCreatedTable(String tableName, String tableFormat)
    {
        String sparkTableName = "trino_created_table_" + tableName + "_" + randomTableSuffix();
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
    public void testReadSparkBucketedTable()
    {
        // Spark tables can be created using native Spark code or by going through Hive code
        // This tests the native Spark path.
        String sparkTableName = "test_trino_reading_spark_native_buckets_" + randomTableSuffix();
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
}
