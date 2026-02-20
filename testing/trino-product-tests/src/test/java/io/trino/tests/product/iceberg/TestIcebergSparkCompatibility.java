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
import io.airlift.concurrent.MoreFutures;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AND_INSERT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_AS_SELECT;
import static io.trino.tests.product.iceberg.TestIcebergSparkCompatibility.CreateMode.CREATE_TABLE_WITH_NO_DATA_AND_INSERT;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests compatibility between Iceberg connector and Spark Iceberg.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestIcebergSparkCompatibility
{
    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

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
            "with\u20aceuro",
            "with non-ascii \u0105\u0119\u0142\u00f3\u015b\u0107 \u0398 \u03a6 \u0394",
            "with\ud83d\udc68\u200d\ud83c\udfedcombining character",
            " \ud83d\udc68\u200d\ud83c\udfed",
            "\ud83d\udc68\u200d\ud83c\udfed ");

    private static final String TRINO_INSERTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), (value, index) -> format("(%d, '%s')", index, escapeTrinoString(value)))
                    .collect(Collectors.joining(", "));

    private static final String SPARK_INSERTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), (value, index) -> format("(%d, '%s')", index, escapeSparkString(value)))
                    .collect(Collectors.joining(", "));

    private static final List<io.trino.testing.containers.environment.Row> EXPECTED_PARTITION_VALUES =
            Streams.mapWithIndex(SPECIAL_CHARACTER_VALUES.stream(), (value, index) -> row((int) index, value))
                    .collect(toImmutableList());

    @BeforeAll
    static void createDefaultSchema(SparkIcebergEnvironment env)
    {
        // Create 'default' schema if it doesn't exist because JDBC catalog doesn't have such schema
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.default WITH (location = 'hdfs://hadoop-master:9000/user/hive/warehouse/default')");
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadingSparkData(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reading_primitive_types_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);

        env.executeSparkUpdate(format(
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
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName("\"" + baseTableName + "$snapshots\"")))).hasNoRows();
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName))).hasNoRows();
        assertThat(env.executeTrino(format("SELECT * FROM %s WHERE _integer > 0", trinoTableName))).hasNoRows();

        env.executeSparkUpdate(format(
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

        io.trino.testing.containers.environment.Row expectedRow = row(
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

        assertThat(env.executeSpark(
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
                .containsOnly(expectedRow);

        assertThat(env.executeTrino(
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
                .containsOnly(expectedRow);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("testSparkReadingTrinoDataDataProvider")
    void testSparkReadingTrinoData(StorageFormat storageFormat, CreateMode createMode, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reading_primitive_types_" + storageFormat + "_" + createMode);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        String namedValues =
                """
                SELECT\s
                VARCHAR 'a_string' _string\s
                , 1000000000000000 _bigint\s
                , 1000000000 _integer\s
                , REAL '10000000.123' _real\s
                , DOUBLE '100000000000.123' _double\s
                , DECIMAL '123456.78' _short_decimal\s
                , DECIMAL '1234567890123456789.0123456789012345678' _long_decimal\s
                , true _boolean\s
                , TIMESTAMP '2020-06-28 14:16:00.123456' _timestamp\s
                , TIMESTAMP '2021-08-03 08:32:21.123456 Europe/Warsaw' _timestamptz\s
                , DATE '1950-06-28' _date\s
                , X'000102f0feff' _binary\s
                , UUID '406caec7-68b9-4778-81b2-a12ece70c8b1' _uuid\s
                """;

        switch (createMode) {
            case CREATE_TABLE_AND_INSERT:
                env.executeTrinoUpdate(
                        """
                        CREATE TABLE %s (
                          _string VARCHAR
                        , _bigint BIGINT
                        , _integer INTEGER
                        , _real REAL
                        , _double DOUBLE
                        , _short_decimal decimal(8,2)
                        , _long_decimal decimal(38,19)
                        , _boolean BOOLEAN
                        , _timestamp TIMESTAMP
                        , _timestamptz timestamp(6) with time zone
                        , _date DATE
                        , _binary VARBINARY
                        , _uuid UUID
                        ) WITH (format = '%s')""".formatted(trinoTableName, storageFormat));

                env.executeTrinoUpdate(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            case CREATE_TABLE_AS_SELECT:
                env.executeTrinoUpdate(format("CREATE TABLE %s AS %s", trinoTableName, namedValues));
                break;

            case CREATE_TABLE_WITH_NO_DATA_AND_INSERT:
                env.executeTrinoUpdate(format("CREATE TABLE %s AS %s WITH NO DATA", trinoTableName, namedValues));
                env.executeTrinoUpdate(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            default:
                throw new UnsupportedOperationException("Unsupported create mode: " + createMode);
        }

        io.trino.testing.containers.environment.Row expectedRow = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                new BigDecimal("123456.78"),
                new BigDecimal("1234567890123456789.0123456789012345678"),
                true,
                "2020-06-28 14:16:00.123456",
                "2021-08-03 06:32:21.123456 UTC", // Iceberg's timestamptz stores point in time, without zone
                "1950-06-28",
                new byte[] {0, 1, 2, -16, -2, -1},
                "406caec7-68b9-4778-81b2-a12ece70c8b1");
        assertThat(env.executeTrino(
                """
                 SELECT\s
                   _string
                 , _bigint
                 , _integer
                 , _real
                 , _double
                 , _short_decimal
                 , _long_decimal
                 , _boolean
                 , CAST(_timestamp AS varchar)
                 , CAST(_timestamptz AS varchar)
                 , CAST(_date AS varchar)
                 , _binary
                 , _uuid
                  FROM %s""".formatted(trinoTableName)))
                .containsOnly(expectedRow);

        assertThat(env.executeSpark(
                """
                 SELECT\s
                   _string
                 , _bigint
                 , _integer
                 , _real
                 , _double
                 , _short_decimal
                 , _long_decimal
                 , _boolean
                 , CAST(_timestamp AS string)
                 , CAST(_timestamptz AS string) || ' UTC'
                 , CAST(_date AS string)
                 , _binary
                 , _uuid
                  FROM %s""".formatted(sparkTableName)))
                .containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    static Stream<Arguments> testSparkReadingTrinoDataDataProvider()
    {
        return Stream.of(storageFormats().toArray(Object[][]::new))
                .map(array -> getOnlyElement(asList(array)))
                .flatMap(storageFormat -> Stream.of(
                        Arguments.of(storageFormat, CREATE_TABLE_AND_INSERT),
                        Arguments.of(storageFormat, CREATE_TABLE_AS_SELECT),
                        Arguments.of(storageFormat, CREATE_TABLE_WITH_NO_DATA_AND_INSERT)));
    }

    // HMS-only test that covers all format versions (v1, v2, v3)
    // Non-HMS catalogs (Nessie, REST, JDBC) don't fully support v1 and v3 yet
    @ParameterizedTest
    @MethodSource("testSparkReadingTrinoDataWithVersionsDataProvider")
    void testSparkReadingTrinoDataWithVersions(StorageFormat storageFormat, CreateMode createMode, int specVersion, SparkIcebergEnvironment env)
    {
        if (specVersion == 3) {
            assumeTrinoSupportsFormatVersion3(env);
        }

        String baseTableName = toLowerCase("test_spark_reading_primitive_types_" + storageFormat + "_" + createMode + "_v" + specVersion);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        String namedValues = "SELECT " +
                "  VARCHAR 'a_string' _string " +
                ", 1000000000000000 _bigint " +
                ", 1000000000 _integer " +
                ", REAL '10000000.123' _real " +
                ", DOUBLE '100000000000.123' _double " +
                ", DECIMAL '123456.78' _short_decimal " +
                ", DECIMAL '1234567890123456789.0123456789012345678' _long_decimal " +
                ", true _boolean " +
                ", TIMESTAMP '2020-06-28 14:16:00.123456' _timestamp " +
                ", TIMESTAMP '2021-08-03 08:32:21.123456 Europe/Warsaw' _timestamptz " +
                ", DATE '1950-06-28' _date " +
                ", X'000102f0feff' _binary " +
                ", UUID '406caec7-68b9-4778-81b2-a12ece70c8b1' _uuid " +
                "";

        switch (createMode) {
            case CREATE_TABLE_AND_INSERT:
                env.executeTrinoUpdate(format(
                        "CREATE TABLE %s (" +
                                "  _string VARCHAR" +
                                ", _bigint BIGINT" +
                                ", _integer INTEGER" +
                                ", _real REAL" +
                                ", _double DOUBLE" +
                                ", _short_decimal decimal(8,2)" +
                                ", _long_decimal decimal(38,19)" +
                                ", _boolean BOOLEAN" +
                                ", _timestamp TIMESTAMP" +
                                ", _timestamptz timestamp(6) with time zone" +
                                ", _date DATE" +
                                ", _binary VARBINARY" +
                                ", _uuid UUID" +
                                ") WITH (format = '%s', format_version = %d)",
                        trinoTableName,
                        storageFormat,
                        specVersion));

                env.executeTrinoUpdate(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            case CREATE_TABLE_AS_SELECT:
                env.executeTrinoUpdate(format("CREATE TABLE %s WITH (format_version = %d) AS %s", trinoTableName, specVersion, namedValues));
                break;

            case CREATE_TABLE_WITH_NO_DATA_AND_INSERT:
                env.executeTrinoUpdate(format("CREATE TABLE %s WITH (format_version = %d) AS %s WITH NO DATA", trinoTableName, specVersion, namedValues));
                env.executeTrinoUpdate(format("INSERT INTO %s %s", trinoTableName, namedValues));
                break;

            default:
                throw new UnsupportedOperationException("Unsupported create mode: " + createMode);
        }

        io.trino.testing.containers.environment.Row expectedRow = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                new BigDecimal("123456.78"),
                new BigDecimal("1234567890123456789.0123456789012345678"),
                true,
                "2020-06-28 14:16:00.123456",
                "2021-08-03 06:32:21.123456 UTC", // Iceberg's timestamptz stores point in time, without zone
                "1950-06-28",
                new byte[] {00, 01, 02, -16, -2, -1},
                "406caec7-68b9-4778-81b2-a12ece70c8b1");
        assertThat(env.executeTrino(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        ", CAST(_timestamp AS varchar)" +
                        ", CAST(_timestamptz AS varchar)" +
                        ", CAST(_date AS varchar)" +
                        ", _binary" +
                        ", _uuid" +
                        " FROM " + trinoTableName))
                .containsOnly(expectedRow);

        assertThat(env.executeSpark(
                "SELECT " +
                        "  _string" +
                        ", _bigint" +
                        ", _integer" +
                        ", _real" +
                        ", _double" +
                        ", _short_decimal" +
                        ", _long_decimal" +
                        ", _boolean" +
                        ", CAST(_timestamp AS string)" +
                        ", CAST(_timestamptz AS string) || ' UTC'" + // Iceberg timestamptz is mapped to Spark timestamp and gets represented without time zone
                        ", CAST(_date AS string)" +
                        ", _binary" +
                        ", _uuid" +
                        " FROM " + sparkTableName))
                .containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    static Stream<Arguments> testSparkReadingTrinoDataWithVersionsDataProvider()
    {
        return Stream.of(storageFormats().toArray(Object[][]::new))
                .map(array -> getOnlyElement(asList(array)))
                .flatMap(storageFormat -> Stream.of(1, 2, 3)
                        .flatMap(specVersion -> Stream.of(
                                Arguments.of(storageFormat, CREATE_TABLE_AND_INSERT, specVersion),
                                Arguments.of(storageFormat, CREATE_TABLE_AS_SELECT, specVersion),
                                Arguments.of(storageFormat, CREATE_TABLE_WITH_NO_DATA_AND_INSERT, specVersion))));
    }

    @ParameterizedTest
    @MethodSource("specVersions")
    void testSparkCreatesTrinoDrops(int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_creates_trino_drops";
        env.executeSparkUpdate(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(baseTableName), specVersion));
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName(baseTableName));
    }

    @Test
    void testTrinoCreatesSparkDrops(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_trino_creates_spark_drops";
        env.executeTrinoUpdate(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT)", trinoTableName(baseTableName)));
        env.executeSparkUpdate("DROP TABLE " + sparkTableName(baseTableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoPartitionedTable(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (_string VARCHAR, _varbinary VARBINARY, _bigint BIGINT) WITH (partitioning = ARRAY['_string', '_varbinary'], format = '%s')", trinoTableName, storageFormat));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", trinoTableName));

        io.trino.testing.containers.environment.Row row1 = row("b", new byte[] {15, -15, 2, -16, -2, -2}, 1002);
        String selectByString = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(row1);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(row1);

        io.trino.testing.containers.environment.Row row2 = row("a", new byte[] {15, -15, 2, -16, -2, -1}, 1001);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102f0feff'";
        assertThat(env.executeTrino(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(env.executeSpark(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadsSparkPartitionedTable(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reads_spark_partitioned_table_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);

        env.executeSparkUpdate(format(
                "CREATE TABLE %s (_string STRING, _varbinary BINARY, _bigint BIGINT) USING ICEBERG PARTITIONED BY (_string, _varbinary) TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));
        env.executeSparkUpdate(format("INSERT INTO %s VALUES ('a', X'0ff102f0feff', 1001), ('b', X'0ff102f0fefe', 1002), ('c', X'0ff102fdfeff', 1003)", sparkTableName));

        io.trino.testing.containers.environment.Row row1 = row("a", new byte[] {15, -15, 2, -16, -2, -1}, 1001);
        String select = "SELECT * FROM %s WHERE _string = 'a'";
        assertThat(env.executeSpark(format(select, sparkTableName)))
                .containsOnly(row1);
        assertThat(env.executeTrino(format(select, trinoTableName)))
                .containsOnly(row1);

        io.trino.testing.containers.environment.Row row2 = row("c", new byte[] {15, -15, 2, -3, -2, -1}, 1003);
        String selectByVarbinary = "SELECT * FROM %s WHERE _varbinary = X'0ff102fdfeff'";
        assertThat(env.executeTrino(format(selectByVarbinary, trinoTableName)))
                .containsOnly(row2);
        assertThat(env.executeSpark(format(selectByVarbinary, sparkTableName)))
                .containsOnly(row2);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoNestedPartitionedTable(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_nested_partitioned_table_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _struct ROW(_field INT, _another_field VARCHAR))" +
                        " WITH (partitioning = ARRAY['\"_struct._field\"'], format = '%s')",
                trinoTableName,
                storageFormat));
        env.executeTrinoUpdate(format(
                "INSERT INTO %s VALUES" +
                        " ('update', 1001, ROW(1, 'x'))," +
                        " ('b', 1002, ROW(2, 'y'))," +
                        " ('c', 1003, ROW(3, 'z'))",
                trinoTableName));

        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET _string = 'a' WHERE _struct._field = 1");
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE _struct._another_field = 'y'");
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " EXECUTE OPTIMIZE");
        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " DROP COLUMN _struct._field"))
                .hasMessageContaining("Cannot drop partition field: _struct._field");

        List<io.trino.testing.containers.environment.Row> expectedRows = ImmutableList.of(
                row("a", 1001, 1, "x"),
                row("c", 1003, 3, "z"));
        String select = "SELECT _string, _bigint, _struct._field, _struct._another_field FROM %s" +
                " WHERE _struct._field = 1 OR _struct._another_field = 'z'";

        assertThat(env.executeTrino(format(select, trinoTableName)))
                .containsOnly(expectedRows);
        assertThat(env.executeSpark(format(select, sparkTableName)))
                .containsOnly(expectedRows);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoReadsSparkNestedPartitionedTable(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reads_spark_nested_partitioned_table_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format(
                "CREATE TABLE %s (_string STRING, _varbinary BINARY, _bigint BIGINT, _struct STRUCT<_field:INT, _another_field:STRING>)" +
                        " USING ICEBERG PARTITIONED BY (_struct._field) TBLPROPERTIES ('write.format.default'='%s', 'format-version' = 2)",
                sparkTableName,
                storageFormat));
        env.executeSparkUpdate(format(
                "INSERT INTO %s VALUES" +
                        " ('update', X'0ff102f0feff', 1001, named_struct('_field', 1, '_another_field', 'x'))," +
                        " ('b', X'0ff102f0fefe', 1002, named_struct('_field', 2, '_another_field', 'y'))," +
                        " ('c', X'0ff102fdfeff', 1003, named_struct('_field', 3, '_another_field', 'z'))",
                sparkTableName));

        env.executeSparkUpdate("UPDATE " + sparkTableName + " SET _string = 'a' WHERE _struct._field = 1");
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE _struct._another_field = 'y'");
        assertThatThrownBy(() -> env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP COLUMN _struct._field"))
                .isInstanceOf(RuntimeException.class);

        io.trino.testing.containers.environment.Row[] expectedRows = {
                row("a", new byte[] {15, -15, 2, -16, -2, -1}, 1001, 1, "x"),
                row("c", new byte[] {15, -15, 2, -3, -2, -1}, 1003, 3, "z")
        };
        String select = "SELECT _string, _varbinary, _bigint, _struct._field, _struct._another_field FROM %s" +
                " WHERE _struct._field = 1 OR _struct._another_field = 'z'";

        assertThat(env.executeTrino(format(select, trinoTableName)))
                .containsOnly(expectedRows);
        assertThat(env.executeSpark(format(select, sparkTableName)))
                .containsOnly(expectedRows);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoNestedPartitionedTableWithOneFieldStruct(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_nested_partitioned_table_with_one_field_struct_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _struct ROW(_field BIGINT))" +
                        " WITH (partitioning = ARRAY['\"_struct._field\"'], format = '%s')",
                trinoTableName,
                storageFormat));
        env.executeTrinoUpdate(format(
                "INSERT INTO %s VALUES" +
                        " ('a', 1001, ROW(1))," +
                        " ('b', 1002, ROW(2))," +
                        " ('c', 1003, ROW(3))",
                trinoTableName));

        io.trino.testing.containers.environment.Row expectedRow = row("a", 1001, 1);
        String select = "SELECT _string, _bigint, _struct._field FROM %s WHERE _string = 'a'";

        assertThat(env.executeTrino(format(select, trinoTableName)))
                .containsOnly(expectedRow);

        if (storageFormat == StorageFormat.ORC) {
            // Open iceberg issue https://github.com/apache/iceberg/issues/3139 to read ORC table with nested partition column
            assertThatThrownBy(() -> env.executeSpark(format(select, sparkTableName)))
                    .hasStackTraceContaining("java.lang.IndexOutOfBoundsException: Index 2 out of bounds for length 2");
        }
        else {
            assertThat(env.executeSpark(format(select, sparkTableName)))
                    .containsOnly(expectedRow);
        }

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoReadsSparkNestedPartitionedTableWithOneFieldStruct(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reads_spark_nested_partitioned_table_with_one_field_struct_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format(
                "CREATE TABLE %s (_string STRING, _bigint BIGINT, _struct STRUCT<_field:STRING>)" +
                        " USING ICEBERG PARTITIONED BY (_struct._field) TBLPROPERTIES ('write.format.default'='%s', 'format-version' = 2)",
                sparkTableName,
                storageFormat));
        env.executeSparkUpdate(format(
                "INSERT INTO %s VALUES" +
                        " ('a', 1001, named_struct('_field', 'field1'))," +
                        " ('b', 1002, named_struct('_field', 'field2'))," +
                        " ('c', 1003, named_struct('_field', 'field3'))",
                sparkTableName));

        io.trino.testing.containers.environment.Row expectedRow = row("a", 1001, "field1");
        String selectNested = "SELECT _string, _bigint, _struct._field FROM %s WHERE _struct._field = 'field1'";

        assertThat(env.executeTrino(format(selectNested, trinoTableName)))
                .containsOnly(expectedRow);

        if (storageFormat == StorageFormat.ORC) {
            // Open iceberg issue https://github.com/apache/iceberg/issues/3139 to read ORC table with nested partition column
            assertThatThrownBy(() -> env.executeSpark(format(selectNested, sparkTableName)))
                    .hasStackTraceContaining("java.lang.IndexOutOfBoundsException: Index 2 out of bounds for length 2");
        }
        else {
            assertThat(env.executeSpark(format(selectNested, sparkTableName)))
                    .containsOnly(expectedRow);
        }

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoPartitionedByRealWithNaN(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        testTrinoPartitionedByNaN("REAL", storageFormat, Float.NaN, env);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoPartitionedByDoubleWithNaN(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        testTrinoPartitionedByNaN("DOUBLE", storageFormat, Double.NaN, env);
    }

    private void testTrinoPartitionedByNaN(String typeName, StorageFormat storageFormat, Object expectedValue, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_trino_partitioned_by_nan_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "', partitioning = ARRAY['col'])" +
                "AS SELECT " + typeName + " 'NaN' AS col");

        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(expectedValue));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(expectedValue));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkPartitionedByRealWithNaN(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        testSparkPartitionedByNaN("FLOAT", storageFormat, Float.NaN, env);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkPartitionedByDoubleWithNaN(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        testSparkPartitionedByNaN("DOUBLE", storageFormat, Double.NaN, env);
    }

    private void testSparkPartitionedByNaN(String typeName, StorageFormat storageFormat, Object expectedValue, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_partitioned_by_nan_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " " +
                "PARTITIONED BY (col) " +
                "TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT CAST('NaN' AS " + typeName + ") AS col");

        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(expectedValue));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(expectedValue));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadingCompositeSparkData(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reading_spark_composites_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("" +
                        "CREATE TABLE %s (" +
                        "  doc_id string,\n" +
                        "  info MAP<STRING, INT>,\n" +
                        "  pets ARRAY<STRING>,\n" +
                        "  user_info STRUCT<name:STRING, surname:STRING, age:INT, gender:STRING>)" +
                        "  USING ICEBERG" +
                        " TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName, storageFormat, specVersion));

        env.executeSparkUpdate(format(
                "INSERT INTO TABLE %s SELECT 'Doc213', map('age', 28, 'children', 3), array('Dog', 'Cat', 'Pig'), \n" +
                        "named_struct('name', 'Santa', 'surname', 'Claus','age', 1000,'gender', 'MALE')",
                sparkTableName));

        assertThat(env.executeTrino("SELECT doc_id, info['age'], pets[2], user_info.surname FROM " + trinoTableName))
                .containsOnly(row("Doc213", 28, "Cat", "Claus"));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadingCompositeTrinoData(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reading_trino_composites_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (" +
                        "  doc_id VARCHAR,\n" +
                        "  info MAP(VARCHAR, INTEGER),\n" +
                        "  pets ARRAY(VARCHAR),\n" +
                        "  user_info ROW(name VARCHAR, surname VARCHAR, age INTEGER, gender VARCHAR)) " +
                        "  WITH (format = '%s')",
                trinoTableName,
                storageFormat));

        env.executeTrinoUpdate(format(
                "INSERT INTO %s VALUES('Doc213', MAP(ARRAY['age', 'children'], ARRAY[28, 3]), ARRAY['Dog', 'Cat', 'Pig'], ROW('Santa', 'Claus', 1000, 'MALE'))",
                trinoTableName));

        assertThat(env.executeSpark("SELECT doc_id, info['age'], pets[1], user_info.surname FROM " + sparkTableName))
                .containsOnly(row("Doc213", 28, "Cat", "Claus"));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadingSparkIcebergTablePropertiesData(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reading_spark_iceberg_table_properties_" + storageFormat);
        String propertiesTableName = "\"" + baseTableName + "$properties\"";
        String sparkTableName = sparkTableName(baseTableName);
        String trinoPropertiesTableName = trinoTableName(propertiesTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
        env.executeSparkUpdate(format(
                "CREATE TABLE %s (\n" +
                        " doc_id STRING)\n" +
                        " USING ICEBERG TBLPROPERTIES (" +
                        " 'write.format.default'='%s'," +
                        " 'write.object-storage.enabled'=true," +
                        " 'write.data.path'='local:///write-data-path'," +
                        " 'format-version' = %s," +
                        " 'custom.table-property' = 'my_custom_value')",
                sparkTableName,
                storageFormat.toString(),
                specVersion));

        assertThat(env.executeTrino("SELECT key, value FROM " + trinoPropertiesTableName))
                // Use contains method because the result may contain format-specific properties
                .contains(
                        row("custom.table-property", "my_custom_value"),
                        row("write.format.default", storageFormat.name()),
                        row("write.object-storage.enabled", "true"),
                        row("write.data.path", "local:///write-data-path"),
                        row("owner", "hive"));
        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
    }

    @Test
    void testSparkReadingTrinoIcebergTablePropertiesData(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_reading_trino_iceberg_table_properties" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(
                "CREATE TABLE " + trinoTableName + " (doc_id VARCHAR)\n" +
                        " WITH (" +
                        " object_store_layout_enabled = true," +
                        " data_location = 'local:///write-data-path'," +
                        " extra_properties = MAP(ARRAY['custom.table-property'], ARRAY['my_custom_value'])" +
                        " )");

        assertThat(env.executeSpark("SHOW TBLPROPERTIES " + sparkTableName))
                .contains(
                        row("custom.table-property", "my_custom_value"),
                        row("write.object-storage.enabled", "true"),
                        row("write.data.path", "local:///write-data-path"));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadingNestedSparkData(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_reading_nested_spark_data_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format(
                "CREATE TABLE %s (\n" +
                        "  doc_id STRING\n" +
                        ", nested_map MAP<STRING, ARRAY<STRUCT<sname: STRING, snumber: INT>>>\n" +
                        ", nested_array ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>\n" +
                        ", nested_struct STRUCT<name:STRING, complicated: ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>>)\n" +
                        " USING ICEBERG TBLPROPERTIES ('write.format.default'='%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));

        env.executeSparkUpdate(format(
                "INSERT INTO TABLE %s SELECT" +
                        "  'Doc213'" +
                        ", map('s1', array(named_struct('sname', 'ASName1', 'snumber', 201), named_struct('sname', 'ASName2', 'snumber', 202)))" +
                        ", array(map('m1', array(named_struct('mname', 'MAS1Name1', 'mnumber', 301), named_struct('mname', 'MAS1Name2', 'mnumber', 302)))" +
                        "       ,map('m2', array(named_struct('mname', 'MAS2Name1', 'mnumber', 401), named_struct('mname', 'MAS2Name2', 'mnumber', 402))))" +
                        ", named_struct('name', 'S1'," +
                        "               'complicated', array(map('m1', array(named_struct('mname', 'SAMA1Name1', 'mnumber', 301), named_struct('mname', 'SAMA1Name2', 'mnumber', 302)))" +
                        "                                   ,map('m2', array(named_struct('mname', 'SAMA2Name1', 'mnumber', 401), named_struct('mname', 'SAMA2Name2', 'mnumber', 402)))))",
                sparkTableName));

        io.trino.testing.containers.environment.Row expectedRow = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        assertThat(env.executeSpark(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][1].sname" +
                        ", nested_map['s1'][0].snumber" +
                        ", nested_array[1]['m2'][0].mname" +
                        ", nested_array[0]['m1'][1].mnumber" +
                        ", nested_struct.complicated[0]['m1'][0].mname" +
                        ", nested_struct.complicated[1]['m2'][1].mnumber" +
                        "  FROM " + sparkTableName))
                .containsOnly(expectedRow);

        assertThat(env.executeTrino("SELECT" +
                "  doc_id" +
                ", nested_map['s1'][2].sname" +
                ", nested_map['s1'][1].snumber" +
                ", nested_array[2]['m2'][1].mname" +
                ", nested_array[1]['m1'][2].mnumber" +
                ", nested_struct.complicated[1]['m1'][1].mname" +
                ", nested_struct.complicated[2]['m2'][2].mnumber" +
                "  FROM " + trinoTableName))
                .containsOnly(expectedRow);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadingNestedTrinoData(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reading_nested_trino_data_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (\n" +
                        "  doc_id VARCHAR\n" +
                        ", nested_map MAP(VARCHAR, ARRAY(ROW(sname VARCHAR, snumber INT)))\n" +
                        ", nested_array ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))\n" +
                        ", nested_struct ROW(name VARCHAR, complicated ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))))" +
                        "  WITH (format = '%s')",
                trinoTableName,
                storageFormat));

        env.executeTrinoUpdate(format(
                "INSERT INTO %s SELECT" +
                        "  'Doc213'" +
                        ", map(array['s1'], array[array[row('ASName1', 201), row('ASName2', 202)]])" +
                        ", array[map(array['m1'], array[array[row('MAS1Name1', 301), row('MAS1Name2', 302)]])" +
                        "       ,map(array['m2'], array[array[row('MAS2Name1', 401), row('MAS2Name2', 402)]])]" +
                        ", row('S1'" +
                        "      ,array[map(array['m1'], array[array[row('SAMA1Name1', 301), row('SAMA1Name2', 302)]])" +
                        "            ,map(array['m2'], array[array[row('SAMA2Name1', 401), row('SAMA2Name2', 402)]])])",
                trinoTableName));

        io.trino.testing.containers.environment.Row expectedRow = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        assertThat(env.executeTrino(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][2].sname" +
                        ", nested_map['s1'][1].snumber" +
                        ", nested_array[2]['m2'][1].mname" +
                        ", nested_array[1]['m1'][2].mnumber" +
                        ", nested_struct.complicated[1]['m1'][1].mname" +
                        ", nested_struct.complicated[2]['m2'][2].mnumber" +
                        "  FROM " + trinoTableName))
                .containsOnly(expectedRow);

        QueryResult sparkResult = env.executeSpark(
                "SELECT" +
                        "  doc_id" +
                        ", nested_map['s1'][1].sname" +
                        ", nested_map['s1'][0].snumber" +
                        ", nested_array[1]['m2'][0].mname" +
                        ", nested_array[0]['m1'][1].mnumber" +
                        ", nested_struct.complicated[0]['m1'][0].mname" +
                        ", nested_struct.complicated[1]['m2'][1].mnumber" +
                        "  FROM " + sparkTableName);
        assertThat(sparkResult).containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testTrinoWritingDataAfterSpark(SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_write_after_spark");
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " (a INT) USING ICEBERG");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES 1");

        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1), row(2));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    /**
     * @see TestIcebergInsert#testIcebergConcurrentInsert()
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testTrinoSparkConcurrentInsert(SparkIcebergEnvironment env)
            throws Exception
    {
        int insertsPerEngine = 7;

        String baseTableName = "trino_spark_insert_concurrent_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(e varchar, a bigint)");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CyclicBarrier barrier = new CyclicBarrier(2);
            List<io.trino.testing.containers.environment.Row> allInserted = executor.invokeAll(
                            Stream.of(Engine.TRINO, Engine.SPARK)
                                    .map(engine -> (Callable<List<io.trino.testing.containers.environment.Row>>) () -> {
                                        List<io.trino.testing.containers.environment.Row> inserted = new ArrayList<>();
                                        for (int i = 0; i < insertsPerEngine; i++) {
                                            barrier.await(20, SECONDS);
                                            String engineName = engine.name().toLowerCase(ENGLISH);
                                            long value = i;
                                            switch (engine) {
                                                case TRINO:
                                                    try {
                                                        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('%s', %d)", trinoTableName, engineName, value));
                                                    }
                                                    catch (RuntimeException e) {
                                                        // failed to insert
                                                        continue; // next loop iteration
                                                    }
                                                    break;
                                                case SPARK:
                                                    env.executeSparkUpdate(format("INSERT INTO %s VALUES ('%s', %d)", sparkTableName, engineName, value));
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
            assertThat(allInserted).hasSizeBetween(insertsPerEngine, insertsPerEngine * 2);

            // All Spark inserts should succeed (and not be obliterated)
            assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE e = 'spark'"))
                    .containsOnly(row(insertsPerEngine));

            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName))
                    .containsOnly(allInserted);

            env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
        }
        finally {
            executor.shutdownNow();
        }
    }

    // Helper enum for concurrent insert test
    enum Engine
    {
        TRINO,
        SPARK
    }

    @ParameterizedTest
    @MethodSource("specVersions")
    void testTrinoShowingSparkCreatedTables(int specVersion, SparkIcebergEnvironment env)
    {
        String sparkTable = "test_table_listing_for_spark";
        String trinoTable = "test_table_listing_for_trino";

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName(sparkTable));
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName(trinoTable));

        env.executeSparkUpdate(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG TBLPROPERTIES('format-version' = %s)", sparkTableName(sparkTable), specVersion));
        env.executeTrinoUpdate(format("CREATE TABLE %s (_integer INTEGER )", trinoTableName(trinoTable)));

        assertThat(env.executeTrino(format("SHOW TABLES FROM %s.%s LIKE '%s'", TRINO_CATALOG, TEST_SCHEMA_NAME, "test_table_listing_for_%")))
                .containsOnly(row(sparkTable), row(trinoTable));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName(sparkTable));
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName(trinoTable));
    }

    // TODO Spark with Iceberg 1.8 returns incorrect results for Parquet bloom filters Trino wrote
    @Disabled("Spark with Iceberg 1.8 returns incorrect results for Parquet bloom filters Trino wrote")
    @Test
    void testSparkReadingTrinoParquetBloomFilters(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_reading_trino_bloom_filters";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString VARCHAR, testDouble DOUBLE, testFloat REAL) ", trinoTableName) +
                        "WITH (" +
                        "format = 'PARQUET'," +
                        "parquet_bloom_filter_columns = ARRAY['testInteger', 'testLong', 'testString', 'testDouble', 'testFloat']" +
                        ")");

        env.executeTrinoUpdate(format(
                "INSERT INTO %s " +
                        "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                        "  (-999999, -999999, 'aaaaaaaaaaa', DOUBLE '-9999999999.99', REAL '-9999999.9999')" +
                        ", (3, 30, 'fdsvxxbv33cb', DOUBLE '97662.2', REAL '98862.2')" +
                        ", (5324, 2466, 'refgfdfrexx', DOUBLE '8796.1', REAL '-65496.1')" +
                        ", (999999, 9999999999999, 'zzzzzzzzzzz', DOUBLE '9999999999.99', REAL '-9999999.9999')" +
                        ", (9444, 4132455, 'ff34322vxff', DOUBLE '32137758.7892', REAL '9978.129887')) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                trinoTableName));

        assertTrinoBloomFilterTableSelectResult(trinoTableName, env);
        assertSparkBloomFilterTableSelectResult(sparkTableName, env);

        env.executeSparkUpdate(format(
                "CREATE OR REPLACE TABLE %s AS " +
                        "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                        "  (-999999, -999999, 'aaaaaaaaaaa', -9999999999.99D, -9999999.9999F)" +
                        ", (3, 30, 'fdsvxxbv33cb', 97662.2D, 98862.2F)" +
                        ", (5324, 2466, 'refgfdfrexx', 8796.1D, -65496.1F)" +
                        ", (999999, 9999999999999, 'zzzzzzzzzzz', 9999999999.99D, -9999999.9999F)" +
                        ", (9444, 4132455, 'ff34322vxff', 32137758.7892D, 9978.129887F)) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                sparkTableName));

        assertTrinoBloomFilterTableSelectResult(trinoTableName, env);
        assertSparkBloomFilterTableSelectResult(sparkTableName, env);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testTrinoReadingSparkParquetBloomFilters(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_reading_trino_bloom_filters";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString STRING, testDouble DOUBLE, testFloat REAL) ", sparkTableName) +
                        "USING ICEBERG " +
                        "TBLPROPERTIES (" +
                        "'write.parquet.bloom-filter-enabled.column.testInteger' = true," +
                        "'write.parquet.bloom-filter-enabled.column.testLong' = true," +
                        "'write.parquet.bloom-filter-enabled.column.testString' = true," +
                        "'write.parquet.bloom-filter-enabled.column.testDouble' = true," +
                        "'write.parquet.bloom-filter-enabled.column.testFloat' = true" +
                        ")");

        env.executeSparkUpdate(format(
                "INSERT INTO %s " +
                        "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                        "  (-999999, -999999, 'aaaaaaaaaaa', -9999999999.99D, -9999999.9999F)" +
                        ", (3, 30, 'fdsvxxbv33cb', 97662.2D, 98862.2F)" +
                        ", (5324, 2466, 'refgfdfrexx', 8796.1D, -65496.1F)" +
                        ", (999999, 9999999999999, 'zzzzzzzzzzz', 9999999999.99D, -9999999.9999F)" +
                        ", (9444, 4132455, 'ff34322vxff', 32137758.7892D, 9978.129887F)) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                sparkTableName));

        assertTrinoBloomFilterTableSelectResult(trinoTableName, env);
        assertSparkBloomFilterTableSelectResult(sparkTableName, env);

        env.executeTrinoUpdate(format(
                "CREATE OR REPLACE TABLE %s AS " +
                        "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                        "  (-999999, -999999, 'aaaaaaaaaaa', DOUBLE '-9999999999.99', REAL '-9999999.9999')" +
                        ", (3, 30, 'fdsvxxbv33cb', DOUBLE '97662.2', REAL '98862.2')" +
                        ", (5324, 2466, 'refgfdfrexx', DOUBLE '8796.1', REAL '-65496.1')" +
                        ", (999999, 9999999999999, 'zzzzzzzzzzz', DOUBLE '9999999999.99', REAL '-9999999.9999')" +
                        ", (9444, 4132455, 'ff34322vxff', DOUBLE '32137758.7892', REAL '9978.129887')) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                trinoTableName));

        assertTrinoBloomFilterTableSelectResult(trinoTableName, env);
        assertSparkBloomFilterTableSelectResult(sparkTableName, env);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    private void assertTrinoBloomFilterTableSelectResult(String trinoTable, SparkIcebergEnvironment env)
    {
        assertThat(env.executeTrino("SELECT COUNT(*) FROM " + trinoTable + " WHERE testInteger IN (9444, -88777, 6711111)")).containsOnly(List.of(row(1)));
        assertThat(env.executeTrino("SELECT COUNT(*) FROM " + trinoTable + " WHERE testLong IN (4132455, 321324, 312321321322)")).containsOnly(List.of(row(1)));
        assertThat(env.executeTrino("SELECT COUNT(*) FROM " + trinoTable + " WHERE testString IN ('fdsvxxbv33cb', 'cxxx322', 'cxxx323')")).containsOnly(List.of(row(1)));
        assertThat(env.executeTrino("SELECT COUNT(*) FROM " + trinoTable + " WHERE testDouble IN (DOUBLE '97662.2', DOUBLE '-97221.2', DOUBLE '-88777.22233')")).containsOnly(List.of(row(1)));
        assertThat(env.executeTrino("SELECT COUNT(*) FROM " + trinoTable + " WHERE testFloat IN (REAL '-65496.1', REAL '98211862.2', REAL '6761111555.1222')")).containsOnly(List.of(row(1)));
    }

    private void assertSparkBloomFilterTableSelectResult(String sparkTable, SparkIcebergEnvironment env)
    {
        assertThat(env.executeSpark("SELECT COUNT(*) FROM " + sparkTable + " WHERE testInteger IN (9444, -88777, 6711111)")).containsOnly(List.of(row(1)));
        assertThat(env.executeSpark("SELECT COUNT(*) FROM " + sparkTable + " WHERE testLong IN (4132455, 321324, 312321321322)")).containsOnly(List.of(row(1)));
        assertThat(env.executeSpark("SELECT COUNT(*) FROM " + sparkTable + " WHERE testString IN ('fdsvxxbv33cb', 'cxxx322', 'cxxx323')")).containsOnly(List.of(row(1)));
        assertThat(env.executeSpark("SELECT COUNT(*) FROM " + sparkTable + " WHERE testDouble IN (97662.2D, -97221.2D, -88777.22233D)")).containsOnly(List.of(row(1)));
        assertThat(env.executeSpark("SELECT COUNT(*) FROM " + sparkTable + " WHERE testFloat IN (-65496.1F, 98211862.2F, 6761111555.1222F)")).containsOnly(List.of(row(1)));
    }

    @Test
    void testTrinoAnalyze(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_trino_analyze_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " AS SELECT regionkey, name FROM tpch.tiny.region");
        env.executeTrinoUpdate("ANALYZE " + trinoTableName);

        // We're not verifying results of ANALYZE (covered by non-product tests), but we're verifying table is readable.
        List<io.trino.testing.containers.environment.Row> expected = List.of(row(0, "AFRICA"), row(1, "AMERICA"), row(2, "ASIA"), row(3, "EUROPE"), row(4, "MIDDLE EAST"));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testOptimizeManifests(SparkIcebergEnvironment env)
    {
        String tableName = "test_optimize_manifests_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(a INT) USING ICEBERG");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (2)");

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " EXECUTE optimize_manifests");
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName))
                .containsOnly(row(1), row(2));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1), row(2));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testAlterTableExecuteProceduresOnEmptyTable(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_alter_table_execute_procedures_on_empty_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format(
                "CREATE TABLE %s (" +
                        "  _string STRING" +
                        ", _bigint BIGINT" +
                        ", _integer INTEGER" +
                        ") USING ICEBERG",
                sparkTableName));

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " EXECUTE optimize");
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " EXECUTE expire_snapshots(retention_threshold => '7d')");
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " EXECUTE remove_orphan_files(retention_threshold => '7d')");

        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).hasNoRows();
    }

    @Test
    void testAddNotNullColumn(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_add_not_null_column_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " AS SELECT 1 col");
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1));

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ADD COLUMN new_col INT NOT NULL"))
                .hasMessageContaining("This connector does not support adding not null columns");
        assertThatThrownBy(() -> env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD COLUMN new_col INT NOT NULL"))
                .hasStackTraceContaining("Unsupported table change: Incompatible change: cannot add required column");

        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1));
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testAddNestedField(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_add_nested_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " AS SELECT CAST(row(1, row(10)) AS row(a integer, b row(x integer))) AS col");

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ADD COLUMN col.c integer");
        assertThat(env.executeTrino("SELECT col.a, col.b.x, col.c FROM " + trinoTableName)).containsOnly(row(1, 10, null));
        assertThat(env.executeSpark("SELECT col.a, col.b.x, col.c FROM " + sparkTableName)).containsOnly(row(1, 10, null));

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ADD COLUMN col.b.y integer");
        assertThat(env.executeTrino("SELECT col.a, col.b.x, col.b.y, col.c FROM " + trinoTableName)).containsOnly(row(1, 10, null, null));
        assertThat(env.executeSpark("SELECT col.a, col.b.x, col.b.y, col.c FROM " + sparkTableName)).containsOnly(row(1, 10, null, null));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testDropNestedField(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_drop_nested_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " AS SELECT CAST(row(1, 2, row(10, 20)) AS row(a integer, b integer, c row(x integer, y integer))) AS col");

        // Drop a nested field
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " DROP COLUMN col.b");
        assertThat(env.executeTrino("SELECT col.a, col.c.x, col.c.y FROM " + trinoTableName)).containsOnly(row(1, 10, 20));
        assertThat(env.executeSpark("SELECT col.a, col.c.x, col.c.y FROM " + sparkTableName)).containsOnly(row(1, 10, 20));

        // Drop a row type having fields
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " DROP COLUMN col.c");
        assertThat(env.executeTrino("SELECT col.a FROM " + trinoTableName)).containsOnly(row(1));
        assertThat(env.executeSpark("SELECT col.a FROM " + sparkTableName)).containsOnly(row(1));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("tableFormatWithDeleteFormat")
    void testTrinoReadsSparkRowLevelDeletes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat, SparkIcebergEnvironment env)
    {
        String tableName = toLowerCase(format("test_trino_reads_spark_row_level_deletes_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(a INT, b INT) " +
                "USING ICEBERG PARTITIONED BY (b) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        env.executeSparkUpdate("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + tableName + "', options => map('min-input-files','1'))");
        // Delete one row in a file
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE a = 13");
        // Delete an entire partition
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE b = 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete to a file that already has deleted rows
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoRowLevelDeletes(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String tableName = toLowerCase(format("test_spark_reads_trino_row_level_deletes_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(a INT, b INT) WITH(partitioning = ARRAY['b'], format_version = 2, format = '" + storageFormat.name() + "')");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Delete one row in a file
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE a = 13");
        // Delete an entire partition
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE b = 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete to a file that already has deleted rows
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    // Test that Spark can read deletion vectors (DVs) written by Trino in format version 3
    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoV3DeletionVectors(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        assumeTrinoSupportsFormatVersion3(env);

        String tableName = toLowerCase(format("test_spark_reads_trino_v3_dv_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(a INT, b INT) WITH(partitioning = ARRAY['b'], format_version = 3, format = '" + storageFormat.name() + "')");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Delete rows from multiple partitions in a single statement - this creates deletion vectors in v3
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE a IN (3, 13)");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1, 2), row(2, 2), row(11, 12), row(12, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete entire partition
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE b = 2");

        expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete additional row from file that already has a DV
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    // Test that Trino can read deletion vectors (DVs) written by Spark in format version 3
    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoReadsSparkV3DeletionVectors(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        assumeTrinoSupportsFormatVersion3(env);

        String tableName = toLowerCase(format("test_trino_reads_spark_v3_dv_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(a INT, b INT) " +
                "USING ICEBERG PARTITIONED BY (b) " +
                "TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read', " +
                "'write.format.default'='" + storageFormat.name() + "')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        // Delete one row - this creates a deletion vector in v3
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE a = 13");
        // Delete entire partition
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE b = 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(11, 12), row(12, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Delete additional row from file that already has a DV
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE a = 12");
        expected = ImmutableList.of(row(11, 12));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testIdBasedFieldMapping(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_schema_evolution_for_nested_fields_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
        env.executeSparkUpdate(format(
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

        env.executeSparkUpdate(format(
                "INSERT INTO TABLE %s SELECT " +
                        "1, " + // remove_col
                        "2, " + // rename_col
                        "3, " + // keep_col
                        "4, " + // drop_and_add_col,
                        "5, " + // CaseSensitiveCol,
                        " named_struct('removed', 10, 'rename', 11, 'keep', 12, 'drop_and_add', 13, 'CaseSensitive', 14), " // a_struct
                        + "1001", // a_partition
                sparkTableName));

        env.executeSparkUpdate(format("ALTER TABLE %s DROP COLUMN remove_col", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s RENAME COLUMN rename_col TO quite_renamed_col", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s DROP COLUMN drop_and_add_col", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD COLUMN drop_and_add_col BIGINT", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD COLUMN add_col BIGINT", sparkTableName));

        env.executeSparkUpdate(format("ALTER TABLE %s DROP COLUMN a_struct.removed", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s RENAME COLUMN a_struct.rename TO renamed", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s DROP COLUMN a_struct.drop_and_add", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD COLUMN a_struct.drop_and_add BIGINT", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD COLUMN a_struct.added BIGINT", sparkTableName));

        assertThat(env.executeTrino("DESCRIBE " + trinoTableName).project(1, 2))
                .containsOnly(
                        row("quite_renamed_col", "bigint"),
                        row("keep_col", "bigint"),
                        row("drop_and_add_col", "bigint"),
                        row("add_col", "bigint"),
                        row("casesensitivecol", "bigint"),
                        row("a_struct", "row(\"renamed\" bigint, \"keep\" bigint, \"CaseSensitive\" bigint, \"drop_and_add\" bigint, \"added\" bigint)"),
                        row("a_partition", "bigint"));

        assertThat(env.executeTrino(format("SELECT quite_renamed_col, keep_col, drop_and_add_col, add_col, casesensitivecol, a_struct, a_partition FROM %s", trinoTableName)))
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
        // make sure predicates are also ID based
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE drop_and_add_col IS NULL", trinoTableName))).containsOnly(row(3L));

        // smoke test for dereference
        assertThat(env.executeTrino(format("SELECT a_struct.renamed FROM %s", trinoTableName))).containsOnly(row(11L));
        assertThat(env.executeTrino(format("SELECT a_struct.keep FROM %s", trinoTableName))).containsOnly(row(12L));
        assertThat(env.executeTrino(format("SELECT a_struct.casesensitive FROM %s", trinoTableName))).containsOnly(row(14L));
        assertThat(env.executeTrino(format("SELECT a_struct.drop_and_add FROM %s", trinoTableName))).containsOnly(row((Object) null));
        assertThat(env.executeTrino(format("SELECT a_struct.added FROM %s", trinoTableName))).containsOnly(row((Object) null));

        // smoke test for dereference in a predicate
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE a_struct.renamed = 11", trinoTableName))).containsOnly(row(3L));
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE a_struct.keep = 12", trinoTableName))).containsOnly(row(3L));
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE a_struct.casesensitive = 14", trinoTableName))).containsOnly(row(3L));
        // make sure predicates are also ID based
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE a_struct.drop_and_add IS NULL", trinoTableName))).containsOnly(row(3L));
        assertThat(env.executeTrino(format("SELECT keep_col FROM %s WHERE a_struct.added IS NULL", trinoTableName))).containsOnly(row(3L));

        env.executeSparkUpdate(format(
                "INSERT INTO TABLE %s SELECT " +
                        "12, " + // quite_renamed_col
                        "13, " + // keep_col
                        "15, " + // CaseSensitiveCol,
                        "named_struct('renamed', 111, 'keep', 112, 'CaseSensitive', 113, 'drop_and_add', 114, 'added', 115), " + // a_struct
                        "1001, " + // a_partition,
                        "14, " + // drop_and_add_col
                        "15", // add_col
                sparkTableName));

        assertThat(env.executeTrino("SELECT DISTINCT a_struct.renamed, a_struct.added, a_struct.keep FROM " + trinoTableName)).containsOnly(
                row(11L, null, 12L),
                row(111L, 115L, 112L));
        assertThat(env.executeTrino("SELECT DISTINCT a_struct.renamed, a_struct.keep FROM " + trinoTableName + " WHERE a_struct.added IS NULL")).containsOnly(
                row(11L, 12L));

        assertThat(env.executeTrino("SELECT a_struct FROM " + trinoTableName + " WHERE a_struct.added IS NOT NULL")).containsOnly(
                row(rowBuilder()
                        .addField("renamed", 111L)
                        .addField("keep", 112L)
                        .addField("CaseSensitive", 113L)
                        .addField("drop_and_add", 114L)
                        .addField("added", 115L)
                        .build()));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testReadAfterPartitionEvolution(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_read_after_partition_evolution_" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
        env.executeSparkUpdate(format(
                "CREATE TABLE %s (" +
                        "int_col BIGINT, " +
                        "struct_col STRUCT<field_one: INT, field_two: INT>, " +
                        "timestamp_col TIMESTAMP) "
                        + " USING ICEBERG"
                        + " TBLPROPERTIES ('write.format.default' = '%s', 'format-version' = %s)",
                sparkTableName,
                storageFormat,
                specVersion));
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, named_struct('field_one', 1, 'field_two', 1), TIMESTAMP '2021-06-28 14:16:00.456')");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, int_col)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (2, named_struct('field_one', 2, 'field_two', 2), TIMESTAMP '2022-06-28 14:16:00.456')");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD struct_col.field_one");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (3, named_struct('field_one', 3, 'field_two', 3), TIMESTAMP '2023-06-28 14:16:00.456')");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD struct_col.field_one");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD struct_col.field_two");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (4, named_struct('field_one', 4, 'field_two', 4), TIMESTAMP '2024-06-28 14:16:00.456')");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD bucket(3, int_col)");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD struct_col.field_two");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD days(timestamp_col)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (5, named_struct('field_one', 5, 'field_two', 5), TIMESTAMP '2025-06-28 14:16:00.456')");

        // The Iceberg documentation states it is not necessary to drop a day transform partition field in order to add an hourly one
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD hours(timestamp_col)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (6, named_struct('field_one', 6, 'field_two', 6), TIMESTAMP '2026-06-28 14:16:00.456')");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD int_col");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (7, named_struct('field_one', 7, 'field_two', 7), TIMESTAMP '2027-06-28 14:16:00.456')");

        assertThat(env.executeTrino("SELECT struct_col.field_two FROM " + trinoTableName))
                .containsOnly(row(1), row(2), row(3), row(4), row(5), row(6), row(7));
        assertThat(env.executeTrino("SELECT CAST(timestamp_col AS TIMESTAMP) FROM " + trinoTableName + " WHERE struct_col.field_two = 2"))
                .containsOnly(row(Timestamp.valueOf("2022-06-28 14:16:00.456")));

        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE int_col = 2"))
                .containsOnly(row(1L));
        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE int_col % 2 = 0"))
                .containsOnly(row(3L));
        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE struct_col.field_one = 2"))
                .containsOnly(row(1L));
        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE struct_col.field_one % 2 = 0"))
                .containsOnly(row(3L));
        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE year(timestamp_col) = 2022"))
                .containsOnly(row(1L));
        assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE year(timestamp_col) % 2 = 0"))
                .containsOnly(row(3L));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testUpdateAfterSchemaEvolution(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_update_after_schema_evolution_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(part_key INT, a INT, b INT, c INT) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2, 3, 4), (11, 12, 13, 14)");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD part_key");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD a");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP COLUMN b");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP COLUMN c");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD COLUMN c INT");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1, 2, null), row(11, 12, null));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Because of the DROP/ADD on column c these two should be no-op updates
        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET c = c + 1");
        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET a = a + 1 WHERE c = 4");
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Check the new data files are using the updated partition scheme
        List<Object> filePaths = env.executeTrino("SELECT DISTINCT file_path FROM " + TRINO_CATALOG + "." + TEST_SCHEMA_NAME + ".\"" + baseTableName + "$files\"").column(1);
        assertThat(filePaths.stream()
                .map(String::valueOf)
                .filter(path -> path.contains("/a=") && !path.contains("/part_key="))
                .count())
                .isEqualTo(2);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testUpdateOnPartitionColumn(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_update_on_partition_column" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(a INT, b STRING) " +
                "USING ICEBERG PARTITIONED BY (a) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 'first'), (1, 'second'), (2, 'third'), (2, 'forth'), (2, 'fifth')");

        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET a = a + 1");
        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(2, "first"), row(2, "second"), row(3, "third"), row(3, "forth"), row(3, "fifth"));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET a = a + (CASE b WHEN 'first' THEN 1 ELSE 0 END)");
        expected = ImmutableList.of(row(3, "first"), row(2, "second"), row(3, "third"), row(3, "forth"), row(3, "fifth"));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        // Test moving rows from one file into different partitions, compact first
        env.executeSparkUpdate("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + baseTableName + "', options => map('min-input-files','1'))");
        env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET a = a + (CASE b WHEN 'forth' THEN -1 ELSE 1 END)");
        expected = ImmutableList.of(row(4, "first"), row(3, "second"), row(4, "third"), row(2, "forth"), row(4, "fifth"));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testPartitionColumnNameConflict(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_conflict_partition" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(ts timestamp, ts_day int) WITH (partitioning = ARRAY['day(ts)'])");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (TIMESTAMP '2021-07-24 03:43:57.987654', 1)");

        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(Timestamp.valueOf("2021-07-24 03:43:57.987654"), 1));
        assertThat(env.executeSpark("SELECT partition['ts_day_2'] FROM " + sparkTableName + ".partitions"))
                .containsOnly(row(Date.valueOf("2021-07-24")));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testTrinoReadsSparkSortOrder(SparkIcebergEnvironment env)
    {
        String sourceTableNameBase = "test_insert_into_sorted_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(sourceTableNameBase);
        String sparkTableName = sparkTableName(sourceTableNameBase);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " (a INT, b INT, c INT) USING ICEBERG");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " WRITE ORDERED BY b, c DESC NULLS LAST");

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + trinoTableName).getOnlyValue())
                .contains("sorted_by = ARRAY['b ASC NULLS FIRST','c DESC NULLS LAST']");

        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (3, 2, 1), (1, 2, 3), (NULL, NULL, NULL)");
        assertThat(env.executeSpark("SELECT _pos, a, b, c FROM " + sparkTableName))
                .contains(
                        row(0, null, null, null),
                        row(1, 1, 2, 3),
                        row(2, 3, 2, 1));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testMetadataCompressionCodecGzip(SparkIcebergEnvironment env)
    {
        // Verify that Trino can read and write to a table created by Spark
        String baseTableName = "test_metadata_compression_codec_gzip" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(col int) USING iceberg TBLPROPERTIES ('write.metadata.compression-codec'='gzip')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1)");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (2)");

        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2));

        // Verify that all metadata files are compressed as Gzip
        HdfsClient hdfsClient = env.createHdfsClient();
        String tableLocation = SparkIcebergEnvironment.stripNamenodeURI(env.getTableLocation(trinoTableName));
        List<String> metadataFiles = Arrays.stream(hdfsClient.listDirectory(tableLocation + "/metadata"))
                .filter(file -> file.endsWith("metadata.json"))
                .collect(toImmutableList());
        assertThat(metadataFiles)
                .isNotEmpty()
                .filteredOn(file -> file.endsWith("gz.metadata.json"))
                .isEqualTo(metadataFiles);

        // Change 'write.metadata.compression-codec' to none and insert and select the table in Trino
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES ('write.metadata.compression-codec'='none')");
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2));

        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (3)");
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1), row(2), row(3));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("specVersions")
    void testCreateAndDropTableWithSameLocationWorksOnSpark(int specVersion, SparkIcebergEnvironment env)
    {
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_create_table_same_location/obj-data";
        String tableSameLocation1 = "test_same_location_spark_1_" + randomNameSuffix();
        String tableSameLocation2 = "test_same_location_spark_2_" + randomNameSuffix();

        env.executeSparkUpdate(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation1), dataPath, specVersion));
        env.executeSparkUpdate(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation2), dataPath, specVersion));

        env.executeSparkUpdate(format("DROP TABLE IF EXISTS %s", sparkTableName(tableSameLocation1)));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName(tableSameLocation2)))).hasNoRows();

        env.executeSparkUpdate(format("DROP TABLE %s", sparkTableName(tableSameLocation2)));
    }

    @ParameterizedTest
    @MethodSource("specVersions")
    void testCreateAndDropTableWithSameLocationFailsOnTrino(int specVersion, SparkIcebergEnvironment env)
    {
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_create_table_same_location/obj-data";
        String tableSameLocation1 = "test_same_location_trino_1_" + randomNameSuffix();
        String tableSameLocation2 = "test_same_location_trino_2_" + randomNameSuffix();

        env.executeSparkUpdate(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation1), dataPath, specVersion));
        env.executeSparkUpdate(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG LOCATION '%s' TBLPROPERTIES('format-version' = %s)",
                sparkTableName(tableSameLocation2), dataPath, specVersion));

        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName(tableSameLocation1)));

        assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM %s", trinoTableName(tableSameLocation2))))
                .hasMessageContaining("Metadata not found in metadata location for table default." + tableSameLocation2);

        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName(tableSameLocation2)));
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoWritingDataWithObjectStorageLocationProvider(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_object_storage_location_provider_" + storageFormat);
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_object_storage_location_provider/obj-data";

        env.executeSparkUpdate(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES (" +
                        "'write.object-storage.enabled'=true," +
                        "'write.object-storage.path'='%s'," +
                        "'write.format.default' = '%s'," +
                        "'format-version' = %s)",
                sparkTableName, dataPath, storageFormat, specVersion));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        io.trino.testing.containers.environment.Row result = row("a_string", 1000000000000000L);
        assertThat(env.executeSpark(format("SELECT _string, _bigint FROM %s", sparkTableName))).containsOnly(result);
        assertThat(env.executeTrino(format("SELECT _string, _bigint FROM %s", trinoTableName))).containsOnly(result);

        QueryResult queryResult = env.executeTrino(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult.getRowsCount()).isEqualTo(1);
        assertThat(queryResult.getColumnCount()).isEqualTo(1);
        assertThat(((String) queryResult.getOnlyValue())).contains(dataPath);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testSparkReadingTrinoObjectStorage(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_object_storage_location_provider_" + storageFormat);
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_trino_object_storage_location_provider/obj-data";

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (" +
                          "object_store_layout_enabled = true," +
                          "data_location = '%s'," +
                          "format = '%s'," +
                          "format_version = %s)",
                trinoTableName,
                dataPath,
                storageFormat,
                specVersion));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        io.trino.testing.containers.environment.Row result = row("a_string", 1000000000000000L);
        assertThat(env.executeSpark("SELECT _string, _bigint FROM " + sparkTableName)).containsOnly(result);
        assertThat(env.executeTrino("SELECT _string, _bigint FROM " + trinoTableName)).containsOnly(result);

        QueryResult queryResult = env.executeTrino(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult.getRowsCount()).isEqualTo(1);
        assertThat(queryResult.getColumnCount()).isEqualTo(1);
        assertThat(((String) queryResult.getOnlyValue())).contains(dataPath);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testOptimizeOnV2IcebergTable(SparkIcebergEnvironment env)
    {
        String tableName = format("test_optimize_on_v2_iceberg_table_%s", randomNameSuffix());
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);
        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(a INT, b INT) " +
                "USING ICEBERG PARTITIONED BY (b) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2), (2, 2), (3, 2), (11, 12), (12, 12), (13, 12)");
        env.executeTrinoUpdate(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));

        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(row(1, 2), row(2, 2), row(3, 2), row(11, 12), row(12, 12), row(13, 12));
    }

    @Test
    void testPartialStats(SparkIcebergEnvironment env)
    {
        String tableName = "test_partial_stats_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT, col2 STRING, col3 BINARY)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2, 'col2Value0', X'000102f0feff')");
        assertThat(env.executeTrino("SHOW STATS FOR " + trinoTableName))
                .containsOnly(
                        row("col0", null, null, 0.0, null, "1", "1"),
                        row("col1", null, null, 0.0, null, "2", "2"),
                        row("col2", 132.0, null, 0.0, null, null, null),
                        row("col3", 62.0, null, 0.0, null, null, null),
                        row(null, null, null, null, 1.0, null, null));

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " SET TBLPROPERTIES (write.metadata.metrics.column.col1='none')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (3, 4, 'col2Value1', X'000102f0feee')");
        assertThat(env.executeTrino("SHOW STATS FOR " + trinoTableName))
                .containsOnly(
                        row("col0", null, null, 0.0, null, "1", "3"),
                        row("col1", null, null, 0.0, null, null, null),
                        row("col2", 264.0, null, 0.0, null, null, null),
                        row("col3", 124.0, null, 0.0, null, null, null),
                        row(null, null, null, null, 2.0, null, null));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testStatsAfterAddingPartitionField(SparkIcebergEnvironment env)
    {
        String tableName = "test_stats_after_adding_partition_field_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 2)");
        assertThat(env.executeTrino("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "1"), row("col1", null, null, 0.0, null, "2", "2"), row(null, null, null, null, 1.0, null, null));

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col1");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (3, 4)");
        assertThat(env.executeTrino("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "3"), row("col1", null, null, 0.0, null, "2", "4"), row(null, null, null, null, 2.0, null, null));

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col1");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, col1)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (5, 6)");
        assertThat(env.executeTrino("SHOW STATS FOR " + trinoTableName))
                .containsOnly(row("col0", null, null, 0.0, null, "1", "5"), row("col1", null, null, 0.0, null, "2", "6"), row(null, null, null, null, 3.0, null, null));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testMissingMetrics(SparkIcebergEnvironment env)
    {
        String tableName = "test_missing_metrics_" + randomNameSuffix();
        String sparkTableName = sparkTableName(tableName);
        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " (name STRING, country STRING) USING ICEBERG " +
                "PARTITIONED BY (country) TBLPROPERTIES ('write.metadata.metrics.default'='none')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES ('Christoph', 'AT'), (NULL, 'RO')");
        assertThat(env.executeTrino(format("SELECT count(*) FROM %s.%s.\"%s$partitions\" WHERE data.name IS NOT NULL", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName)))
                .containsOnly(row(0));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testTrinoAnalyzeWithNonLowercaseColumnName(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_trino_analyze_with_uppercase_field" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(col1 INT, COL2 INT) USING ICEBERG");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 1)");
        env.executeTrinoUpdate("ANALYZE " + trinoTableName);

        // We're not verifying results of ANALYZE (covered by non-product tests), but we're verifying table is readable.
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(1, 1));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(1, 1));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoTableAfterCleaningUp(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_after_expiring_snapshots" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s')", trinoTableName, storageFormat));
        // separate inserts give us snapshot per insert
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1004)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1005)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1006)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1007)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1008)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1009)", trinoTableName));
        env.executeTrinoUpdate(format("DELETE FROM %s WHERE _string = '%s'", trinoTableName, 'b'));

        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            session.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            session.executeUpdate(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
            session.executeUpdate(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));
        });

        io.trino.testing.containers.environment.Row expectedRow = row(3006);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(expectedRow);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testDeleteAfterPartitionEvolution(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_delete_after_partition_evolution_" + storageFormat + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
        env.executeSparkUpdate(format(
                "CREATE TABLE %s (" +
                        "col0 BIGINT, " +
                        "col1 BIGINT, " +
                        "col2 BIGINT) "
                        + " USING ICEBERG"
                        + " TBLPROPERTIES ('write.format.default' = '%s', 'format-version' = 2, 'write.delete.mode' = 'merge-on-read')",
                sparkTableName,
                storageFormat));
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (1, 11, 21)");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD bucket(3, col0)");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (2, 12, 22)");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col1");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (3, 13, 23)");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col1");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col2");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (4, 14, 24)");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD bucket(3, col0)");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP PARTITION FIELD col2");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD col0");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (5, 15, 25)");

        List<io.trino.testing.containers.environment.Row> expected = new ArrayList<>();
        expected.add(row(1, 11, 21));
        expected.add(row(2, 12, 22));
        expected.add(row(3, 13, 23));
        expected.add(row(4, 14, 24));
        expected.add(row(5, 15, 25));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);

        for (int columnValue = 1; columnValue <= 5; columnValue++) {
            env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE col0 = " + columnValue);
            // Rows are in order so removing the first one always matches columnValue
            expected.remove(0);
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected);
            assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        }

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoAlterStructColumnType(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_trino_alter_row_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "')" +
                "AS SELECT CAST(row(1, 2) AS row(a integer, b integer)) AS col");

        // Add a nested field
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, b integer, c integer)");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"b\" integer, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(env.executeTrino("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Update a nested field
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, b bigint, c integer)");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"b\" bigint, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(env.executeTrino("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Drop a nested field
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, c integer)");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.c FROM " + sparkTableName)).containsOnly(row(1, null));
        assertThat(env.executeTrino("SELECT col.a, col.c FROM " + trinoTableName)).containsOnly(row(1, null));

        // Adding a nested field with the same name doesn't restore the old data
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(a integer, c integer, b bigint)");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"c\" integer, \"b\" bigint)");
        assertThat(env.executeSpark("SELECT col.a, col.c, col.b FROM " + sparkTableName)).containsOnly(row(1, null, null));
        assertThat(env.executeTrino("SELECT col.a, col.c, col.b FROM " + trinoTableName)).containsOnly(row(1, null, null));

        // Reorder fields
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE row(c integer, b bigint, a integer)");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"c\" integer, \"b\" bigint, \"a\" integer)");
        assertThat(env.executeSpark("SELECT col.b, col.c, col.a FROM " + sparkTableName)).containsOnly(row(null, null, 1));
        assertThat(env.executeTrino("SELECT col.b, col.c, col.a FROM " + trinoTableName)).containsOnly(row(null, null, 1));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("testSetColumnTypeDataProvider")
    void testTrinoSetColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        testTrinoSetColumnType(false, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue, env);
    }

    @ParameterizedTest
    @MethodSource("testSetColumnTypeDataProvider")
    void testTrinoSetPartitionedColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        testTrinoSetColumnType(true, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue, env);
    }

    private void testTrinoSetColumnType(boolean partitioned, StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_set_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "'" + (partitioned ? ", partitioning = ARRAY['col']" : "") + ")" +
                "AS SELECT CAST(" + sourceValueLiteral + " AS " + sourceColumnType + ") AS col");

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col SET DATA TYPE " + newColumnType);

        assertThat(getColumnType(baseTableName, env)).isEqualTo(newColumnType);
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(newValue));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(newValue));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("testSetColumnTypeDataProvider")
    void testTrinoSetFieldType(StorageFormat storageFormat, String sourceFieldType, String sourceValueLiteral, String newFieldType, Object newValue, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_set_field_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " " +
                "WITH (format = '" + storageFormat + "')" +
                "AS SELECT CAST(row(" + sourceValueLiteral + ") AS row(field " + sourceFieldType + ")) AS col");

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ALTER COLUMN col.field SET DATA TYPE " + newFieldType);

        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"field\" " + newFieldType + ")");
        assertThat(env.executeTrino("SELECT col.field FROM " + trinoTableName)).containsOnly(row(newValue));
        assertThat(env.executeSpark("SELECT col.field FROM " + sparkTableName)).containsOnly(row(newValue));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("sparkAlterColumnTypeDataProvider")
    void testSparkAlterColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        testSparkAlterColumnType(false, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue, env);
    }

    @ParameterizedTest
    @MethodSource("sparkAlterColumnTypeDataProvider")
    void testSparkAlterPartitionedColumnType(StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        testSparkAlterColumnType(true, storageFormat, sourceColumnType, sourceValueLiteral, newColumnType, newValue, env);
    }

    private void testSparkAlterColumnType(boolean partitioned, StorageFormat storageFormat, String sourceColumnType, String sourceValueLiteral, String newColumnType, Object newValue, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_alter_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName +
                (partitioned ? " PARTITIONED BY (col)" : "") +
                " TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT CAST(" + sourceValueLiteral + " AS " + sourceColumnType + ") AS col");

        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ALTER COLUMN col TYPE " + newColumnType);

        assertThat(getColumnType(baseTableName, env)).isEqualTo(newColumnType);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(row(newValue));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(newValue));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkAlterStructColumnType(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_spark_alter_struct_column_type_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName +
                " TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')" +
                "AS SELECT named_struct('a', 1, 'b', 2) AS col");

        // Add a nested field
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD COLUMN col.c integer");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"b\" integer, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(env.executeTrino("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Update a nested field
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ALTER COLUMN col.b TYPE bigint");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"b\" bigint, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.b, col.c FROM " + sparkTableName)).containsOnly(row(1, 2, null));
        assertThat(env.executeTrino("SELECT col.a, col.b, col.c FROM " + trinoTableName)).containsOnly(row(1, 2, null));

        // Drop a nested field
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " DROP COLUMN col.b");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"c\" integer)");
        assertThat(env.executeSpark("SELECT col.a, col.c FROM " + sparkTableName)).containsOnly(row(1, null));
        assertThat(env.executeTrino("SELECT col.a, col.c FROM " + trinoTableName)).containsOnly(row(1, null));

        // Adding a nested field with the same name doesn't restore the old data
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD COLUMN col.b bigint");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"a\" integer, \"c\" integer, \"b\" bigint)");
        assertThat(env.executeSpark("SELECT col.a, col.c, col.b FROM " + sparkTableName)).containsOnly(row(1, null, null));
        assertThat(env.executeTrino("SELECT col.a, col.c, col.b FROM " + trinoTableName)).containsOnly(row(1, null, null));

        // Reorder fields
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ALTER COLUMN col.a AFTER b");
        assertThat(getColumnType(baseTableName, env)).isEqualTo("row(\"c\" integer, \"b\" bigint, \"a\" integer)");
        assertThat(env.executeSpark("SELECT col.b, col.c, col.a FROM " + sparkTableName)).containsOnly(row(null, null, 1));
        assertThat(env.executeTrino("SELECT col.b, col.c, col.a FROM " + trinoTableName)).containsOnly(row(null, null, 1));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsAndCompressionCodecs")
    void testTrinoReadingSparkCompressedData(StorageFormat storageFormat, String compressionCodec, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        List<io.trino.testing.containers.environment.Row> rows = IntStream.range(0, 555)
                .mapToObj(i -> row("a" + i, i))
                .collect(toImmutableList());

        switch (storageFormat) {
            case PARQUET:
                env.executeSparkUpdate("SET spark.sql.parquet.compression.codec = " + compressionCodec);
                break;

            case ORC:
                if ("GZIP".equals(compressionCodec)) {
                    env.executeSparkUpdate("SET spark.sql.orc.compression.codec = zlib");
                }
                else {
                    env.executeSparkUpdate("SET spark.sql.orc.compression.codec = " + compressionCodec);
                }
                break;

            case AVRO:
                if ("NONE".equals(compressionCodec)) {
                    env.executeSparkUpdate("SET spark.sql.avro.compression.codec = uncompressed");
                }
                else if ("SNAPPY".equals(compressionCodec)) {
                    env.executeSparkUpdate("SET spark.sql.avro.compression.codec = snappy");
                }
                else if ("ZSTD".equals(compressionCodec)) {
                    env.executeSparkUpdate("SET spark.sql.avro.compression.codec = zstandard");
                }
                else {
                    assertThatThrownBy(() -> env.executeSparkUpdate("SET spark.sql.avro.compression.codec = " + compressionCodec))
                            .hasStackTraceContaining("The value of spark.sql.avro.compression.codec should be one of bzip2, deflate, uncompressed, xz, snappy, zstandard");
                    Assumptions.abort("Unsupported compression codec");
                }
                break;

            default:
                throw new UnsupportedOperationException("Unsupported storage format: " + storageFormat);
        }

        env.executeSparkUpdate(
                "CREATE TABLE " + sparkTableName + " (a string, b bigint) " +
                        "USING ICEBERG TBLPROPERTIES ('write.format.default' = '" + storageFormat + "')");
        env.executeSparkUpdate(
                "INSERT INTO " + sparkTableName + " VALUES " +
                        rows.stream()
                                .map(row -> format("('%s', %s)", row.getValues().get(0), row.getValues().get(1)))
                                .collect(Collectors.joining(", ")));
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(rows);
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName))
                .containsOnly(rows);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsAndCompressionCodecs")
    void testSparkReadingTrinoCompressedData(StorageFormat storageFormat, String compressionCodec, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_trino_compression" +
                "_" + storageFormat +
                "_" + compressionCodec +
                "_" + randomNameSuffix());
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        String createTable = "CREATE TABLE " + trinoTableName + " WITH (format = '" + storageFormat + "', compression_codec = '" + compressionCodec + "') AS TABLE tpch.tiny.nation";
        if (storageFormat == StorageFormat.PARQUET && "LZ4".equals(compressionCodec)) {
            // TODO (https://github.com/trinodb/trino/issues/9142) LZ4 is not supported with native Parquet writer
            assertThatThrownBy(() -> env.executeTrinoUpdate(createTable))
                    .hasMessageContaining("Compression codec LZ4 not supported for Parquet");
            return;
        }
        if (storageFormat == StorageFormat.AVRO && compressionCodec.equals("LZ4")) {
            assertThatThrownBy(() -> env.executeTrinoUpdate(createTable))
                    .hasMessageContaining("Compression codec LZ4 not supported for Avro");
            return;
        }
        env.executeTrinoUpdate(createTable);

        List<io.trino.testing.containers.environment.Row> expected = env.executeTrino("TABLE tpch.tiny.nation").rows().stream()
                .map(row -> row(row.toArray()))
                .collect(toImmutableList());
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName))
                .containsOnly(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName))
                .containsOnly(expected);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoWritingDataWithWriterDataPathSet(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_writer_data_path_" + storageFormat);
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_writer_data_path_/obj-data";

        env.executeSparkUpdate(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES (" +
                        "'write.data.path'='%s'," +
                        "'write.format.default' = '%s'," +
                        "'format-version' = %s)",
                sparkTableName, dataPath, storageFormat, specVersion));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        io.trino.testing.containers.environment.Row result = row("a_string", 1000000000000000L);
        assertThat(env.executeSpark(format("SELECT _string, _bigint FROM %s", sparkTableName))).containsOnly(result);
        assertThat(env.executeTrino(format("SELECT _string, _bigint FROM %s", trinoTableName))).containsOnly(result);

        QueryResult queryResult = env.executeTrino(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult.getRowsCount()).isEqualTo(1);
        assertThat(queryResult.getColumnCount()).isEqualTo(1);
        assertThat(((String) queryResult.getOnlyValue())).contains(dataPath);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testTrinoIgnoresUnsupportedSparkSortOrder(SparkIcebergEnvironment env)
    {
        String sourceTableNameBase = "test_insert_into_sorted_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(sourceTableNameBase);
        String sparkTableName = sparkTableName(sourceTableNameBase);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " (a INT, b INT, c INT) USING ICEBERG");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " WRITE ORDERED BY truncate(b, 3), a NULLS LAST");

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + trinoTableName).getOnlyValue())
                .doesNotContain("sorted_by");

        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (3, 2333, 1), (NULL, NULL, NULL), (1, 2222, 3)");
        assertThat(env.executeSpark("SELECT _pos, a, b, c FROM " + sparkTableName))
                .contains(
                        row(0, 1, 2222, 3),
                        row(1, 3, 2333, 1),
                        row(2, null, null, null));

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @Test
    void testRenameNestedField(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_rename_nested_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " AS SELECT CAST(row(1, row(10)) AS row(a integer, b row(x integer))) AS col");

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ADD COLUMN col.c integer");
        assertThat(env.executeTrino("SELECT col.a, col.b.x, col.c FROM " + trinoTableName)).containsOnly(row(1, 10, null));
        assertThat(env.executeSpark("SELECT col.a, col.b.x, col.c FROM " + sparkTableName)).containsOnly(row(1, 10, null));

        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " ADD COLUMN col.b.y integer");
        assertThat(env.executeTrino("SELECT col.a, col.b.x, col.b.y, col.c FROM " + trinoTableName)).containsOnly(row(1, 10, null, null));
        assertThat(env.executeSpark("SELECT col.a, col.b.x, col.b.y, col.c FROM " + sparkTableName)).containsOnly(row(1, 10, null, null));

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testDropPastPartitionedField(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_drop_past_partitioned_field_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(id INTEGER, parent ROW(nested VARCHAR, nested_another VARCHAR))");
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " ADD PARTITION FIELD parent.nested");
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY[]");

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " DROP COLUMN parent.nested"))
                .hasMessageContaining("Cannot drop column which is used by an old partition spec: parent.nested");

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("tableFormatWithDeleteFormat")
    void testTrinoReadsSparkRowLevelDeletesWithRowTypes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat, SparkIcebergEnvironment env)
    {
        String tableName = toLowerCase(format("test_trino_reads_spark_row_level_deletes_row_types_%s_%s_%s", tableStorageFormat.name(), deleteFileStorageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(part_key INT, int_t INT, row_t STRUCT<a:INT, b:INT>) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES " +
                "(1, 1, named_struct('a', 1, 'b', 2)), (1, 2, named_struct('a', 3, 'b', 4)), (1, 3, named_struct('a', 5, 'b', 6)), (2, 4, named_struct('a', 1, 'b',2))");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        env.executeSparkUpdate("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + tableName + "', options => map('min-input-files','1'))");
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE int_t = 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1, 2), row(1, 6), row(2, 2));
        assertThat(env.executeTrino("SELECT part_key, row_t.b FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT part_key, row_t.b FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testSparkReadsTrinoRowLevelDeletesWithRowTypes(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String tableName = toLowerCase(format("test_spark_reads_trino_row_level_deletes_row_types_%s_%s", storageFormat.name(), randomNameSuffix()));
        String sparkTableName = sparkTableName(tableName);
        String trinoTableName = trinoTableName(tableName);

        env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(part_key INT, int_t INT, row_t ROW(a INT, b INT)) " +
                "WITH(partitioning = ARRAY['part_key'], format_version = 2, format = '" + storageFormat.name() + "') ");
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 1, row(1, 2)), (1, 2, row(3, 4)), (1, 3, row(5, 6)), (2, 4, row(1, 2))");
        env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE int_t = 2");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1, 2), row(1, 6), row(2, 2));
        assertThat(env.executeTrino("SELECT part_key, row_t.b FROM " + trinoTableName)).containsOnly(expected);
        assertThat(env.executeSpark("SELECT part_key, row_t.b FROM " + sparkTableName)).containsOnly(expected);

        env.executeSparkUpdate("DROP TABLE " + sparkTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testSparkReadsTrinoTableAfterOptimizeAndCleaningUp(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_after_expiring_snapshots_after_optimize" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s', format_version = %s)", trinoTableName, storageFormat, specVersion));
        // separate inserts give us snapshot per insert
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1004)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1005)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('b', 1006)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1007)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1008)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('c', 1009)", trinoTableName));
        env.executeTrinoUpdate(format("DELETE FROM %s WHERE _string = '%s'", trinoTableName, 'b'));

        env.executeTrinoUpdate(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            session.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            session.executeUpdate(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
            session.executeUpdate(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));
        });

        io.trino.testing.containers.environment.Row expectedRow = row(3006);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(expectedRow);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("storageFormatsWithSpecVersion")
    void testTrinoReadsTrinoTableWithSparkDeletesAfterOptimizeAndCleanUp(StorageFormat storageFormat, int specVersion, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_spark_reads_trino_partitioned_table_with_deletes_after_expiring_snapshots_after_optimize" + storageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'], format = '%s', format_version = %s)", trinoTableName, storageFormat, specVersion));
        // separate inserts give us snapshot per insert
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1001)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1002)", trinoTableName));
        env.executeSparkUpdate(format("DELETE FROM %s WHERE _bigint = 1002", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1003)", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('a', 1004)", trinoTableName));

        env.executeTrinoUpdate(format("ALTER TABLE %s EXECUTE OPTIMIZE", trinoTableName));
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            session.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            session.executeUpdate(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
            session.executeUpdate(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));
        });

        io.trino.testing.containers.environment.Row expectedRow = row(3008);
        String selectByString = "SELECT SUM(_bigint) FROM %s WHERE _string = 'a'";
        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(expectedRow);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(expectedRow);

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @ParameterizedTest
    @MethodSource("tableFormatWithDeleteFormat")
    void testCleaningUpIcebergTableWithRowLevelDeletes(StorageFormat tableStorageFormat, StorageFormat deleteFileStorageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = toLowerCase("test_cleaning_up_iceberg_table_fails_for_table_v2" + tableStorageFormat);
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + "(part_key INT, int_t INT, row_t STRUCT<a:INT, b:INT>) " +
                "USING ICEBERG PARTITIONED BY (part_key) " +
                "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read'," +
                "'write.format.default'='" + tableStorageFormat.name() + "'," +
                "'write.delete.format.default'='" + deleteFileStorageFormat.name() + "')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES " +
                "(1, 1, named_struct('a', 1, 'b', 2)), (1, 2, named_struct('a', 3, 'b', 4)), (1, 3, named_struct('a', 5, 'b', 6)), (2, 4, named_struct('a', 1, 'b', 2)), (2, 2, named_struct('a', 2, 'b', 3))");
        // Spark inserts may create multiple files. rewrite_data_files ensures it is compacted to one file so a row level delete occurs.
        env.executeSparkUpdate("CALL " + SPARK_CATALOG + ".system.rewrite_data_files(table=>'" + TEST_SCHEMA_NAME + "." + baseTableName + "', options => map('min-input-files','1'))");
        env.executeSparkUpdate("DELETE FROM " + sparkTableName + " WHERE int_t = 2");

        io.trino.testing.containers.environment.Row expectedRow = row(4);
        String selectByString = "SELECT SUM(int_t) FROM %s WHERE part_key = 1";
        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(expectedRow);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(expectedRow);

        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            session.executeUpdate(format("ALTER TABLE %s EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')", trinoTableName));
            session.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            session.executeUpdate(format("ALTER TABLE %s EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')", trinoTableName));
        });

        assertThat(env.executeTrino(format(selectByString, trinoTableName)))
                .containsOnly(expectedRow);
        assertThat(env.executeSpark(format(selectByString, sparkTableName)))
                .containsOnly(expectedRow);
    }

    private String getColumnType(String tableName, SparkIcebergEnvironment env)
    {
        return (String) env.executeTrino("SELECT data_type FROM " + TRINO_CATALOG + ".information_schema.columns " +
                        "WHERE table_schema = '" + TEST_SCHEMA_NAME + "' AND " +
                        "table_name = '" + tableName + "' AND " +
                        "column_name = 'col'")
                .getOnlyValue();
    }

    // Register/Unregister table tests

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithTableLocation(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_table_location_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));

        List<io.trino.testing.containers.environment.Row> expected = List.of(row(1, "INDIA", true));
        String tableLocation = env.getTableLocation(trinoTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(env.executeSpark(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithComments(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_comments_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));
        env.executeTrinoUpdate(format("COMMENT ON TABLE %s is 'my-table-comment'", trinoTableName));
        env.executeTrinoUpdate(format("COMMENT ON COLUMN %s.a is 'a-comment'", trinoTableName));
        env.executeTrinoUpdate(format("COMMENT ON COLUMN %s.b is 'b-comment'", trinoTableName));
        env.executeTrinoUpdate(format("COMMENT ON COLUMN %s.c is 'c-comment'", trinoTableName));

        String tableLocation = env.getTableLocation(trinoTableName);
        String metadataFileName = env.getLatestMetadataFilename(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation, metadataFileName));

        assertThat(getTableComment(baseTableName, env)).isEqualTo("my-table-comment");
        assertThat(getColumnComment(baseTableName, "a", env)).isEqualTo("a-comment");
        assertThat(getColumnComment(baseTableName, "b", env)).isEqualTo("b-comment");
        assertThat(getColumnComment(baseTableName, "c", env)).isEqualTo("c-comment");
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithShowCreateTable(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_show_create_table_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        QueryResult expectedDescribeTable = env.executeSpark("DESCRIBE TABLE EXTENDED " + sparkTableName);
        List<io.trino.testing.containers.environment.Row> expectedDescribeTableRows = expectedDescribeTable.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());

        QueryResult expectedShowCreateTable = env.executeTrino("SHOW CREATE TABLE " + trinoTableName);
        List<io.trino.testing.containers.environment.Row> expectedShowCreateTableRows = expectedShowCreateTable.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());

        String tableLocation = env.getTableLocation(trinoTableName);
        String metadataFileName = env.getLatestMetadataFilename(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation, metadataFileName));

        QueryResult actualDescribeTable = env.executeSpark("DESCRIBE TABLE EXTENDED " + sparkTableName);
        QueryResult actualShowCreateTable = env.executeTrino("SHOW CREATE TABLE " + trinoTableName);

        assertThat(actualDescribeTable).hasColumns(expectedDescribeTable.getColumnTypes()).containsExactlyInOrder(expectedDescribeTableRows);
        assertThat(actualShowCreateTable).hasColumns(expectedShowCreateTable.getColumnTypes()).containsExactlyInOrder(expectedShowCreateTableRows);
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithReInsert(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_re_insert_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = env.getTableLocation(trinoTableName);
        String metadataFileName = env.getLatestMetadataFilename(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation, metadataFileName));
        env.executeSparkUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", sparkTableName));

        List<io.trino.testing.containers.environment.Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(env.executeSpark(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithDroppedTable(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_register_table_with_dropped_table_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = env.getTableLocation(trinoTableName);
        String baseTableNameNew = baseTableName + "_new";

        // Drop table to verify register_table call fails when no metadata can be found (table doesn't exist)
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));

        assertThatThrownBy(() -> env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableNameNew, tableLocation)))
                .hasStackTraceContaining("No versioned metadata file exists at location");
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithDifferentTableName(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_different_table_name_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = env.getTableLocation(trinoTableName);
        String metadataFileName = env.getLatestMetadataFilename(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        String baseTableNameNew = baseTableName + "_new";
        String trinoTableNameNew = trinoTableName(baseTableNameNew);
        String sparkTableNameNew = sparkTableName(baseTableNameNew);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableNameNew, tableLocation, metadataFileName));
        env.executeSparkUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", sparkTableNameNew));
        List<io.trino.testing.containers.environment.Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableNameNew))).containsOnly(expected);
        assertThat(env.executeSpark(format("SELECT * FROM %s", sparkTableNameNew))).containsOnly(expected);
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableNameNew));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testRegisterTableWithMetadataFile(StorageFormat storageFormat, SparkIcebergEnvironment env)
            throws TException
    {
        String baseTableName = "test_register_table_with_metadata_file_" + storageFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate(format("CREATE TABLE %s (a INT, b STRING, c BOOLEAN) USING ICEBERG TBLPROPERTIES ('write.format.default' = '%s')", sparkTableName, storageFormat));
        env.executeSparkUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", sparkTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName));

        String tableLocation = env.getTableLocation(trinoTableName);
        String metadataFileName = env.getLatestMetadataFilename(TRINO_CATALOG, TEST_SCHEMA_NAME, baseTableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        env.dropTableFromMetastore(TEST_SCHEMA_NAME, baseTableName);

        env.executeTrinoUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseTableName, tableLocation, metadataFileName));
        env.executeTrinoUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", trinoTableName));
        List<io.trino.testing.containers.environment.Row> expected = List.of(row(1, "INDIA", true), row(2, "USA", false), row(3, "POLAND", true));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableName))).containsOnly(expected);
        assertThat(env.executeSpark(format("SELECT * FROM %s", sparkTableName))).containsOnly(expected);
        env.executeTrinoUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @Test
    void testUnregisterNotIcebergTable(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_unregister_not_iceberg_table_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String hiveTableName = TEST_SCHEMA_NAME + "." + baseTableName;

        env.executeHiveUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 a");

        assertThatThrownBy(() -> env.executeTrinoUpdate("CALL iceberg.system.unregister_table('default', '" + baseTableName + "')"))
                .hasStackTraceContaining("Not an Iceberg table");

        assertThat(env.executeSpark("SELECT * FROM " + hiveTableName)).containsOnly(row(1));
        assertThat(env.executeTrino("SELECT * FROM hive.default." + baseTableName)).containsOnly(row(1));
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM " + trinoTableName))
                .hasStackTraceContaining("Not an Iceberg table");

        env.executeHiveUpdate("DROP TABLE " + hiveTableName);
    }

    // Partition tests with mixed case columns

    @Test
    void testPartitionedByNonLowercaseColumn(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_partitioned_by_non_lowercase_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("CREATE TABLE " + sparkTableName + " USING ICEBERG PARTITIONED BY (`PART`) TBLPROPERTIES ('format-version'='2') AS SELECT 1 AS data, 2 AS `PART`");
        try {
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).contains(row(1, 2));

            env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (3, 4)");
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).contains(row(1, 2), row(3, 4));

            env.executeTrinoUpdate("DELETE FROM " + trinoTableName + " WHERE data = 3");
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).contains(row(1, 2));

            env.executeTrinoUpdate("UPDATE " + trinoTableName + " SET part = 20");
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).contains(row(1, 20));

            env.executeTrinoUpdate("MERGE INTO " + trinoTableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE");
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).hasNoRows();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE " + sparkTableName);
        }
    }

    @Test
    void testPartitioningWithMixedCaseColumnInTrino(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_partitioning_with_mixed_case_column_in_spark";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeSparkUpdate("DROP TABLE IF EXISTS " + sparkTableName);
        env.executeSparkUpdate(format(
                "CREATE TABLE %s (id INTEGER, `mIxEd_COL` STRING) USING ICEBERG",
                sparkTableName));
        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY['mIxEd_COL']"))
                .hasStackTraceContaining("Unable to parse partitioning value");
        env.executeTrinoUpdate("ALTER TABLE " + trinoTableName + " SET PROPERTIES partitioning = ARRAY['\"mIxEd_COL\"']");

        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 'trino')");
        env.executeSparkUpdate("INSERT INTO " + sparkTableName + " VALUES (2, 'spark')");

        List<io.trino.testing.containers.environment.Row> expected = ImmutableList.of(row(1, "trino"), row(2, "spark"));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).contains(expected);
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).contains(expected);

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + trinoTableName).getOnlyValue())
                .contains("partitioning = ARRAY['\"mIxEd_COL\"']");
        assertThat((String) env.executeSpark("SHOW CREATE TABLE " + sparkTableName).getOnlyValue())
                .contains("PARTITIONED BY (mIxEd_COL)");

        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    // Special character partitioning tests

    @Test
    void testStringPartitioningWithSpecialCharactersCtasInTrino(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_trino";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (id, part_col) " +
                        "WITH (partitioning = ARRAY['part_col']) " +
                        "AS VALUES %s",
                trinoTableName,
                TRINO_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName, env);
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testStringPartitioningWithSpecialCharactersInsertInTrino(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_trino";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioning = ARRAY['part_col'])",
                trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES %s", trinoTableName, TRINO_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName, env);
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    @Test
    void testStringPartitioningWithSpecialCharactersInsertInSpark(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_string_partitioning_with_special_chars_ctas_in_spark";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioning = ARRAY['part_col'])",
                trinoTableName));
        env.executeSparkUpdate(format("INSERT INTO %s VALUES %s", sparkTableName, SPARK_INSERTED_PARTITION_VALUES));
        assertSelectsOnSpecialCharacters(trinoTableName, sparkTableName, env);
        env.executeTrinoUpdate("DROP TABLE " + trinoTableName);
    }

    private void assertSelectsOnSpecialCharacters(String trinoTableName, String sparkTableName, SparkIcebergEnvironment env)
    {
        assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        for (String value : SPECIAL_CHARACTER_VALUES) {
            String trinoValue = escapeTrinoString(value);
            String sparkValue = escapeSparkString(value);
            // Ensure Trino written metadata is readable from Spark and vice versa
            assertThat(env.executeSpark("SELECT count(*) FROM " + sparkTableName + " WHERE part_col = '" + sparkValue + "'"))
                    .withFailMessage("Spark query with predicate containing '" + value + "' contained no matches, expected one")
                    .containsOnly(row(1L));
            assertThat(env.executeTrino("SELECT count(*) FROM " + trinoTableName + " WHERE part_col = '" + trinoValue + "'"))
                    .withFailMessage("Trino query with predicate containing '" + value + "' contained no matches, expected one")
                    .containsOnly(row(1L));
        }
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testTrinoReadingMigratedNestedData(StorageFormat storageFormat, SparkIcebergEnvironment env)
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
        env.executeSparkUpdate(format(sparkTableDefinition, defaultCatalogTableName, storageFormat.name().toLowerCase(ENGLISH)));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", map('s1', array(named_struct('sName', 'ASName1', 'sNumber', 201), named_struct('sName', 'ASName2', 'sNumber', 202)))" +
                ", array(map('m1', array(named_struct('mName', 'MAS1Name1', 'mNumber', 301), named_struct('mName', 'MAS1Name2', 'mNumber', 302)))" +
                "       ,map('m2', array(named_struct('mName', 'MAS2Name1', 'mNumber', 401), named_struct('mName', 'MAS2Name2', 'mNumber', 402))))" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('street_number', 42, 'street_name', 'Wallaby Way'))";
        env.executeSparkUpdate(format(insert, defaultCatalogTableName));
        try {
            env.executeSparkUpdate(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                Assumptions.abort("This catalog doesn't support calling system.migrate procedure");
            }
            throw e;
        }

        String sparkTableName = sparkTableName(baseTableName);
        io.trino.testing.containers.environment.Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "P. Sherman", 42, "Wallaby Way");

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

        QueryResult sparkResult = env.executeSpark(sparkSelect + sparkTableName);
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
        QueryResult trinoResult = env.executeTrino(trinoSelect + trinoTableName);
        assertThat(trinoResult).containsOnly(row);

        // After removing the name mapping, columns from migrated files should be null since they are missing the Iceberg Field IDs
        env.executeSparkUpdate(format("ALTER TABLE %s UNSET TBLPROPERTIES ('schema.name-mapping.default')", sparkTableName));
        assertThat(env.executeTrino(trinoSelect + trinoTableName)).containsOnly(row(null, null, null, null, null, null, null, null));
        assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(row(null, null, null, null));
        assertThat(env.executeTrino("SELECT nested_struct.address.street_number, nested_struct.address.street_name FROM " + trinoTableName)).containsOnly(row(null, null));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testMigratedDataWithAlteredSchema(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_migrated_data_with_altered_schema_" + randomNameSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_struct STRUCT<id:INT, name:STRING, address:STRUCT<a:INT, b:STRING>>)\n" +
                " USING %s";
        env.executeSparkUpdate(format(sparkTableDefinition, defaultCatalogTableName, storageFormat));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", named_struct('id', 1, 'name', 'P. Sherman', 'address', named_struct('a', 42, 'b', 'Wallaby Way'))";
        env.executeSparkUpdate(format(insert, defaultCatalogTableName));
        try {
            env.executeSparkUpdate(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                Assumptions.abort("This catalog doesn't support calling system.migrate procedure");
            }
            throw e;
        }

        String sparkTableName = sparkTableName(baseTableName);
        env.executeSparkUpdate("ALTER TABLE " + sparkTableName + " RENAME COLUMN nested_struct TO nested_struct_moved");

        String select = "SELECT" +
                " nested_struct_moved.name" +
                ", nested_struct_moved.address.a" +
                ", nested_struct_moved.address.b" +
                "  FROM ";
        io.trino.testing.containers.environment.Row row = row("P. Sherman", 42, "Wallaby Way");

        QueryResult sparkResult = env.executeSpark(select + sparkTableName);
        assertThat(sparkResult).containsOnly(ImmutableList.of(row));

        String trinoTableName = trinoTableName(baseTableName);
        assertThat(env.executeTrino(select + trinoTableName)).containsOnly(ImmutableList.of(row));

        // After removing the name mapping, columns from migrated files should be null since they are missing the Iceberg Field IDs
        env.executeSparkUpdate(format("ALTER TABLE %s UNSET TBLPROPERTIES ('schema.name-mapping.default')", sparkTableName));
        assertThat(env.executeTrino(select + trinoTableName)).containsOnly(row(null, null, null));
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testMigratedDataWithPartialNameMapping(StorageFormat storageFormat, SparkIcebergEnvironment env)
    {
        String baseTableName = "test_migrated_data_with_partial_name_mapping_" + randomNameSuffix();
        String defaultCatalogTableName = sparkDefaultCatalogTableName(baseTableName);

        String sparkTableDefinition = "CREATE TABLE %s (a INT, b INT) USING " + storageFormat.name().toLowerCase(ENGLISH);
        env.executeSparkUpdate(format(sparkTableDefinition, defaultCatalogTableName));

        String insert = "INSERT INTO TABLE %s SELECT 1, 2";
        env.executeSparkUpdate(format(insert, defaultCatalogTableName));
        try {
            env.executeSparkUpdate(format("CALL system.migrate('%s')", defaultCatalogTableName));
        }
        catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Cannot use catalog spark_catalog: not a ProcedureCatalog")) {
                Assumptions.abort("This catalog doesn't support calling system.migrate procedure");
            }
            throw e;
        }

        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        // Test missing entry for column 'b'
        env.executeSparkUpdate(format(
                "ALTER TABLE %s SET TBLPROPERTIES ('schema.name-mapping.default'='[{\"field-id\": 1, \"names\": [\"a\"]}, {\"field-id\": 2, \"names\": [\"c\"]} ]')",
                sparkTableName));
        assertThat(env.executeTrino("SELECT a, b FROM " + trinoTableName))
                .containsOnly(row(1, null));
    }

    @Test
    void testHandlingPartitionSchemaEvolutionInPartitionMetadata(SparkIcebergEnvironment env)
    {
        String baseTableName = "test_handling_partition_schema_evolution_" + randomNameSuffix();
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        env.executeTrinoUpdate(format("CREATE TABLE %s (old_partition_key INT, new_partition_key INT, value date) WITH (PARTITIONING = array['old_partition_key'])", trinoTableName));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1, 10, date '2022-04-10'), (2, 20, date '2022-05-11'), (3, 30, date '2022-06-12'), (2, 20, date '2022-06-13')", trinoTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1"),
                ImmutableMap.of("old_partition_key", "2"),
                ImmutableMap.of("old_partition_key", "3")), env);

        env.executeSparkUpdate(format("ALTER TABLE %s DROP PARTITION FIELD old_partition_key", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD PARTITION FIELD new_partition_key", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")), env);

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (4, 40, date '2022-08-15')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")), env);

        env.executeSparkUpdate(format("ALTER TABLE %s DROP PARTITION FIELD new_partition_key", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD PARTITION FIELD old_partition_key", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")), env);

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (5, 50, date '2022-08-15')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null")), env);

        env.executeSparkUpdate(format("ALTER TABLE %s DROP PARTITION FIELD old_partition_key", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD PARTITION FIELD days(value)", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null")), env);

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (6, 60, date '2022-08-16')", trinoTableName));
        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null")), env);

        env.executeSparkUpdate(format("ALTER TABLE %s DROP PARTITION FIELD value_day", sparkTableName));
        env.executeSparkUpdate(format("ALTER TABLE %s ADD PARTITION FIELD months(value)", sparkTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null", "value_month", "null")), env);

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (7, 70, date '2022-08-17')", trinoTableName));

        validatePartitioning(baseTableName, sparkTableName, ImmutableList.of(
                ImmutableMap.of("old_partition_key", "1", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "null", "value_month", "631"),
                ImmutableMap.of("old_partition_key", "2", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "40", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "null", "new_partition_key", "null", "value_day", "2022-08-16", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "5", "new_partition_key", "null", "value_day", "null", "value_month", "null"),
                ImmutableMap.of("old_partition_key", "3", "new_partition_key", "null", "value_day", "null", "value_month", "null")), env);
    }

    @Test
    void testInsertReadingFromParquetTableWithNestedRowFieldNotPresentInDataFile(SparkIcebergEnvironment env)
    {
        // regression test for https://github.com/trinodb/trino/issues/9264
        String sourceTableNameBase = "test_nested_missing_row_field_source";
        String trinoSourceTableName = trinoTableName(sourceTableNameBase);
        String sparkSourceTableName = sparkTableName(sourceTableNameBase);

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoSourceTableName);
        env.executeTrinoUpdate(
                "CREATE TABLE " + trinoSourceTableName + " WITH (format = 'PARQUET') AS " +
                        " SELECT CAST(" +
                        "    ROW(1, ROW(2, 3)) AS " +
                        "    ROW(foo BIGINT, a_sub_struct ROW(x BIGINT, y BIGINT)) " +
                        ") AS a_struct");

        env.executeSparkUpdate("ALTER TABLE " + sparkSourceTableName + " ADD COLUMN a_struct.a_sub_struct_2 STRUCT<z: BIGINT>");

        env.executeTrinoUpdate(
                "INSERT INTO " + trinoSourceTableName +
                        " SELECT CAST(" +
                        "    ROW(1, ROW(2, 3), ROW(4)) AS " +
                        "    ROW(foo BIGINT,\n" +
                        "        a_sub_struct ROW(x BIGINT, y BIGINT), " +
                        "        a_sub_struct_2 ROW(z BIGINT)" +
                        "    )" +
                        ") AS a_struct");

        String trinoTargetTableName = trinoTableName("test_nested_missing_row_field_target");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTargetTableName);
        env.executeTrinoUpdate("CREATE TABLE " + trinoTargetTableName + " WITH (format = 'PARQUET') AS SELECT * FROM " + trinoSourceTableName);

        assertThat(env.executeTrino("SELECT * FROM " + trinoTargetTableName))
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

    // Helper methods

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

    private void validatePartitioning(String baseTableName, String sparkTableName, List<Map<String, String>> expectedValues, SparkIcebergEnvironment env)
    {
        List<String> trinoResult = expectedValues.stream().map(m ->
                        m.entrySet().stream()
                                .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                                .collect(Collectors.joining(", ", "{", "}")))
                .collect(toImmutableList());
        List<Object> partitioning = env.executeTrino(format("SELECT partition, record_count FROM iceberg.default.\"%s$partitions\"", baseTableName))
                .column(1);
        Set<String> partitions = partitioning.stream().map(String::valueOf).collect(toUnmodifiableSet());
        assertThat(partitions).hasSize(expectedValues.size());
        assertThat(partitions).containsAll(trinoResult);
        List<String> sparkResult = expectedValues.stream().map(m ->
                        m.entrySet().stream()
                                .map(entry -> format("\"%s\":%s", entry.getKey(), entry.getValue()))
                                .collect(Collectors.joining(",", "{", "}")))
                .collect(toImmutableList());
        partitioning = env.executeSpark(format("SELECT partition from %s.files", sparkTableName)).column(1);
        partitions = partitioning.stream().map(String::valueOf).collect(toUnmodifiableSet());
        assertThat(partitions).hasSize(expectedValues.size());
        assertThat(partitions).containsAll(sparkResult);
    }

    private static String getTableComment(String tableName, SparkIcebergEnvironment env)
    {
        return (String) env.executeTrino(
                "SELECT comment FROM system.metadata.table_comments " +
                "WHERE catalog_name = '" + TRINO_CATALOG + "' " +
                "AND schema_name = '" + TEST_SCHEMA_NAME + "' " +
                "AND table_name = '" + tableName + "'").getOnlyValue();
    }

    private static String getColumnComment(String tableName, String columnName, SparkIcebergEnvironment env)
    {
        return (String) env.executeTrino(
                "SELECT comment FROM " + TRINO_CATALOG + ".information_schema.columns " +
                "WHERE table_schema = '" + TEST_SCHEMA_NAME + "' " +
                "AND table_name = '" + tableName + "' " +
                "AND column_name = '" + columnName + "'").getOnlyValue();
    }

    private void assumeTrinoSupportsFormatVersion3(SparkIcebergEnvironment env)
    {
        String tableName = toLowerCase("test_assume_format_v3_support_" + randomNameSuffix());
        String trinoTableName = trinoTableName(tableName);
        try {
            env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + "(a INT) WITH (format_version = 3)");
        }
        catch (RuntimeException e) {
            Assumptions.assumeTrue(false, "Trino format v3 unsupported in this environment: " + e.getMessage());
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        }
    }

    // Data providers

    static Stream<Arguments> specVersions()
    {
        return Stream.of(Arguments.of(1), Arguments.of(2));
    }

    static Stream<Object[]> storageFormats()
    {
        return Stream.of(StorageFormat.values())
                .map(storageFormat -> new Object[] {storageFormat});
    }

    // Provides each supported table formats paired with each delete file format.
    static Stream<Arguments> tableFormatWithDeleteFormat()
    {
        return Stream.of(StorageFormat.values())
                .flatMap(tableStorageFormat -> Arrays.stream(StorageFormat.values())
                        .map(deleteFileStorageFormat -> Arguments.of(tableStorageFormat, deleteFileStorageFormat)));
    }

    static Stream<Arguments> storageFormatsWithSpecVersion()
    {
        List<StorageFormat> storageFormats = Stream.of(StorageFormat.values())
                .collect(toImmutableList());
        List<Integer> specVersions = ImmutableList.of(1, 2);

        return storageFormats.stream()
                .flatMap(storageFormat -> specVersions.stream().map(specVersion -> Arguments.of(storageFormat, specVersion)));
    }

    static Stream<Arguments> testSetColumnTypeDataProvider()
    {
        // sourceColumnType, sourceValueLiteral, newColumnType, newValue
        List<Object[]> typeConversions = List.of(
                new Object[] {"integer", "2147483647", "bigint", 2147483647L},
                new Object[] {"real", "10.3", "double", 10.300000190734863},
                new Object[] {"real", "'NaN'", "double", Double.NaN},
                new Object[] {"decimal(5,3)", "'12.345'", "decimal(10,3)", BigDecimal.valueOf(12.345)});

        return Stream.of(StorageFormat.values())
                .flatMap(storageFormat -> typeConversions.stream()
                        .map(conversion -> Arguments.of(
                                storageFormat,
                                conversion[0],
                                conversion[1],
                                conversion[2],
                                conversion[3])));
    }

    static Stream<Arguments> sparkAlterColumnTypeDataProvider()
    {
        // sourceColumnType, sourceValueLiteral, newColumnType, newValue
        // Note: Uses Spark type names (float instead of real)
        List<Object[]> typeConversions = List.of(
                new Object[] {"integer", "2147483647", "bigint", 2147483647L},
                new Object[] {"float", "10.3", "double", 10.300000190734863},
                new Object[] {"float", "'NaN'", "double", Double.NaN},
                new Object[] {"decimal(5,3)", "'12.345'", "decimal(10,3)", BigDecimal.valueOf(12.345)});

        return Stream.of(StorageFormat.values())
                .flatMap(storageFormat -> typeConversions.stream()
                        .map(conversion -> Arguments.of(
                                storageFormat,
                                conversion[0],
                                conversion[1],
                                conversion[2],
                                conversion[3])));
    }

    static Stream<Arguments> storageFormatsAndCompressionCodecs()
    {
        List<String> codecs = compressionCodecs();
        return Stream.of(StorageFormat.values())
                .flatMap(storageFormat -> codecs.stream()
                        .map(compressionCodec -> Arguments.of(storageFormat, compressionCodec)));
    }

    private static List<String> compressionCodecs()
    {
        return List.of(
                "NONE",
                "SNAPPY",
                "LZ4",
                "ZSTD",
                "GZIP");
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

    private static String toLowerCase(String name)
    {
        return name.toLowerCase(ENGLISH);
    }
}
