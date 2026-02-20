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
import com.google.common.collect.ImmutableMap;
import com.google.common.math.IntMath;
import io.trino.jdbc.TrinoConnection;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * JUnit 5 port of TestHiveCompatibility.
 * <p>
 * Tests compatibility between Trino and Hive for various storage formats and data types.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormatsDetailed
class TestHiveCompatibility
{
    // ---- Data Providers ----

    static Stream<StorageFormat> storageFormatsWithConfiguration()
    {
        return Stream.of(
                storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_validate", "true")),
                storageFormat("PARQUET", ImmutableMap.of("hive.parquet_optimized_writer_validation_percentage", "100")),
                storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_validate", "true")),
                storageFormat("SEQUENCEFILE"),
                storageFormat("TEXTFILE"),
                storageFormat("TEXTFILE", ImmutableMap.of(), ImmutableMap.of("textfile_field_separator", "F", "textfile_field_separator_escape", "E")),
                storageFormat("AVRO"));
    }

    // ---- Tests ----

    @ParameterizedTest
    @MethodSource("storageFormatsWithConfiguration")
    void testInsertAllSupportedDataTypesWithTrino(StorageFormat storageFormat, HiveStorageFormatsEnvironment env)
            throws SQLException
    {
        // only admin user is allowed to change session properties
        setAdminRole(env);

        try (Connection conn = env.createTrinoConnection()) {
            for (Map.Entry<String, String> sessionProperty : storageFormat.getSessionProperties().entrySet()) {
                setSessionProperty(conn, sessionProperty.getKey(), sessionProperty.getValue());
            }
        }

        String tableName = "storage_formats_compatibility_data_types_" + storageFormat.getName().toLowerCase(ENGLISH);

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));

        boolean isAvroStorageFormat = "AVRO".equals(storageFormat.getName());
        List<HiveCompatibilityColumnData> columnDataList = new ArrayList<>();
        columnDataList.add(new HiveCompatibilityColumnData("c_boolean", "boolean", "true", true));
        if (!isAvroStorageFormat) {
            // The AVRO storage format does not support tinyint and smallint types
            columnDataList.add(new HiveCompatibilityColumnData("c_tinyint", "tinyint", "127", 127));
            columnDataList.add(new HiveCompatibilityColumnData("c_smallint", "smallint", "32767", 32767));
        }
        columnDataList.add(new HiveCompatibilityColumnData("c_int", "integer", "2147483647", 2147483647));
        columnDataList.add(new HiveCompatibilityColumnData("c_bigint", "bigint", "9223372036854775807", 9223372036854775807L));
        columnDataList.add(new HiveCompatibilityColumnData("c_real", "real", "123.345", 123.345d));
        columnDataList.add(new HiveCompatibilityColumnData("c_double", "double", "234.567", 234.567d));
        // Hive expects `FIXED_LEN_BYTE_ARRAY` for decimal values irrespective of the Parquet specification which allows `INT32`, `INT64` for short precision decimal types
        columnDataList.add(new HiveCompatibilityColumnData("c_decimal_10_0", "decimal(10,0)", "346", new BigDecimal("346")));
        columnDataList.add(new HiveCompatibilityColumnData("c_decimal_10_2", "decimal(10,2)", "12345678.91", new BigDecimal("12345678.91")));
        columnDataList.add(new HiveCompatibilityColumnData("c_decimal_38_5", "decimal(38,5)", "1234567890123456789012.34567", new BigDecimal("1234567890123456789012.34567")));
        columnDataList.add(new HiveCompatibilityColumnData("c_char", "char(10)", "'ala ma    '", "ala ma    "));
        columnDataList.add(new HiveCompatibilityColumnData("c_varchar", "varchar(10)", "'ala ma kot'", "ala ma kot"));
        columnDataList.add(new HiveCompatibilityColumnData("c_string", "varchar", "'ala ma kota'", "ala ma kota"));
        columnDataList.add(new HiveCompatibilityColumnData("c_binary", "varbinary", "X'62696e61727920636f6e74656e74'", "binary content".getBytes(StandardCharsets.UTF_8)));
        columnDataList.add(new HiveCompatibilityColumnData("c_date", "date", "DATE '2015-05-10'", Date.valueOf(LocalDate.of(2015, 5, 10))));
        if (isAvroStorageFormat) {
            columnDataList.add(new HiveCompatibilityColumnData(
                    "c_timestamp",
                    "timestamp",
                    "TIMESTAMP '2015-05-10 12:15:35.123'",
                    env.isHiveWithBrokenAvroTimestamps()
                            // TODO (https://github.com/trinodb/trino/issues/1218) requires https://issues.apache.org/jira/browse/HIVE-21002
                            ? Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 6, 30, 35, 123_000_000))
                            : Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000))));
        }
        else {
            // Hive expects `INT96` (deprecated on Parquet) for timestamp values
            columnDataList.add(new HiveCompatibilityColumnData(
                    "c_timestamp",
                    "timestamp",
                    "TIMESTAMP '2015-05-10 12:15:35.123'",
                    Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000))));
        }
        columnDataList.add(new HiveCompatibilityColumnData("c_array", "array(integer)", "ARRAY[1, 2, 3]", "[1,2,3]"));
        columnDataList.add(new HiveCompatibilityColumnData("c_map", "map(varchar, varchar)", "MAP(ARRAY['foo'], ARRAY['bar'])", "{\"foo\":\"bar\"}"));
        columnDataList.add(new HiveCompatibilityColumnData("c_row", "row(f1 integer, f2 varchar)", "ROW(42, 'Trino')", "{\"f1\":42,\"f2\":\"Trino\"}"));

        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (" +
                        "%s" +
                        ") " +
                        "WITH (%s)",
                tableName,
                columnDataList.stream()
                        .map(data -> format("%s %s", data.columnName, data.trinoColumnType))
                        .collect(joining(", ")),
                storageFormat.getStoragePropertiesAsSql()));

        env.executeTrinoUpdate(format(
                "INSERT INTO %s VALUES (%s)",
                tableName,
                columnDataList.stream()
                        .map(data -> data.trinoInsertValue)
                        .collect(joining(", "))));

        // array, map and struct fields are interpreted as strings in the hive jdbc driver and need therefore special handling
        Function<HiveCompatibilityColumnData, Boolean> columnsInterpretedCorrectlyByHiveJdbcDriverPredicate = data ->
                !ImmutableList.of("c_array", "c_map", "c_row").contains(data.columnName);
        QueryResult queryResult = env.executeHive(format(
                "SELECT %s FROM %s",
                columnDataList.stream()
                        .filter(columnsInterpretedCorrectlyByHiveJdbcDriverPredicate::apply)
                        .map(data -> data.columnName)
                        .collect(joining(", ")),
                tableName));
        assertThat(queryResult).containsOnly(Row.fromList(
                columnDataList.stream()
                        .filter(columnsInterpretedCorrectlyByHiveJdbcDriverPredicate::apply)
                        .map(data -> data.hiveJdbcExpectedValue)
                        .collect(toImmutableList())));

        queryResult = env.executeHive(format("SELECT c_array_value FROM %s LATERAL VIEW EXPLODE(c_array) t AS c_array_value", tableName));
        assertThat(queryResult).containsOnly(row(1), row(2), row(3));

        queryResult = env.executeHive(format("SELECT key, c_map[\"foo\"] AS value FROM %s t LATERAL VIEW EXPLODE(map_keys(t.c_map)) keys AS key", tableName));
        assertThat(queryResult).containsOnly(row("foo", "bar"));

        queryResult = env.executeHive(format("SELECT c_row.f1, c_row.f2 FROM %s", tableName));
        assertThat(queryResult).containsOnly(row(42, "Trino"));

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    void testTimestampFieldWrittenByOptimizedParquetWriterCanBeReadByHive(HiveStorageFormatsEnvironment env)
            throws SQLException
    {
        String tableName = "parquet_table_timestamp_created_in_trino";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeTrinoUpdate("CREATE TABLE " + tableName + "(timestamp_precision varchar, a_timestamp timestamp) WITH (format = 'PARQUET')");
        for (HiveTimestampPrecision hiveTimestampPrecision : HiveTimestampPrecision.values()) {
            try (Connection conn = env.createTrinoConnection()) {
                setSessionProperty(conn, "hive.timestamp_precision", hiveTimestampPrecision.name());
            }
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES ('" + hiveTimestampPrecision.name() + "', TIMESTAMP '2021-01-05 12:01:00.111901001')");
            // Hive expects `INT96` (deprecated on Parquet) for timestamp values
            int precisionScalingFactor = (int) Math.pow(10, 9 - hiveTimestampPrecision.getPrecision());
            int nanoOfSecond = IntMath.divide(111901001, precisionScalingFactor, RoundingMode.HALF_UP) * precisionScalingFactor;
            // Apache Hive 3.1 JDBC exposes timestamps at millisecond precision.
            int hiveJdbcNanoOfSecond = IntMath.divide(nanoOfSecond, 1_000_000, RoundingMode.HALF_UP) * 1_000_000;
            LocalDateTime expectedHiveJdbcValue = LocalDateTime.of(2021, 1, 5, 12, 1, 0, hiveJdbcNanoOfSecond);
            assertThat(env.executeHive("SELECT a_timestamp FROM " + tableName + " WHERE timestamp_precision = '" + hiveTimestampPrecision.name() + "'"))
                    .containsOnly(row(Timestamp.valueOf(expectedHiveJdbcValue)));
            // Verify with hive.parquet.timestamp.skip.conversion explicitly enabled
            // Apache Hive 3.2 and above enable this by default
            try {
                env.executeHiveUpdate("SET hive.parquet.timestamp.skip.conversion=true");
                assertThat(env.executeHive("SELECT a_timestamp FROM " + tableName + " WHERE timestamp_precision = '" + hiveTimestampPrecision.name() + "'"))
                        .containsOnly(row(Timestamp.valueOf(expectedHiveJdbcValue)));
            }
            finally {
                env.executeHiveUpdate("RESET");
            }
        }
        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    void testSmallDecimalFieldWrittenByOptimizedParquetWriterCanBeReadByHive(HiveStorageFormatsEnvironment env)
    {
        String tableName = "parquet_table_small_decimal_created_in_trino";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeTrinoUpdate("CREATE TABLE " + tableName + " (a_decimal DECIMAL(5,0)) WITH (format='PARQUET')");
        env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (123)");

        // Hive expects `FIXED_LEN_BYTE_ARRAY` for decimal values irrespective of the Parquet specification which allows `INT32`, `INT64` for short precision decimal types
        assertThat(env.executeHive("SELECT a_decimal FROM " + tableName))
                .containsOnly(row(new BigDecimal("123")));

        env.executeTrinoUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    void testTrinoHiveParquetBloomFilterCompatibility(HiveStorageFormatsEnvironment env)
    {
        String trinoTableNameWithBloomFilter = "test_trino_hive_parquet_bloom_filter_compatibility_enabled_" + randomNameSuffix();
        String trioTableNameNoBloomFilter = "test_trino_hive_parquet_bloom_filter_compatibility_disabled_" + randomNameSuffix();

        env.executeTrinoUpdate(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString VARCHAR, testDouble DOUBLE, testFloat REAL) ", trinoTableNameWithBloomFilter) +
                        "WITH (" +
                        "format = 'PARQUET'," +
                        "parquet_bloom_filter_columns = ARRAY['testInteger', 'testLong', 'testString', 'testDouble', 'testFloat']" +
                        ")");
        env.executeTrinoUpdate(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString VARCHAR, testDouble DOUBLE, testFloat REAL) WITH (FORMAT = 'PARQUET')", trioTableNameNoBloomFilter));
        String[] tables = {trinoTableNameWithBloomFilter, trioTableNameNoBloomFilter};

        for (String trinoTable : tables) {
            env.executeTrinoUpdate(format(
                    "INSERT INTO %s " +
                            "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                            "  (-999999, -999999, 'aaaaaaaaaaa', DOUBLE '-9999999999.99', REAL '-9999999.9999')" +
                            ", (3, 30, 'fdsvxxbv33cb', DOUBLE '97662.2', REAL '98862.2')" +
                            ", (5324, 2466, 'refgfdfrexx', DOUBLE '8796.1', REAL '-65496.1')" +
                            ", (999999, 9999999999999, 'zzzzzzzzzzz', DOUBLE '9999999999.99', REAL '-9999999.9999')" +
                            ", (9444, 4132455, 'ff34322vxff', DOUBLE '32137758.7892', REAL '9978.129887')) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                    trinoTable));
        }

        assertHiveBloomFilterTableSelectResult(tables, env);

        for (String trinoTable : tables) {
            env.executeTrinoUpdate("DROP TABLE " + trinoTable);
        }
    }

    // ---- Helper Methods ----

    private static void assertHiveBloomFilterTableSelectResult(String[] hiveTables, HiveStorageFormatsEnvironment env)
    {
        for (String hiveTable : hiveTables) {
            assertThat(env.executeHive("SELECT COUNT(*) FROM " + hiveTable + " WHERE testInteger IN (9444, -88777, 6711111)")).containsOnly(List.of(row(1)));
            assertThat(env.executeHive("SELECT COUNT(*) FROM " + hiveTable + " WHERE testLong IN (4132455, 321324, 312321321322)")).containsOnly(List.of(row(1)));
            assertThat(env.executeHive("SELECT COUNT(*) FROM " + hiveTable + " WHERE testString IN ('fdsvxxbv33cb', 'cxxx322', 'cxxx323')")).containsOnly(List.of(row(1)));
            assertThat(env.executeHive("SELECT COUNT(*) FROM " + hiveTable + " WHERE testDouble IN (97662.2D, -97221.2D, -88777.22233D)")).containsOnly(List.of(row(1)));
            assertThat(env.executeHive("SELECT COUNT(*) FROM " + hiveTable + " WHERE testFloat IN (-65496.1, 98211862.2, 6761111555.1222)")).containsOnly(List.of(row(1)));
        }
    }

    private void setAdminRole(HiveStorageFormatsEnvironment env)
    {
        try (Connection connection = env.createTrinoConnection()) {
            try {
                setRole(connection, "admin");
            }
            catch (SQLException _) {
                // The test environments do not properly setup or manage
                // roles, so try to set the role, but ignore any errors
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setSessionProperty(Connection connection, String key, String value)
            throws SQLException
    {
        @SuppressWarnings("resource")
        TrinoConnection trinoConnection = connection.unwrap(TrinoConnection.class);
        trinoConnection.setSessionProperty(key, value);
    }

    private static void setRole(Connection connection, String role)
            throws SQLException
    {
        connection.createStatement().execute("SET ROLE " + role);
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(
            String name,
            Map<String, String> sessionProperties,
            Map<String, String> properties)
    {
        return new StorageFormat(name, sessionProperties, properties);
    }

    // ---- Inner Classes ----

    public static class StorageFormat
    {
        private final String name;
        private final Map<String, String> properties;
        private final Map<String, String> sessionProperties;

        private StorageFormat(
                String name,
                Map<String, String> sessionProperties,
                Map<String, String> properties)
        {
            this.name = requireNonNull(name, "name is null");
            this.properties = requireNonNull(properties, "properties is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public String getStoragePropertiesAsSql()
        {
            return Stream.concat(
                    Stream.of(immutableEntry("format", name)),
                    properties.entrySet().stream())
                    .map(entry -> format("%s = '%s'", entry.getKey(), entry.getValue()))
                    .collect(joining(", "));
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("properties", properties)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }

    private static class HiveCompatibilityColumnData
    {
        private final String columnName;
        private final String trinoColumnType;
        private final String trinoInsertValue;
        private final Object hiveJdbcExpectedValue;

        public HiveCompatibilityColumnData(String columnName, String trinoColumnType, String trinoInsertValue, Object hiveJdbcExpectedValue)
        {
            this.columnName = columnName;
            this.trinoColumnType = trinoColumnType;
            this.trinoInsertValue = trinoInsertValue;
            this.hiveJdbcExpectedValue = hiveJdbcExpectedValue;
        }
    }
}
