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
import com.google.common.math.IntMath;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.utils.JdbcDriverUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS_DETAILED;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveCompatibility
        extends HiveProductTest
{
    @Inject(optional = true)
    @Named("databases.trino.admin_role_enabled")
    private boolean adminRoleEnabled;

    @Test(dataProvider = "storageFormatsWithConfiguration", groups = STORAGE_FORMATS_DETAILED)
    public void testInsertAllSupportedDataTypesWithTrino(TestHiveStorageFormats.StorageFormat storageFormat)
            throws SQLException
    {
        // only admin user is allowed to change session properties
        setAdminRole(onTrino().getConnection());

        for (Map.Entry<String, String> sessionProperty : storageFormat.getSessionProperties().entrySet()) {
            setSessionProperty(onTrino().getConnection(), sessionProperty.getKey(), sessionProperty.getValue());
        }

        String tableName = "storage_formats_compatibility_data_types_" + storageFormat.getName().toLowerCase(ENGLISH);

        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));

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
                    isHiveWithBrokenAvroTimestamps()
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

        onTrino().executeQuery(format(
                "CREATE TABLE %s (" +
                        "%s" +
                        ") " +
                        "WITH (%s)",
                tableName,
                columnDataList.stream()
                        .map(data -> format("%s %s", data.columnName, data.trinoColumnType))
                        .collect(joining(", ")),
                storageFormat.getStoragePropertiesAsSql()));

        onTrino().executeQuery(format(
                "INSERT INTO %s VALUES (%s)",
                tableName,
                columnDataList.stream()
                        .map(data -> data.trinoInsertValue)
                        .collect(joining(", "))));

        // array, map and struct fields are interpreted as strings in the hive jdbc driver and need therefore special handling
        Function<HiveCompatibilityColumnData, Boolean> columnsInterpretedCorrectlyByHiveJdbcDriverPredicate = data ->
                !ImmutableList.of("c_array", "c_map", "c_row").contains(data.columnName);
        QueryResult queryResult = onHive().executeQuery(format(
                "SELECT %s FROM %s",
                columnDataList.stream()
                        .filter(columnsInterpretedCorrectlyByHiveJdbcDriverPredicate::apply)
                        .map(data -> data.columnName)
                        .collect(joining(", ")),
                tableName));
        assertThat(queryResult).containsOnly(new QueryAssert.Row(
                columnDataList.stream()
                        .filter(columnsInterpretedCorrectlyByHiveJdbcDriverPredicate::apply)
                        .map(data -> data.hiveJdbcExpectedValue)
                        .collect(toImmutableList())));

        queryResult = onHive().executeQuery(format("SELECT c_array_value FROM %s LATERAL VIEW EXPLODE(c_array) t AS c_array_value", tableName));
        assertThat(queryResult).containsOnly(row(1), row(2), row(3));

        queryResult = onHive().executeQuery(format("SELECT key, c_map[\"foo\"] AS value FROM %s t LATERAL VIEW EXPLODE(map_keys(t.c_map)) keys AS key", tableName));
        assertThat(queryResult).containsOnly(row("foo", "bar"));

        queryResult = onHive().executeQuery(format("SELECT c_row.f1, c_row.f2 FROM %s", tableName));
        assertThat(queryResult).containsOnly(row(42, "Trino"));

        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS_DETAILED)
    public void testTimestampFieldWrittenByOptimizedParquetWriterCanBeReadByHive()
            throws Exception
    {
        String tableName = "parquet_table_timestamp_created_in_trino";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("CREATE TABLE " + tableName + "(timestamp_precision varchar, a_timestamp timestamp) WITH (format = 'PARQUET')");
        for (HiveTimestampPrecision hiveTimestampPrecision : HiveTimestampPrecision.values()) {
            setSessionProperty(onTrino().getConnection(), "hive.timestamp_precision", hiveTimestampPrecision.name());
            onTrino().executeQuery("INSERT INTO " + tableName + " VALUES ('" + hiveTimestampPrecision.name() + "', TIMESTAMP '2021-01-05 12:01:00.111901001')");
            // Hive expects `INT96` (deprecated on Parquet) for timestamp values
            int precisionScalingFactor = (int) Math.pow(10, 9 - hiveTimestampPrecision.getPrecision());
            int nanoOfSecond = IntMath.divide(111901001, precisionScalingFactor, RoundingMode.HALF_UP) * precisionScalingFactor;
            LocalDateTime expectedValue = LocalDateTime.of(2021, 1, 5, 12, 1, 0, nanoOfSecond);
            assertThat(onHive().executeQuery("SELECT a_timestamp FROM " + tableName + " WHERE timestamp_precision = '" + hiveTimestampPrecision.name() + "'"))
                    .containsOnly(row(Timestamp.valueOf(expectedValue)));
            // Verify with hive.parquet.timestamp.skip.conversion explicitly enabled
            // Apache Hive 3.2 and above enable this by default
            try {
                onHive().executeQuery("SET hive.parquet.timestamp.skip.conversion=true");
                assertThat(onHive().executeQuery("SELECT a_timestamp FROM " + tableName + " WHERE timestamp_precision = '" + hiveTimestampPrecision.name() + "'"))
                        .containsOnly(row(Timestamp.valueOf(expectedValue)));
            }
            finally {
                onHive().executeQuery("RESET");
            }
        }
        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS_DETAILED)
    public void testSmallDecimalFieldWrittenByOptimizedParquetWriterCanBeReadByHive()
            throws Exception
    {
        String tableName = "parquet_table_small_decimal_created_in_trino";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("CREATE TABLE " + tableName + " (a_decimal DECIMAL(5,0)) WITH (format='PARQUET')");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (123)");

        // Hive expects `FIXED_LEN_BYTE_ARRAY` for decimal values irrespective of the Parquet specification which allows `INT32`, `INT64` for short precision decimal types
        assertThat(onHive().executeQuery("SELECT a_decimal FROM " + tableName))
                .containsOnly(row(new BigDecimal("123")));

        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS_DETAILED)
    public void testTrinoHiveParquetBloomFilterCompatibility()
    {
        String trinoTableNameWithBloomFilter = "test_trino_hive_parquet_bloom_filter_compatibility_enabled_" + randomNameSuffix();
        String trioTableNameNoBloomFilter = "test_trino_hive_parquet_bloom_filter_compatibility_disabled_" + randomNameSuffix();

        onTrino().executeQuery(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString VARCHAR, testDouble DOUBLE, testFloat REAL) ", trinoTableNameWithBloomFilter) +
                        "WITH (" +
                        "format = 'PARQUET'," +
                        "parquet_bloom_filter_columns = ARRAY['testInteger', 'testLong', 'testString', 'testDouble', 'testFloat']" +
                        ")");
        onTrino().executeQuery(
                String.format("CREATE TABLE %s (testInteger INTEGER, testLong BIGINT, testString VARCHAR, testDouble DOUBLE, testFloat REAL) WITH (FORMAT = 'PARQUET')", trioTableNameNoBloomFilter));
        String[] tables = new String[] {trinoTableNameWithBloomFilter, trioTableNameNoBloomFilter};

        for (String trinoTable : tables) {
            onTrino().executeQuery(format(
                    "INSERT INTO %s " +
                            "SELECT testInteger, testLong, testString, testDouble, testFloat FROM (VALUES " +
                            "  (-999999, -999999, 'aaaaaaaaaaa', DOUBLE '-9999999999.99', REAL '-9999999.9999')" +
                            ", (3, 30, 'fdsvxxbv33cb', DOUBLE '97662.2', REAL '98862.2')" +
                            ", (5324, 2466, 'refgfdfrexx', DOUBLE '8796.1', REAL '-65496.1')" +
                            ", (999999, 9999999999999, 'zzzzzzzzzzz', DOUBLE '9999999999.99', REAL '-9999999.9999')" +
                            ", (9444, 4132455, 'ff34322vxff', DOUBLE '32137758.7892', REAL '9978.129887')) AS DATA(testInteger, testLong, testString, testDouble, testFloat)",
                    trinoTable));
        }

        assertHiveBloomFilterTableSelectResult(tables);

        for (String trinoTable : tables) {
            onTrino().executeQuery("DROP TABLE " + trinoTable);
        }
    }

    private static void assertHiveBloomFilterTableSelectResult(String[] hiveTables)
    {
        for (String hiveTable : hiveTables) {
            assertThat(onHive().executeQuery("SELECT COUNT(*) FROM " + hiveTable + " WHERE testInteger IN (9444, -88777, 6711111)")).containsOnly(List.of(row(1)));
            assertThat(onHive().executeQuery("SELECT COUNT(*) FROM " + hiveTable + " WHERE testLong IN (4132455, 321324, 312321321322)")).containsOnly(List.of(row(1)));
            assertThat(onHive().executeQuery("SELECT COUNT(*) FROM " + hiveTable + " WHERE testString IN ('fdsvxxbv33cb', 'cxxx322', 'cxxx323')")).containsOnly(List.of(row(1)));
            assertThat(onHive().executeQuery("SELECT COUNT(*) FROM " + hiveTable + " WHERE testDouble IN (97662.2D, -97221.2D, -88777.22233D)")).containsOnly(List.of(row(1)));
            assertThat(onHive().executeQuery("SELECT COUNT(*) FROM " + hiveTable + " WHERE testFloat IN (-65496.1, 98211862.2, 6761111555.1222)")).containsOnly(List.of(row(1)));
        }
    }

    @DataProvider
    public static TestHiveStorageFormats.StorageFormat[] storageFormatsWithConfiguration()
    {
        return TestHiveStorageFormats.storageFormatsWithConfiguration();
    }

    private void setAdminRole(Connection connection)
    {
        if (adminRoleEnabled) {
            return;
        }

        try {
            JdbcDriverUtils.setRole(connection, "admin");
        }
        catch (SQLException _) {
            // The test environments do not properly setup or manage
            // roles, so try to set the role, but ignore any errors
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
