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
import com.google.inject.Inject;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.utils.JdbcDriverUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Named;

import java.math.BigDecimal;
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
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS_DETAILED;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class TestHiveCompatibility
        extends HiveProductTest
{
    @Inject(optional = true)
    @Named("databases.presto.admin_role_enabled")
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
        boolean isParquetStorageFormat = "PARQUET".equals(storageFormat.getName());
        boolean isParquetOptimizedWriterEnabled = Boolean.parseBoolean(
                storageFormat.getSessionProperties().getOrDefault("hive.experimental_parquet_optimized_writer_enabled", "false"));
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
        if (!(isParquetStorageFormat && isParquetOptimizedWriterEnabled)) {
            // Hive expects `FIXED_LEN_BYTE_ARRAY` for decimal values irrespective of the Parquet specification which allows `INT32`, `INT64` for short precision decimal types
            columnDataList.add(new HiveCompatibilityColumnData("c_decimal_10_0", "decimal(10,0)", "346", new BigDecimal("346")));
            columnDataList.add(new HiveCompatibilityColumnData("c_decimal_10_2", "decimal(10,2)", "12345678.91", new BigDecimal("12345678.91")));
        }
        columnDataList.add(new HiveCompatibilityColumnData("c_decimal_38_5", "decimal(38,5)", "1234567890123456789012.34567", new BigDecimal("1234567890123456789012.34567")));
        columnDataList.add(new HiveCompatibilityColumnData("c_char", "char(10)", "'ala ma    '", "ala ma    "));
        columnDataList.add(new HiveCompatibilityColumnData("c_varchar", "varchar(10)", "'ala ma kot'", "ala ma kot"));
        columnDataList.add(new HiveCompatibilityColumnData("c_string", "varchar", "'ala ma kota'", "ala ma kota"));
        columnDataList.add(new HiveCompatibilityColumnData("c_binary", "varbinary", "X'62696e61727920636f6e74656e74'", "binary content".getBytes(StandardCharsets.UTF_8)));
        if (!(isParquetStorageFormat && isHiveVersionBefore12())) {
            // The PARQUET storage format does not support DATE type in CDH5 distribution
            columnDataList.add(new HiveCompatibilityColumnData("c_date", "date", "DATE '2015-05-10'", Date.valueOf(LocalDate.of(2015, 5, 10))));
        }
        if (isAvroStorageFormat) {
            if (!isHiveVersionBefore12()) {
                // The AVRO storage format does not support TIMESTAMP type in CDH5 distribution
                columnDataList.add(new HiveCompatibilityColumnData(
                        "c_timestamp",
                        "timestamp",
                        "TIMESTAMP '2015-05-10 12:15:35.123'",
                        isHiveWithBrokenAvroTimestamps()
                                // TODO (https://github.com/trinodb/trino/issues/1218) requires https://issues.apache.org/jira/browse/HIVE-21002
                                ? Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 6, 30, 35, 123_000_000))
                                : Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000))));
            }
        }
        else if (!(isParquetStorageFormat && isParquetOptimizedWriterEnabled)) {
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
    public void testTimestampFieldWrittenByOptimizedParquetWriterCannotBeReadByHive()
            throws Exception
    {
        // only admin user is allowed to change session properties
        setAdminRole(onTrino().getConnection());
        setSessionProperty(onTrino().getConnection(), "hive.experimental_parquet_optimized_writer_enabled", "true");

        String tableName = "parquet_table_timestamp_created_in_trino";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("CREATE TABLE " + tableName + "(timestamp_precision varchar, a_timestamp timestamp) WITH (format = 'PARQUET')");
        for (HiveTimestampPrecision hiveTimestampPrecision : HiveTimestampPrecision.values()) {
            setSessionProperty(onTrino().getConnection(), "hive.timestamp_precision", hiveTimestampPrecision.name());
            onTrino().executeQuery("INSERT INTO " + tableName + " VALUES ('" + hiveTimestampPrecision.name() + "', TIMESTAMP '2021-01-05 12:01:00.111901001')");
            // Hive expects `INT96` (deprecated on Parquet) for timestamp values
            assertQueryFailure(() -> onHive().executeQuery("SELECT a_timestamp FROM " + tableName + " WHERE timestamp_precision = '" + hiveTimestampPrecision.name() + "'"))
                    .hasMessageMatching(".*java.lang.ClassCastException: org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.(TimestampWritable|TimestampWritableV2)");
        }
        onTrino().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS_DETAILED)
    public void testSmallDecimalFieldWrittenByOptimizedParquetWriterCannotBeReadByHive()
            throws Exception
    {
        // only admin user is allowed to change session properties
        setAdminRole(onTrino().getConnection());
        setSessionProperty(onTrino().getConnection(), "hive.experimental_parquet_optimized_writer_enabled", "true");

        String tableName = "parquet_table_small_decimal_created_in_trino";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("CREATE TABLE " + tableName + " (a_decimal DECIMAL(5,0)) WITH (format='PARQUET')");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (123)");

        // Hive expects `FIXED_LEN_BYTE_ARRAY` for decimal values irrespective of the Parquet specification which allows `INT32`, `INT64` for short precision decimal types
        assertQueryFailure(() -> onHive().executeQuery("SELECT a_decimal FROM " + tableName))
                .hasMessageMatching(".*ParquetDecodingException: Can not read value at 1 in block 0 in file .*");

        onTrino().executeQuery(format("DROP TABLE %s", tableName));
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
        catch (SQLException ignored) {
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
