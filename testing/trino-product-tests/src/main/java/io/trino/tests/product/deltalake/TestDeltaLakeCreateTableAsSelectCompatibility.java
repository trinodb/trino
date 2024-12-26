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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_122;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_133;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_143;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeCreateTableAsSelectCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_113, DELTA_LAKE_DATABRICKS_122, DELTA_LAKE_DATABRICKS_133, DELTA_LAKE_DATABRICKS_143, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoTypesWithDatabricks()
    {
        String tableName = "test_dl_ctas_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, boolean, tinyint, smallint, integer, bigint, real, double, short_decimal, long_decimal, varchar_25, varchar, date) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, BOOLEAN 'false', TINYINT '-128', SMALLINT '-32768', INTEGER '-2147483648', BIGINT '-9223372036854775808', REAL '-0x1.fffffeP+127f', DOUBLE '-0x1.fffffffffffffP+1023', CAST('-123456789.123456789' AS DECIMAL(18,9)), CAST('-12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '1901-01-01')" +
                    ", (2, BOOLEAN 'true', TINYINT '127', SMALLINT '32767', INTEGER '2147483647', BIGINT '9223372036854775807', REAL '0x1.fffffeP+127f', DOUBLE '0x1.fffffffffffffP+1023', CAST('123456789.123456789' AS DECIMAL(18,9)), CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Canada' AS VARCHAR(25)), CAST('Germany' AS VARCHAR), DATE '9999-12-31')" +
                    ", (3, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0', DOUBLE '0', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('' AS VARCHAR(25)), CAST('' AS VARCHAR), DATE '2020-08-22')" +
                    ", (4, BOOLEAN 'true', TINYINT '1', SMALLINT '1', INTEGER '1', BIGINT '1', REAL '1', DOUBLE '1', CAST('1' AS DECIMAL(18,9)), CAST('1' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (5, BOOLEAN 'true', TINYINT '37', SMALLINT '12242', INTEGER '2524', BIGINT '132', REAL '3.141592653', DOUBLE '2.718281828459045', CAST('3.141592653' AS DECIMAL(18,9)), CAST('2.718281828459045' AS DECIMAL(38,18)), CAST('\u653b\u6bbb\u6a5f\u52d5\u968a' AS VARCHAR(25)), CAST('\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!' AS VARCHAR), DATE '2020-08-22')" +
                    ", (6, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x0.000002P-126f', DOUBLE '0x0.0000000000001P-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (7, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x1.0p-126f', DOUBLE '0x1.0p-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')"))
                    .containsOnly(row(7));

            QueryResult databricksResult = onDelta().executeQuery(format("SELECT * FROM default.%s", tableName));
            QueryResult trinoResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            assertThat(databricksResult).containsOnly(trinoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoTimestampsWithDatabricks()
    {
        String tableName = "test_dl_ctas_timestamps_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, timestamp_in_utc, timestamp_in_new_york, timestamp_in_warsaw) " +
                    "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, TIMESTAMP '1901-01-01 00:00:00 UTC', TIMESTAMP '1902-01-01 00:00:00 America/New_York', TIMESTAMP '1902-01-01 00:00:00 Europe/Warsaw')" +
                    ", (2, TIMESTAMP '9999-12-31 23:59:59.999 UTC', TIMESTAMP '9998-12-31 23:59:59.999 America/New_York', TIMESTAMP '9998-12-31 23:59:59.999 Europe/Warsaw')" +
                    ", (3, TIMESTAMP '2020-06-10 15:55:23 UTC', TIMESTAMP '2020-06-10 15:55:23.123 America/New_York', TIMESTAMP '2020-06-10 15:55:23.123 Europe/Warsaw')" +
                    ", (4, TIMESTAMP '2001-08-22 03:04:05.321 UTC', TIMESTAMP '2001-08-22 03:04:05.321 America/New_York', TIMESTAMP '2001-08-22 03:04:05.321 Europe/Warsaw')"))
                    .containsOnly(row(4));

            QueryResult databricksResult = onDelta().executeQuery("SELECT id, date_format(timestamp_in_utc, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_new_york, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_warsaw, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName);
            QueryResult trinoResult = onTrino().executeQuery("SELECT id, format('%1$tF %1$tT.%1$tL', timestamp_in_utc), format('%1$tF %1$tT.%1$tL', timestamp_in_new_york), format('%1$tF %1$tT.%1$tL', timestamp_in_warsaw) FROM delta.default.\"" + tableName + "\"");
            assertThat(databricksResult).containsOnly(trinoResult.rows().stream()
                    .map(QueryAssert.Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateFromTrinoWithDefaultPartitionValues()
    {
        String tableName = "test_create_partitioned_table_default_as_" + randomNameSuffix();

        try {
            assertThat(onTrino().executeQuery(
                    "CREATE TABLE delta.default." + tableName + "(number_partition, string_partition, a_value) " +
                            "WITH (" +
                            "   location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "', " +
                            "   partitioned_by = ARRAY['number_partition', 'string_partition']) " +
                            "AS VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')"))
                    .containsOnly(row(3));

            List<QueryAssert.Row> expectedRows = ImmutableList.of(
                    row(null, "partition_a", "jarmuz"),
                    row(1, null, "brukselka"),
                    row(null, null, "kalafior"));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableWithUnsupportedPartitionType()
    {
        String tableName = "test_dl_ctas_unsupported_column_types_" + randomNameSuffix();
        String tableLocation = "s3://%s/databricks-compatibility-test-%s".formatted(bucketName, tableName);
        try {
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, array[1] part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, map() part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, row(1) part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");

            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, array(1) part"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, map() part"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, named_struct('x', 1) part"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelectWithAllPartitionColumns()
    {
        String tableName = "test_dl_ctas_with_all_partition_columns_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + " " +
                    "WITH (partitioned_by = ARRAY['part'], location = 's3://" + bucketName + "/" + tableDirectory + "')" +
                    "AS SELECT 1 part"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + " " +
                    "WITH (partitioned_by = ARRAY['part', 'another_part'], location = 's3://" + bucketName + "/" + tableDirectory + "')" +
                    "AS SELECT 1 part, 2 another_part"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");

            assertThatThrownBy(() -> onDelta().executeQuery("" +
                    "CREATE TABLE default." + tableName + " " +
                    "USING DELTA " +
                    "PARTITIONED BY (part)" +
                    "LOCATION 's3://" + bucketName + "/" + tableDirectory + "'" +
                    "AS SELECT 1 part"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
            assertThatThrownBy(() -> onDelta().executeQuery("" +
                    "CREATE TABLE default." + tableName + " " +
                    "USING DELTA " +
                    "PARTITIONED BY (part, another_part)" +
                    "LOCATION 's3://" + bucketName + "/" + tableDirectory + "'" +
                    "SELECT 1 part, 2 another_part"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
