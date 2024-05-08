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
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeSystemTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTablePropertiesCaseSensitivity()
    {
        String tableName = "test_dl_table_properties_case_sensitivity_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("CREATE TABLE default.%s (col INT) USING DELTA LOCATION 's3://%s/%s' TBLPROPERTIES ('test_key'='test_value', 'Test_Key'='Test_Mixed_Case')",
                tableName,
                bucketName,
                tableDirectory));
        List<Row> expectedRows = ImmutableList.of(
                row("test_key", "test_value"),
                row("Test_Key", "Test_Mixed_Case"));
        try {
            QueryResult deltaResult = onDelta().executeQuery("SHOW TBLPROPERTIES default." + tableName);
            QueryResult trinoResult = onTrino().executeQuery("SELECT * FROM default.\"" + tableName + "$properties\"");
            assertThat(deltaResult).contains(expectedRows);
            assertThat(trinoResult).contains(expectedRows);
            assertThat(trinoResult.rows()).containsExactlyInAnyOrderElementsOf(deltaResult.rows());
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTablePropertiesWithTableFeatures()
    {
        String tableName = "test_dl_table_properties_with_features_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("CREATE TABLE default.%s (col INT) USING DELTA LOCATION 's3://%s/%s'" +
                        " TBLPROPERTIES ('delta.minReaderVersion'='3', 'delta.minWriterVersion'='7', 'delta.columnMapping.mode'='id')",
                tableName,
                bucketName,
                tableDirectory));

        List<Row> expectedRows = ImmutableList.of(
                row("delta.columnMapping.mode", "id"),
                row("delta.feature.columnMapping", "supported"),
                row("delta.minReaderVersion", "3"),
                row("delta.minWriterVersion", "7"));
        try {
            QueryResult deltaResult = onDelta().executeQuery("SHOW TBLPROPERTIES default." + tableName);
            QueryResult trinoResult = onTrino().executeQuery("SELECT * FROM default.\"" + tableName + "$properties\"");
            assertThat(deltaResult).contains(expectedRows);
            assertThat(trinoResult).contains(expectedRows);
            assertThat(trinoResult.rows()).containsExactlyInAnyOrderElementsOf(deltaResult.rows());
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTablePartitionsWithDeletionVectors()
    {
        String tableName = "test_table_partitions_with_deletion_vectors_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT, c VARCHAR(10)) " +
                "USING delta " +
                "PARTITIONED BY (a, c) " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");

        List<Row> expectedBeforeDelete = ImmutableList.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 3L, 1347L, "{\"b\":{\"min\":11,\"max\":19,\"null_count\":0}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 449L, "{\"b\":{\"min\":13,\"max\":13,\"null_count\":0}}"));

        List<Row> expectedAfterFirstDelete = ImmutableList.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 2L, 898L, "{\"b\":{\"min\":11,\"max\":17,\"null_count\":0}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 449L, "{\"b\":{\"min\":13,\"max\":13,\"null_count\":0}}"));

        List<Row> expectedAfterSecondDelete = ImmutableList.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 2L, 898L, "{\"b\":{\"min\":11,\"max\":17,\"null_count\":0}}"));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11, 'varchar_1'), (1, 19, 'varchar_1'), (1, 17, 'varchar_1'), (1, 13, 'varchar_2')");
            assertThat(onTrino().executeQuery(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedBeforeDelete);
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE b = 19");
            assertThat(onTrino().executeQuery(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedAfterFirstDelete);
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE c = 'varchar_2'");
            assertThat(onTrino().executeQuery(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedAfterSecondDelete);
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTablePartitionsWithNoColumnStats()
    {
        String tableName = "test_table_partitions_with_no_column_stats_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT, c VARCHAR(10)) " +
                "USING delta " +
                "PARTITIONED BY (a, c) " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 0)");

        List<Row> expected = ImmutableList.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 3L, 1347L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 449L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"),
                row("{\"a\":1,\"c\":\"varchar_3\"}", 1L, 413L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11, 'varchar_1'), (1, 19, 'varchar_1'), (1, 17, 'varchar_1'), (1, 13, 'varchar_2'), (1, NULL, 'varchar_3')");
            assertThat(onTrino().executeQuery(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expected);
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
