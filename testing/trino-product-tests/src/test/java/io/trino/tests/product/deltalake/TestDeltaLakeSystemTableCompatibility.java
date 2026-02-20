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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Delta Lake system table compatibility between Trino and Spark.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeSystemTableCompatibility
{
    @Test
    void testTablePropertiesCaseSensitivity(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_table_properties_case_sensitivity_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("CREATE TABLE default.%s (col INT) USING DELTA LOCATION 's3://%s/%s' TBLPROPERTIES ('test_key'='test_value', 'Test_Key'='Test_Mixed_Case')",
                tableName,
                env.getBucketName(),
                tableDirectory));
        List<Row> expectedRows = List.of(
                row("test_key", "test_value"),
                row("Test_Key", "Test_Mixed_Case"));
        try {
            QueryResult deltaResult = env.executeSpark("SHOW TBLPROPERTIES default." + tableName);
            QueryResult trinoResult = env.executeTrino("SELECT * FROM delta.default.\"" + tableName + "$properties\"");
            assertThat(deltaResult).contains(expectedRows);
            assertThat(trinoResult).contains(expectedRows);
            assertThat(trinoResult.rows()).containsExactlyInAnyOrderElementsOf(deltaResult.rows());
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTablePropertiesWithTableFeatures(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_table_properties_with_features_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("CREATE TABLE default.%s (col INT) USING DELTA LOCATION 's3://%s/%s'" +
                        " TBLPROPERTIES ('delta.minReaderVersion'='3', 'delta.minWriterVersion'='7', 'delta.columnMapping.mode'='id')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        List<Row> expectedRows = List.of(
                row("delta.columnMapping.mode", "id"),
                row("delta.feature.columnMapping", "supported"),
                row("delta.minReaderVersion", "2"), // https://github.com/delta-io/delta/issues/4024 Delta Lake 3.3.0 ignores minReaderVersion
                row("delta.minWriterVersion", "7"));
        try {
            QueryResult deltaResult = env.executeSpark("SHOW TBLPROPERTIES default." + tableName);
            QueryResult trinoResult = env.executeTrino("SELECT * FROM delta.default.\"" + tableName + "$properties\"");
            assertThat(deltaResult).contains(expectedRows);
            assertThat(trinoResult).contains(expectedRows);
            assertThat(trinoResult.rows()).containsExactlyInAnyOrderElementsOf(deltaResult.rows());
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTablePartitionsWithDeletionVectors(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_table_partitions_with_deletion_vectors_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT, c VARCHAR(10)) " +
                "USING delta " +
                "PARTITIONED BY (a, c) " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");

        List<Row> expectedBeforeDelete = List.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 3L, 1398L, "{\"b\":{\"min\":11,\"max\":19,\"null_count\":0}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 466L, "{\"b\":{\"min\":13,\"max\":13,\"null_count\":0}}"));

        List<Row> expectedAfterFirstDelete = List.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 2L, 932L, "{\"b\":{\"min\":11,\"max\":17,\"null_count\":0}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 466L, "{\"b\":{\"min\":13,\"max\":13,\"null_count\":0}}"));

        List<Row> expectedAfterSecondDelete = List.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 2L, 932L, "{\"b\":{\"min\":11,\"max\":17,\"null_count\":0}}"));

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 11, 'varchar_1'), (1, 19, 'varchar_1'), (1, 17, 'varchar_1'), (1, 13, 'varchar_2')");
            assertThat(env.executeTrino(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedBeforeDelete);
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE b = 19");
            assertThat(env.executeTrino(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedAfterFirstDelete);
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE c = 'varchar_2'");
            assertThat(env.executeTrino(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expectedAfterSecondDelete);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTablePartitionsWithNoColumnStats(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_table_partitions_with_no_column_stats_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT, c VARCHAR(10)) " +
                "USING delta " +
                "PARTITIONED BY (a, c) " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 0)");

        List<Row> expected = List.of(
                row("{\"a\":1,\"c\":\"varchar_1\"}", 3L, 1398L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"),
                row("{\"a\":1,\"c\":\"varchar_2\"}", 1L, 466L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"),
                row("{\"a\":1,\"c\":\"varchar_3\"}", 1L, 429L, "{\"b\":{\"min\":null,\"max\":null,\"null_count\":null}}"));

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 11, 'varchar_1'), (1, 19, 'varchar_1'), (1, 17, 'varchar_1'), (1, 13, 'varchar_2'), (1, NULL, 'varchar_3')");
            assertThat(env.executeTrino(format("SELECT cast(partition as json), file_count, total_size, cast(data as json) FROM delta.default.\"%s$partitions\"", tableName))).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
