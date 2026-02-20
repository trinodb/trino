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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Delta Lake Change Data Feed (CDF) compatibility tests.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeChangeDataFeedCompatibility
{
    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testUpdateTableWithCdf(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_to_table_with_cdf_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (col1 VARCHAR, updated_column INT) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', " +
                    "change_data_feed_enabled = true, column_mapping_mode = '" + columnMappingMode + "')");

            assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString()).contains("change_data_feed_enabled = true");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue2', 2)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue3', 3)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName +
                    " SET updated_column = 5 WHERE col1 = 'testValue3'");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName +
                    " SET updated_column = 4, col1 = 'testValue4' WHERE col1 = 'testValue3'");

            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue2", 2, "insert", 2L),
                            row("testValue3", 3, "insert", 3L),
                            row("testValue3", 3, "update_preimage", 4L),
                            row("testValue3", 5, "update_postimage", 4L),
                            row("testValue3", 5, "update_preimage", 5L),
                            row("testValue4", 4, "update_postimage", 5L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    @Test
    void testUpdateTableWithChangeDataFeedWriterFeature(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_change_data_feed_writer_feature_" + randomNameSuffix();
        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(col1 STRING, updated_column INT)" +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.enableChangeDataFeed'=true, 'delta.minWriterVersion'=7)");
        try {
            assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString()).contains("change_data_feed_enabled = true");

            // TODO https://github.com/trinodb/trino/issues/23620 Fix incorrect CDC entry when deletion vector is enabled
            Map<String, String> properties = getTablePropertiesOnSpark(env, "default", tableName);
            if (properties.getOrDefault("delta.enableChangeDataFeed", "false").equals("true") &&
                    properties.getOrDefault("delta.enableDeletionVectors", "false").equals("true")) {
                return;
            }

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1), ('testValue2', 2), ('testValue3', 3)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 30 WHERE col1 = 'testValue3'");

            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue2", 2, "insert", 1L),
                            row("testValue3", 3, "insert", 1L),
                            row("testValue3", 3, "update_preimage", 2L),
                            row("testValue3", 30, "update_postimage", 2L));

            // CDF shouldn't be generated when delta.feature.changeDataFeed exists, but delta.enableChangeDataFeed doesn't exist
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.enableChangeDataFeed')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.changeDataFeed", "supported"))
                    .doesNotContainKey("delta.enableChangeDataFeed");
            assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString()).doesNotContain("change_data_feed_enabled");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue4', 4)");
            assertThatThrownBy(() -> env.executeSpark("SELECT * FROM table_changes('default." + tableName + "', 4)"))
                    .hasStackTraceContaining("Error getting change data for range [4 , 4] as change data was not");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue5', 5)");
            assertThatThrownBy(() -> env.executeSpark("SELECT * FROM table_changes('default." + tableName + "', 5)"))
                    .hasStackTraceContaining("Error getting change data for range [5 , 5] as change data was not");

            // CDF shouldn't be generated when delta.feature.changeDataFeed exists, but delta.enableChangeDataFeed is disabled
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.feature.changeDataFeed'='supported', 'delta.enableChangeDataFeed'=false)");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.changeDataFeed", "supported"))
                    .contains(entry("delta.enableChangeDataFeed", "false"));
            assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString()).contains("change_data_feed_enabled = false");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue7', 7)");
            assertThatThrownBy(() -> env.executeSpark("SELECT * FROM table_changes('default." + tableName + "', 7)"))
                    .hasStackTraceContaining("Error getting change data for range [7 , 7] as change data was not");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue8', 8)");
            assertThatThrownBy(() -> env.executeSpark("SELECT * FROM table_changes('default." + tableName + "', 8)"))
                    .hasStackTraceContaining("Error getting change data for range [8 , 8] as change data was not");

            // Enabling only delta.enableChangeDataFeed without delta.feature.changeDataFeed property is unsupported
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testUpdateCdfTableWithNonLowercaseColumn(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_cdf_with_non_lowercase_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(col1 string, Updated_Column int)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.enableChangeDataFeed' = true, 'delta.columnMapping.mode'='" + columnMappingMode + "')");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue1', 1), ('testValue2', 2), ('testValue3', 3)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 5 WHERE col1 = 'testValue3'");

            List<Row> expectedRows = ImmutableList.<Row>builder()
                    .add(row("testValue1", 1, "insert", 1L))
                    .add(row("testValue2", 2, "insert", 1L))
                    .add(row("testValue3", 3, "insert", 1L))
                    .add(row("testValue3", 3, "update_preimage", 2L))
                    .add(row("testValue3", 5, "update_postimage", 2L))
                    .build();
            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM TABLE(delta.system.table_changes('default', '" + tableName + "'))"))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testUpdatePartitionedTableWithCdf(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_to_partitioned_table_with_cdf_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (updated_column STRING, partitioning_column_1 INT, partitioning_column_2 STRING) " +
                    "USING DELTA " +
                    "PARTITIONED BY (partitioning_column_1, partitioning_column_2) " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.columnMapping.mode'='" + columnMappingMode + "')");

            // TODO https://github.com/trinodb/trino/issues/23620 Fix incorrect CDC entry when deletion vector is enabled
            Map<String, String> properties = getTablePropertiesOnSpark(env, "default", tableName);
            if (properties.getOrDefault("delta.enableChangeDataFeed", "false").equals("true") &&
                    properties.getOrDefault("delta.enableDeletionVectors", "false").equals("true")) {
                return;
            }

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1, 'partition1')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue2', 2, 'partition2')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue3', 3, 'partition3')");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 'testValue5' WHERE partitioning_column_1 = 3");

            assertThat(env.executeSpark(
                    "SELECT updated_column, partitioning_column_1, partitioning_column_2, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "partition1", "insert", 1L),
                            row("testValue2", 2, "partition2", "insert", 2L),
                            row("testValue3", 3, "partition3", "insert", 3L),
                            row("testValue3", 3, "partition3", "update_preimage", 4L),
                            row("testValue5", 3, "partition3", "update_postimage", 4L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testUpdateTableWithManyRowsInsertedInTheSameQueryAndCdfEnabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_to_table_with_many_rows_inserted_in_one_query_cdf_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (col1 STRING, updated_column INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1), ('testValue2', 2), ('testValue3', 3)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 5 WHERE col1 = 'testValue3'");

            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue2", 2, "insert", 1L),
                            row("testValue3", 3, "insert", 1L),
                            row("testValue3", 3, "update_preimage", 2L),
                            row("testValue3", 5, "update_postimage", 2L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testUpdatePartitionedTableWithManyRowsInsertedInTheSameRequestAndCdfEnabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_to_partitioned_table_with_many_rows_inserted_in_one_query_cdf_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (updated_column STRING, partitioning_column_1 INT, partitioning_column_2 STRING) " +
                    "USING DELTA " +
                    "PARTITIONED BY (partitioning_column_1, partitioning_column_2) " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " +
                    "('testValue1', 1, 'partition1'), " +
                    "('testValue2', 2, 'partition2'), " +
                    "('testValue3', 3, 'partition3')");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 'testValue5' WHERE partitioning_column_1 = 3");

            assertThat(env.executeSpark(
                    "SELECT updated_column, partitioning_column_1, partitioning_column_2, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "partition1", "insert", 1L),
                            row("testValue2", 2, "partition2", "insert", 1L),
                            row("testValue3", 3, "partition3", "insert", 1L),
                            row("testValue3", 3, "partition3", "update_preimage", 2L),
                            row("testValue5", 3, "partition3", "update_postimage", 2L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testUpdatePartitionedTableCdfEnabledAndPartitioningColumnUpdated(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_partitioning_column_in_table_with_cdf_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (updated_column STRING, partitioning_column_1 INT, partitioning_column_2 STRING) " +
                    "USING DELTA " +
                    "PARTITIONED BY (partitioning_column_1, partitioning_column_2) " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " +
                    "('testValue1', 1, 'partition1'), " +
                    "('testValue2', 2, 'partition2'), " +
                    "('testValue3', 3, 'partition3')");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName +
                    " SET partitioning_column_1 = 5 WHERE partitioning_column_2 = 'partition1'");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName +
                    " SET partitioning_column_1 = 4, updated_column = 'testValue4' WHERE partitioning_column_2 = 'partition2'");

            assertThat(env.executeSpark(
                    "SELECT updated_column, partitioning_column_1, partitioning_column_2, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "partition1", "insert", 1L),
                            row("testValue2", 2, "partition2", "insert", 1L),
                            row("testValue3", 3, "partition3", "insert", 1L),
                            row("testValue1", 1, "partition1", "update_preimage", 2L),
                            row("testValue1", 5, "partition1", "update_postimage", 2L),
                            row("testValue2", 2, "partition2", "update_preimage", 3L),
                            row("testValue4", 4, "partition2", "update_postimage", 3L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testUpdateTableWithCdfEnabledAfterTableIsAlreadyCreated(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_updates_to_table_with_cdf_enabled_later_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (col1 STRING, updated_column INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue2', 2)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue3', 3)");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 5 WHERE col1 = 'testValue3'");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES (delta.enableChangeDataFeed = true)");
            long versionWithCdfEnabled = (long) env.executeSpark("DESCRIBE HISTORY default." + tableName + " LIMIT 1")
                    .getRows().get(0)
                    .getValue(0);
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET updated_column = 4 WHERE col1 = 'testValue3'");

            assertThatThrownBy(() -> env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 0)"))
                    .hasStackTraceContaining("Error getting change data for range [0 , 6] as change data was not");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue6', 6)");
            assertThat(env.executeSpark(
                    format("SELECT col1, updated_column, _change_type, _commit_version FROM table_changes('default.%s', %d)",
                            tableName,
                            versionWithCdfEnabled)))
                    .containsOnly(
                            row("testValue3", 5, "update_preimage", 6L),
                            row("testValue3", 4, "update_postimage", 6L),
                            row("testValue6", 6, "insert", 7L));

            long lastVersionWithCdf = (long) env.executeSpark("DESCRIBE HISTORY default." + tableName + " LIMIT 1")
                    .getRows().get(0)
                    .getValue(0);
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES (delta.enableChangeDataFeed = false)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue7', 7)");

            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    format("FROM table_changes('default.%s', %d, %d)", tableName, versionWithCdfEnabled, lastVersionWithCdf)))
                    .containsOnly(
                            row("testValue3", 5, "update_preimage", 6L),
                            row("testValue3", 4, "update_postimage", 6L),
                            row("testValue6", 6, "insert", 7L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testDeleteFromTableWithCdf(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletes_from_table_with_cdf_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (col1 STRING, updated_column INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.columnMapping.mode' = '" + columnMappingMode + "')");

            // TODO https://github.com/trinodb/trino/issues/23620 Fix incorrect CDC entry when deletion vector is enabled
            Map<String, String> properties = getTablePropertiesOnSpark(env, "default", tableName);
            if (properties.getOrDefault("delta.enableChangeDataFeed", "false").equals("true") &&
                    properties.getOrDefault("delta.enableDeletionVectors", "false").equals("true")) {
                return;
            }

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES('testValue1', 1)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES('testValue2', 2)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES('testValue3', 3)");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE col1 = 'testValue3'");

            assertThat(env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue2", 2, "insert", 2L),
                            row("testValue3", 3, "insert", 3L),
                            row("testValue3", 3, "delete", 4L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testMergeUpdateIntoTableWithCdfEnabled(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName1 = "test_merge_update_into_table_with_cdf_" + randomNameSuffix();
        String tableName2 = "test_merge_update_into_table_with_cdf_data_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName1 + " (nationkey INT, name STRING, regionkey INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName1 + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.columnMapping.mode' = '" + columnMappingMode + "')");
            env.executeSparkUpdate("CREATE TABLE default." + tableName2 + " (nationkey INT, name STRING, regionkey INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName2 + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (1, 'nation1', 100)");
            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (2, 'nation2', 200)");
            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (3, 'nation3', 300)");

            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (1000, 'nation1000', 1000)");
            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (2, 'nation2', 20000)");
            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (3000, 'nation3000', 3000)");

            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName1 + " cdf USING delta.default." + tableName2 + " n " +
                    "ON (cdf.nationkey = n.nationkey) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET nationkey = (cdf.nationkey + n.nationkey + n.regionkey) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (nationkey, name, regionkey) VALUES (n.nationkey, n.name, n.regionkey)");

            assertThat(env.executeSpark("SELECT * FROM " + tableName1))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300),
                            row(20004, "nation2", 200));

            assertThat(env.executeSpark(
                    "SELECT nationkey, name, regionkey, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName1 + "', 0)"))
                    .containsOnly(
                            row(1, "nation1", 100, "insert", 1),
                            row(2, "nation2", 200, "insert", 2),
                            row(3, "nation3", 300, "insert", 3),
                            row(1000, "nation1000", 1000, "insert", 4),
                            row(3000, "nation3000", 3000, "insert", 4),
                            row(2, "nation2", 200, "update_preimage", 4),
                            row(20004, "nation2", 200, "update_postimage", 4));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName1);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName2);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testMergeDeleteIntoTableWithCdfEnabled(String columnMappingMode, DeltaLakeMinioEnvironment env)
    {
        String tableName1 = "test_merge_delete_into_table_with_cdf_" + randomNameSuffix();
        String tableName2 = "test_merge_delete_into_table_with_cdf_data_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName1 + " (nationkey INT, name STRING, regionkey INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName1 + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.columnMapping.mode' = '" + columnMappingMode + "')");
            env.executeSparkUpdate("CREATE TABLE default." + tableName2 + " (nationkey INT, name STRING, regionkey INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName2 + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (1, 'nation1', 100)");
            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (2, 'nation2', 200)");
            env.executeSparkUpdate("INSERT INTO default." + tableName1 + " VALUES (3, 'nation3', 300)");

            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (1000, 'nation1000', 1000)");
            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (2, 'nation2', 20000)");
            env.executeSparkUpdate("INSERT INTO default." + tableName2 + " VALUES (3000, 'nation3000', 3000)");

            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName1 + " cdf USING delta.default." + tableName2 + " n " +
                    "ON (cdf.nationkey = n.nationkey) " +
                    "WHEN MATCHED " +
                    "THEN DELETE " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (nationkey, name, regionkey) VALUES (n.nationkey, n.name, n.regionkey)");

            assertThat(env.executeSpark("SELECT * FROM " + tableName1))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300));

            assertThat(env.executeSpark(
                    "SELECT nationkey, name, regionkey, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName1 + "', 0)"))
                    .containsOnly(
                            row(1, "nation1", 100, "insert", 1),
                            row(2, "nation2", 200, "insert", 2),
                            row(3, "nation3", 300, "insert", 3),
                            row(1000, "nation1000", 1000, "insert", 4),
                            row(3000, "nation3000", 3000, "insert", 4),
                            row(2, "nation2", 200, "delete", 4));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName1);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName2);
        }
    }

    @Test
    void testMergeMixedDeleteAndUpdateIntoTableWithCdfEnabled(DeltaLakeMinioEnvironment env)
    {
        String targetTableName = "test_merge_mixed_delete_and_update_into_table_with_cdf_" + randomNameSuffix();
        String sourceTableName = "test_merge_mixed_delete_and_update_into_table_with_cdf_data_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + targetTableName + " (page_id INT, page_url STRING, views INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");
            env.executeSparkUpdate("CREATE TABLE default." + sourceTableName + " (page_id INT, page_url STRING, views INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName + "'");

            // TODO https://github.com/trinodb/trino/issues/23620 Fix incorrect CDC entry when deletion vector is enabled
            Map<String, String> properties = getTablePropertiesOnSpark(env, "default", targetTableName);
            if (properties.getOrDefault("delta.enableChangeDataFeed", "false").equals("true") &&
                    properties.getOrDefault("delta.enableDeletionVectors", "false").equals("true")) {
                return;
            }

            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (1, 'pageUrl1', 100)");
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (2, 'pageUrl2', 200)");
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (3, 'pageUrl3', 300)");
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (4, 'pageUrl4', 400)");

            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (1000, 'pageUrl1000', 1000)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (2, 'pageUrl2', 20000)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (3000, 'pageUrl3000', 3000)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (4, 'pageUrl4000', 4000)");

            env.executeTrinoUpdate("MERGE INTO delta.default." + targetTableName + " targetTable USING delta.default." + sourceTableName + " sourceTable " +
                    "ON (targetTable.page_id = sourceTable.page_id) " +
                    "WHEN MATCHED AND targetTable.page_id = 2 " +
                    "THEN DELETE " +
                    "WHEN MATCHED AND targetTable.page_id > 2 " +
                    "THEN UPDATE SET views = (targetTable.views + sourceTable.views) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (page_id, page_url, views) VALUES (sourceTable.page_id, sourceTable.page_url, sourceTable.views)");

            assertThat(env.executeSpark("SELECT * FROM " + targetTableName))
                    .containsOnly(
                            row(1000, "pageUrl1000", 1000),
                            row(3000, "pageUrl3000", 3000),
                            row(4, "pageUrl4", 4400),
                            row(1, "pageUrl1", 100),
                            row(3, "pageUrl3", 300));

            assertThat(env.executeSpark(
                    "SELECT page_id, page_url, views, _change_type, _commit_version " +
                            "FROM table_changes('default." + targetTableName + "', 0)"))
                    .containsOnly(
                            row(1, "pageUrl1", 100, "insert", 1),
                            row(2, "pageUrl2", 200, "insert", 2),
                            row(3, "pageUrl3", 300, "insert", 3),
                            row(4, "pageUrl4", 400, "insert", 4),
                            row(1000, "pageUrl1000", 1000, "insert", 5),
                            row(3000, "pageUrl3000", 3000, "insert", 5),
                            row(2, "pageUrl2", 200, "delete", 5),
                            row(4, "pageUrl4", 4400, "update_postimage", 5),
                            row(4, "pageUrl4", 400, "update_preimage", 5));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
        }
    }

    @Test
    void testDeleteFromNullPartitionWithCdfEnabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delete_from_null_partition_with_cdf_enabled" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + tableName + " (updated_column STRING, partitioning_column_1 INT, partitioning_column_2 STRING) " +
                    "USING DELTA " +
                    "PARTITIONED BY (partitioning_column_1, partitioning_column_2) " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " +
                    "('testValue1', 1, 'partition1'), " +
                    "('testValue2', 2, 'partition2'), " +
                    "('testValue3', 3, NULL)");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName +
                    " WHERE partitioning_column_2 IS NULL");

            assertThat(env.executeTrino(
                    "SELECT * FROM delta.default." + tableName
            )).containsOnly(
                    row("testValue1", 1, "partition1"),
                    row("testValue2", 2, "partition2"));

            assertThat(env.executeSpark(
                    "SELECT updated_column, partitioning_column_1, partitioning_column_2, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row("testValue1", 1, "partition1", "insert", 1L),
                            row("testValue2", 2, "partition2", "insert", 1L),
                            row("testValue3", 3, null, "insert", 1L),
                            row("testValue3", 3, null, "delete", 2L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTurningOnAndOffCdfFromTrino(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_turning_cdf_on_and_off_from_trino" + randomNameSuffix();
        try {
            env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (col1 VARCHAR, updated_column INT) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', change_data_feed_enabled = true)");

            assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString()).contains("change_data_feed_enabled = true");

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1)");
            env.executeSparkUpdate("UPDATE default." + tableName + " SET updated_column = 10 WHERE col1 = 'testValue1'");
            assertThat(env.executeSpark(
                    "SELECT col1, updated_column, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0, 2)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue1", 1, "update_preimage", 2L),
                            row("testValue1", 10, "update_postimage", 2L));

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " SET PROPERTIES change_data_feed_enabled = false");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue2', 2)");
            env.executeSparkUpdate("UPDATE default." + tableName + " SET updated_column = 20 WHERE col1 = 'testValue2'");
            assertThatThrownBy(() -> env.executeSpark("SELECT col1, updated_column, _change_type, _commit_version " +
                    "FROM table_changes('default." + tableName + "', 4, 5)"))
                    .hasStackTraceContaining("Error getting change data for range [4 , 5] as change data was not");
            assertThat(env.executeSpark(
                    "SELECT col1, updated_column, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 0, 2)"))
                    .containsOnly(
                            row("testValue1", 1, "insert", 1L),
                            row("testValue1", 1, "update_preimage", 2L),
                            row("testValue1", 10, "update_postimage", 2L));

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " SET PROPERTIES change_data_feed_enabled = true");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue3', 3)");
            env.executeSparkUpdate("UPDATE default." + tableName + " SET updated_column = 30 WHERE col1 = 'testValue3'");
            assertThat(env.executeSpark(
                    "SELECT col1, updated_column, _change_type, _commit_version " +
                            "FROM table_changes('default." + tableName + "', 7, 8)"))
                    .containsOnly(
                            row("testValue3", 3, "insert", 7L),
                            row("testValue3", 3, "update_preimage", 8L),
                            row("testValue3", 30, "update_postimage", 8L));

            assertThat(env.executeSpark("SELECT * FROM " + tableName))
                    .containsOnly(row("testValue1", 10), row("testValue2", 20), row("testValue3", 30));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    @Test
    void testThatCdfDoesntWorkWhenPropertyIsNotSet(DeltaLakeMinioEnvironment env)
    {
        String tableName1 = "test_cdf_doesnt_work_when_property_is_not_set_1_" + randomNameSuffix();
        String tableName2 = "test_cdf_doesnt_work_when_property_is_not_set_2_" + randomNameSuffix();
        assertThereIsNoCdfFileGenerated(env, tableName1, "");
        assertThereIsNoCdfFileGenerated(env, tableName2, "change_data_feed_enabled = false");
    }

    @Test
    void testTrinoCanReadCdfEntriesGeneratedByDelta(DeltaLakeMinioEnvironment env)
    {
        String targetTableName = "test_trino_can_read_cdf_entries_generated_by_delta_target_" + randomNameSuffix();
        String sourceTableName = "test_trino_can_read_cdf_entries_generated_by_delta_source_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + targetTableName + " (page_id INT, page_url STRING, views INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");
            env.executeSparkUpdate("CREATE TABLE default." + sourceTableName + " (page_id INT, page_url STRING, views INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName + "'");

            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (1, 'pageUrl1', 100), (2, 'pageUrl2', 200), (3, 'pageUrl3', 300)");
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (4, 'pageUrl4', 400)");

            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (1000, 'pageUrl1000', 1000), (2, 'pageUrl2', 20000)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (3000, 'pageUrl3000', 3000), (4, 'pageUrl4000', 4000)");

            env.executeSparkUpdate("MERGE INTO default." + targetTableName + " targetTable USING default." + sourceTableName + " sourceTable " +
                    "ON (targetTable.page_id = sourceTable.page_id) " +
                    "WHEN MATCHED AND targetTable.page_id = 2 " +
                    "THEN DELETE " +
                    "WHEN MATCHED AND targetTable.page_id > 2 " +
                    "THEN UPDATE SET views = (targetTable.views + sourceTable.views) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (page_id, page_url, views) VALUES (sourceTable.page_id, sourceTable.page_url, sourceTable.views)");
            env.executeSparkUpdate("UPDATE default." + targetTableName + " SET page_url = 'pageUrl30' WHERE page_id = 3");
            env.executeSparkUpdate("DELETE FROM default." + targetTableName + " WHERE page_url = 'pageUrl1'");

            assertThat(env.executeSpark("SELECT * FROM " + targetTableName))
                    .containsOnly(
                            row(1000, "pageUrl1000", 1000),
                            row(3000, "pageUrl3000", 3000),
                            row(4, "pageUrl4", 4400),
                            row(3, "pageUrl30", 300));

            List<Row> rows = ImmutableList.<Row>builder()
                    .add(row(1, "pageUrl1", 100, "insert", 1))
                    .add(row(2, "pageUrl2", 200, "insert", 1))
                    .add(row(3, "pageUrl3", 300, "insert", 1))
                    .add(row(4, "pageUrl4", 400, "insert", 2))
                    .add(row(1000, "pageUrl1000", 1000, "insert", 3))
                    .add(row(3000, "pageUrl3000", 3000, "insert", 3))
                    .add(row(2, "pageUrl2", 200, "delete", 3))
                    .add(row(4, "pageUrl4", 4400, "update_postimage", 3))
                    .add(row(4, "pageUrl4", 400, "update_preimage", 3))
                    .add(row(3, "pageUrl3", 300, "update_preimage", 4))
                    .add(row(3, "pageUrl30", 300, "update_postimage", 4))
                    .add(row(1, "pageUrl1", 100, "delete", 5))
                    .build();
            assertThat(env.executeTrino(
                    "SELECT page_id, page_url, views, _change_type, _commit_version " +
                            "FROM TABLE(delta.system.table_changes('default', '" + targetTableName + "'))"))
                    .containsOnly(rows);

            assertThat(env.executeSpark(
                    "SELECT page_id, page_url, views, _change_type, _commit_version " +
                            "FROM table_changes('default." + targetTableName + "', 0)"))
                    .containsOnly(rows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
        }
    }

    @Test
    void testDeltaCanReadCdfEntriesGeneratedByTrino(DeltaLakeMinioEnvironment env)
    {
        String targetTableName = "test_delta_can_read_cdf_entries_generated_by_trino_target_" + randomNameSuffix();
        String sourceTableName = "test_delta_can_read_cdf_entries_generated_by_trino_source_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate("CREATE TABLE delta.default." + targetTableName + " (page_id INT, page_url VARCHAR, views INT) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName +
                    "', change_data_feed_enabled = true)");

            env.executeTrinoUpdate("CREATE TABLE delta.default." + sourceTableName + " (page_id INT, page_url VARCHAR, views INT) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName +
                    "', change_data_feed_enabled = true)");

            env.executeTrinoUpdate("INSERT INTO delta.default." + targetTableName + " VALUES (1, 'pageUrl1', 100), (2, 'pageUrl2', 200), (3, 'pageUrl3', 300)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + targetTableName + " VALUES (4, 'pageUrl4', 400)");

            env.executeTrinoUpdate("INSERT INTO delta.default." + sourceTableName + " VALUES (1000, 'pageUrl1000', 1000), (2, 'pageUrl2', 20000)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + sourceTableName + " VALUES (3000, 'pageUrl3000', 3000), (4, 'pageUrl4000', 4000)");

            env.executeTrinoUpdate("MERGE INTO delta.default." + targetTableName + " targetTable USING delta.default." + sourceTableName + " sourceTable " +
                    "ON (targetTable.page_id = sourceTable.page_id) " +
                    "WHEN MATCHED AND targetTable.page_id = 2 " +
                    "THEN DELETE " +
                    "WHEN MATCHED AND targetTable.page_id > 2 " +
                    "THEN UPDATE SET views = (targetTable.views + sourceTable.views) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (page_id, page_url, views) VALUES (sourceTable.page_id, sourceTable.page_url, sourceTable.views)");
            env.executeTrinoUpdate("UPDATE delta.default." + targetTableName + " SET page_url = 'pageUrl30' WHERE page_id = 3");
            env.executeTrinoUpdate("DELETE FROM delta.default." + targetTableName + " WHERE page_url = 'pageUrl1'");

            assertThat(env.executeTrino("SELECT * FROM delta.default." + targetTableName))
                    .containsOnly(
                            row(1000, "pageUrl1000", 1000),
                            row(3000, "pageUrl3000", 3000),
                            row(4, "pageUrl4", 4400),
                            row(3, "pageUrl30", 300));

            List<Row> rows = ImmutableList.<Row>builder()
                    .add(row(1, "pageUrl1", 100, "insert", 1))
                    .add(row(2, "pageUrl2", 200, "insert", 1))
                    .add(row(3, "pageUrl3", 300, "insert", 1))
                    .add(row(4, "pageUrl4", 400, "insert", 2))
                    .add(row(1000, "pageUrl1000", 1000, "insert", 3))
                    .add(row(3000, "pageUrl3000", 3000, "insert", 3))
                    .add(row(2, "pageUrl2", 200, "delete", 3))
                    .add(row(4, "pageUrl4", 4400, "update_postimage", 3))
                    .add(row(4, "pageUrl4", 400, "update_preimage", 3))
                    .add(row(3, "pageUrl3", 300, "update_preimage", 4))
                    .add(row(3, "pageUrl30", 300, "update_postimage", 4))
                    .add(row(1, "pageUrl1", 100, "delete", 5))
                    .build();
            assertThat(env.executeTrino(
                    "SELECT page_id, page_url, views, _change_type, _commit_version " +
                            "FROM TABLE(delta.system.table_changes('default', '" + targetTableName + "'))"))
                    .containsOnly(rows);

            assertThat(env.executeSpark(
                    "SELECT page_id, page_url, views, _change_type, _commit_version " +
                            "FROM table_changes('default." + targetTableName + "', 0)"))
                    .containsOnly(rows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
        }
    }

    static Stream<Arguments> columnMappingModeDataProvider()
    {
        return Stream.of(
                Arguments.of("name"),
                Arguments.of("id"),
                Arguments.of("none"));
    }

    private void assertThereIsNoCdfFileGenerated(DeltaLakeMinioEnvironment env, String tableName, String tableProperty)
    {
        try {
            env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (col1 VARCHAR, updated_column INT) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                    (tableProperty.isEmpty() ? "" : ", " + tableProperty) + ")");

            if (tableProperty.isEmpty()) {
                assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString())
                        .doesNotContain("change_data_feed_enabled");
            }
            else {
                assertThat(env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue().toString())
                        .contains(tableProperty);
            }
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue1', 1)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue2', 2)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('testValue3', 3)");

            // as INSERTs don't generate cdf files other operation is needed, UPDATE will do
            env.executeTrinoUpdate("UPDATE delta.default." + tableName +
                    " SET updated_column = 5 WHERE col1 = 'testValue3'");
            env.executeSparkUpdate("UPDATE default." + tableName +
                    " SET updated_column = 4 WHERE col1 = 'testValue2'");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(
                            row("testValue1", 1),
                            row("testValue2", 4),
                            row("testValue3", 5));

            assertThatThereIsNoChangeDataFiles(env, tableName);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    private void assertThatThereIsNoChangeDataFiles(DeltaLakeMinioEnvironment env, String tableName)
    {
        String prefix = "databricks-compatibility-test-" + tableName + "/_change_data/";
        try (MinioClient minioClient = env.createMinioClient()) {
            List<String> objects = minioClient.listObjects(env.getBucketName(), prefix);
            assertThat(objects).isEmpty();
        }
    }

    // Helper method to get table properties from Spark (equivalent to getTablePropertiesOnDelta)
    private Map<String, String> getTablePropertiesOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = env.executeSpark("SHOW TBLPROPERTIES %s.%s".formatted(schemaName, tableName));
        return result.getRows().stream()
                .map(row -> Map.entry((String) row.getValue(0), (String) row.getValue(1)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
