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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * JUnit 5 port of Delta Lake delete compatibility tests from TestDeltaLakeDeleteCompatibility.
 * <p>
 * This class ports only the tests marked with DELTA_LAKE_OSS group.
 * Tests marked only with DELTA_LAKE_DATABRICKS are not included.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeDeleteCompatibility
{
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteOnEnforcedConstraintsReturnsRowsCount(boolean partitioned, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delete_push_down_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(v INT, p INT)" +
                "USING delta " +
                (partitioned ? "PARTITIONED BY (p)" : "") +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 10), (2, 10), (11, 20), (21, 30), (22, 30)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3, 10), (12, 20)");
            if (partitioned) {
                assertThat(env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE p = 10"))
                        .isEqualTo(3);
                assertThat(env.executeTrinoUpdate("DELETE FROM delta.default." + tableName))
                        .isEqualTo(4);
            }
            else {
                assertThat(env.executeTrinoUpdate("DELETE FROM delta.default." + tableName))
                        .isEqualTo(7);
            }

            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).hasNoRows();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeleteOnAppendOnlyWriterFeature(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delete_on_append_only_feature_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.minWriterVersion'='7', 'delta.appendOnly' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "true"));

            assertThatThrownBy(() -> env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 1"))
                    .hasStackTraceContaining("This table is configured to only allow appends");
            assertThatThrownBy(() -> env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 1"))
                    .hasMessageContaining("Cannot modify rows from a table with 'delta.appendOnly' set to true");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 12));

            // delta.feature.appendOnly still exists even after unsetting the property
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.feature.appendOnly')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "true"));

            // Disable delta.appendOnly property
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.appendOnly'=false)");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "false"));

            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 1");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 2");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoDeletionVectors(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_trino_deletion_vectors_" + randomNameSuffix();
        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(a INT)" +
                "WITH (deletion_vectors_enabled = true, location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1, 2");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 2");

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(row(1));
            assertThat(getTablePropertyOnSpark(env, "default", tableName, "delta.enableDeletionVectors")).isEqualTo("true");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    // Databricks 12.1 and OSS Delta 2.4.0 added support for deletion vectors
    @ParameterizedTest
    @MethodSource("columnMappingModeDataProvider")
    void testDeletionVectors(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.columnMapping.mode' = '" + mode + "')");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,11), (2, 22)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));

            // Reinsert the deleted row and verify that the row appears correctly
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (2, 22)");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Execute DELETE statement which doesn't delete any rows
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = -1");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Verify other statements
            assertThat(env.executeTrino("SHOW TABLES FROM delta.default"))
                    .contains(row(tableName));
            assertThat(env.executeTrino("SELECT version, operation FROM delta.default.\"" + tableName + "$history\""))
                    // Use 'contains' method because newer Databricks clusters execute OPTIMIZE statement in the background
                    .contains(row(0L, "CREATE TABLE"), row(1L, "WRITE"), row(2L, "DELETE"));
            assertThat(env.executeTrino("SELECT column_name FROM delta.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .contains(row("a"), row("b"));
            assertThat(env.executeTrino("SHOW COLUMNS FROM delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));
            assertThat(env.executeTrino("DESCRIBE delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (3, 33)");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 1");
            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET a = 30 WHERE b = 33");
            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = -1");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(2, -1), row(30, -1));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2, -1), row(30, -1));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsWithPartitionedTable(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_partitioned_table_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(id INT, part STRING)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "PARTITIONED BY (part)" +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'part'), (2, 'part')");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE id = 1");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(2, "part"));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2, "part"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testDeletionVectorsWithRandomPrefix(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.randomizeFilePrefixes' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22), (3, 33)");

            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 2");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));

            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a = 3");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDisableDeletionVectors(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22), (3, 33)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 2");

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));

            // Delete rows which already existed before disabling deletion vectors
            env.executeSparkUpdate("DELETE FROM default." + tableName);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .hasNoRows();
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .hasNoRows();

            // Insert new rows and delete it after disabling deletion vectors
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (4, 44), (5, 55)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 4");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(5, 55));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(5, 55));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsWithCheckpointInterval(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.checkpointInterval' = 1)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsMergeDelete(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_merge_delete_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10))");
            env.executeSparkUpdate("MERGE INTO default." + tableName + " t USING default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED AND t.a > 5 THEN DELETE");

            List<Row> expected = ImmutableList.of(row(1), row(2), row(3), row(4), row(5));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsLargeNumbers(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_large_numbers_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10000))");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a > 1");

            List<Row> expected = ImmutableList.of(row(1));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testChangeDataFeedWithDeletionVectors(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_change_data_feed_with_deletion_vectors_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(col1 STRING, updated_column INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableChangeDataFeed' = true, 'delta.enableDeletionVectors' = true)");
        env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('testValue1', 1), ('testValue2', 2), ('testValue3', 3)");
        env.executeTrinoUpdate("UPDATE delta.default." + tableName + "  SET updated_column = 30 WHERE col1 = 'testValue3'");

        assertThat(env.executeSpark("SELECT col1, updated_column, _change_type FROM table_changes('default." + tableName + "', 0)"))
                .containsOnly(
                        row("testValue1", 1, "insert"),
                        row("testValue2", 2, "insert"),
                        row("testValue3", 3, "insert"),
                        row("testValue3", 3, "update_preimage"),
                        row("testValue3", 30, "update_postimage"));
        assertThat(env.executeTrino("SELECT col1, updated_column, _change_type FROM TABLE(delta.system.table_changes('default', '" + tableName + "', 0))"))
                .containsOnly(
                        row("testValue1", 1, "insert"),
                        row("testValue2", 2, "insert"),
                        row("testValue3", 3, "insert"),
                        row("testValue3", 3, "update_preimage"),
                        row("testValue3", 30, "update_postimage"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeletionVectorsAcrossAddFile(boolean partitioned, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_accross_add_file_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                (partitioned ? "PARTITIONED BY (a)" : "") +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 2 OR a = 4");

            List<Row> expected = ImmutableList.of(row(1, 11), row(3, 33));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);

            // Verify behavior when the query doesn't read non-partition columns
            assertThat(env.executeTrino("SELECT count(*) FROM delta.default." + tableName)).containsOnly(row(2L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    // OSS Delta Lake doesn't support truncating tables with deletion vector, so we skip this test
    // and only test DELETE FROM
    @Test
    void testDeletionVectorsDeleteFrom(DeltaLakeMinioEnvironment env)
    {
        testDeletionVectorsDeleteAll(env, tableName -> env.executeSparkUpdate("DELETE FROM default." + tableName));
    }

    private void testDeletionVectorsDeleteAll(DeltaLakeMinioEnvironment env, Consumer<String> deleteRow)
    {
        String tableName = "test_deletion_vectors_delete_all_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 1000))");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).hasRowsCount(1000);

            deleteRow.accept(tableName);

            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).hasNoRows();
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsOptimize(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_optimize_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).contains(expected);

            env.executeSparkUpdate("OPTIMIZE default." + tableName);

            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsAbsolutePath(DeltaLakeMinioEnvironment env)
    {
        String baseTableName = "test_deletion_vectors_base_absolute_" + randomNameSuffix();
        String tableName = "test_deletion_vectors_absolute_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + baseTableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + baseTableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + baseTableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            // Ensure that the content of the table is coalesced in a larger file
            env.executeSparkUpdate("OPTIMIZE default." + baseTableName);
            env.executeSparkUpdate("DELETE FROM default." + baseTableName + " WHERE a = 1 OR a = 3");

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(env.executeSpark("SELECT * FROM default." + baseTableName)).contains(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTableName)).contains(expected);

            // The cloned table has 'p' (absolute path) storageType for deletion vector
            env.executeSparkUpdate("" +
                    "CREATE TABLE default." + tableName + " SHALLOW CLONE " + baseTableName + " " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-clone-" + baseTableName + "'");

            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + baseTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeletionVectorsWithChangeDataFeed(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_deletion_vectors_cdf_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.enableChangeDataFeed' = true)");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            assertThat(env.executeSpark(
                    "SELECT a, b, _change_type, _commit_version FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row(1, 11, "insert", 1L),
                            row(2, 22, "insert", 1L),
                            row(3, 33, "insert", 1L),
                            row(4, 44, "insert", 1L),
                            row(1, 11, "delete", 2L),
                            row(3, 33, "delete", 2L));

            // TODO Fix table_changes function failure
            assertThatThrownBy(() -> env.executeTrino("SELECT a, b, _change_type, _commit_version FROM TABLE(delta.system.table_changes('default', '" + tableName + "', 0))"))
                    .hasMessageContaining("Change Data Feed is not enabled at version 2. Version contains 'remove' entries without 'cdc' entries");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    static Stream<Arguments> columnMappingModeDataProvider()
    {
        return Stream.of(
                Arguments.of("none"),
                Arguments.of("name"),
                Arguments.of("id"));
    }

    // Helper method to get table properties from Spark (equivalent to getTablePropertiesOnDelta)
    private static Map<String, String> getTablePropertiesOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = env.executeSpark("SHOW TBLPROPERTIES %s.%s".formatted(schemaName, tableName));
        return result.getRows().stream()
                .map(row -> Map.entry((String) row.getValue(0), (String) row.getValue(1)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // Helper method to get a specific table property from Spark (equivalent to getTablePropertyOnDelta)
    private static String getTablePropertyOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String propertyName)
    {
        QueryResult result = env.executeSpark("SHOW TBLPROPERTIES %s.%s(%s)".formatted(schemaName, tableName, propertyName));
        return (String) result.getRows().get(0).getValue(1);
    }
}
