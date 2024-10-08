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
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testing.DataProviders;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTablePropertiesOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTablePropertyOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

public class TestDeltaLakeDeleteCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private Optional<DatabricksVersion> databricksRuntimeVersion;

    @BeforeMethodWithContext
    public void determineDatabricksVersion()
    {
        databricksRuntimeVersion = getDatabricksRuntimeVersion();
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testDeleteOnEnforcedConstraintsReturnsRowsCount(boolean partitioned)
    {
        String tableName = "test_delete_push_down_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(v INT, p INT)" +
                "USING delta " +
                (partitioned ? "PARTITIONED BY (p)" : "") +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 10), (2, 10), (11, 20), (21, 30), (22, 30)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 10), (12, 20)");
            if (partitioned) {
                assertThat(onTrino().executeQuery("DELETE FROM default." + tableName + " WHERE p = 10"))
                        .containsOnly(row(3));
                assertThat(onTrino().executeQuery("DELETE FROM default." + tableName))
                        .containsOnly(row(4));
            }
            else {
                assertThat(onTrino().executeQuery("DELETE FROM default." + tableName))
                        .containsOnly(row(7));
            }

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteCompatibility()
    {
        String tableName = "test_delete_compatibility_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName + " (a int, b int)" +
                " USING DELTA LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, 2),
                    row(3, 4),
                    row(5, 6));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeleteOnAppendOnlyWriterFeature()
    {
        String tableName = "test_delete_on_append_only_feature_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.minWriterVersion'='7', 'delta.appendOnly' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
            assertThat(getTablePropertiesOnDelta("default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "true"));

            assertQueryFailure(() -> onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1"))
                    .hasMessageContaining("This table is configured to only allow appends");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 1"))
                    .hasMessageContaining("Cannot modify rows from a table with 'delta.appendOnly' set to true");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 12));

            // delta.feature.appendOnly still exists even after unsetting the property
            onDelta().executeQuery("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.feature.appendOnly')");
            assertThat(getTablePropertiesOnDelta("default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "true"));

            // Disable delta.appendOnly property
            onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.appendOnly'=false)");
            assertThat(getTablePropertiesOnDelta("default", tableName))
                    .contains(entry("delta.feature.appendOnly", "supported"))
                    .contains(entry("delta.appendOnly", "false"));

            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 2");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // OSS Delta doesn't support TRUNCATE TABLE statement
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTruncateTable()
    {
        String tableName = "test_truncate_table_" + randomNameSuffix();
        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "(a INT)" +
                "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "')");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 1, 2, 3");
            onTrino().executeQuery("TRUNCATE TABLE delta.default." + tableName);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).hasNoRows();

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES 4, 5, 6");
            onDelta().executeQuery("TRUNCATE TABLE default." + tableName);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoDeletionVectors()
    {
        String tableName = "test_trino_deletion_vectors_" + randomNameSuffix();
        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "(a INT)" +
                "WITH (deletion_vectors_enabled = true, location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "')");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 1, 2");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 2");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(row(1));
            assertThat(getTablePropertyOnDelta("default", tableName, "delta.enableDeletionVectors")).isEqualTo("true");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    // Databricks 12.1 and OSS Delta 2.4.0 added support for deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingModeDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectors(String mode)
    {
        String tableName = "test_deletion_vectors_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.columnMapping.mode' = '" + mode + "')");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));

            // Reinsert the deleted row and verify that the row appears correctly
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 22)");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Execute DELETE statement which doesn't delete any rows
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = -1");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(2, 22));

            // Verify other statements
            assertThat(onTrino().executeQuery("SHOW TABLES FROM delta.default"))
                    .contains(row(tableName));
            assertThat(onTrino().executeQuery("SELECT version, operation FROM delta.default.\"" + tableName + "$history\""))
                    // Use 'contains' method because newer Databricks clusters execute OPTIMIZE statement in the background
                    .contains(row(0, "CREATE TABLE"), row(1, "WRITE"), row(2, "DELETE"));
            assertThat(onTrino().executeQuery("SELECT column_name FROM delta.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .contains(row("a"), row("b"));
            assertThat(onTrino().executeQuery("SHOW COLUMNS FROM delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .contains(row("a", "integer", "", ""), row("b", "integer", "", ""));

            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 33)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 1");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 30 WHERE b = 33");
            onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = -1");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(2, -1), row(30, -1));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2, -1), row(30, -1));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsWithPartitionedTable()
    {
        String tableName = "test_deletion_vectors_partitioned_table_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(id INT, part STRING)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "PARTITIONED BY (part)" +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'part'), (2, 'part')");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE id = 1");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(2, "part"));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(2, "part"));
        }
        finally {
            onDelta().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsWithRandomPrefix()
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.randomizeFilePrefixes' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDisableDeletionVectors()
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22), (3, 33)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11), row(3, 33));

            // Delete rows which already existed before disabling deletion vectors
            onDelta().executeQuery("DELETE FROM default." + tableName);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .hasNoRows();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .hasNoRows();

            // Insert new rows and delete it after disabling deletion vectors
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (4, 44), (5, 55)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 4");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(5, 55));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(5, 55));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsWithCheckpointInterval()
    {
        String tableName = "test_deletion_vectors_random_prefix_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.checkpointInterval' = 1)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 11));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsMergeDelete()
    {
        String tableName = "test_deletion_vectors_merge_delete_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10))");
            onDelta().executeQuery("MERGE INTO default." + tableName + " t USING default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED AND t.a > 5 THEN DELETE");

            List<Row> expected = ImmutableList.of(row(1), row(2), row(3), row(4), row(5));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsLargeNumbers()
    {
        String tableName = "test_deletion_vectors_large_numbers_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 10000))");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a > 1");

            List<Row> expected = ImmutableList.of(row(1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS},
            dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testDeletionVectorsAcrossAddFile(boolean partitioned)
    {
        String tableName = "test_deletion_vectors_accross_add_file_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                (partitioned ? "PARTITIONED BY (a)" : "") +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2 OR a = 4");

            List<Row> expected = ImmutableList.of(row(1, 11), row(3, 33));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);

            // Verify behavior when the query doesn't read non-partition columns
            assertThat(onTrino().executeQuery("SELECT count(*) FROM delta.default." + tableName)).containsOnly(row(2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectorsTruncateTable()
    {
        testDeletionVectorsDeleteAll(tableName -> {
            if (databricksRuntimeVersion.isPresent()) {
                onDelta().executeQuery("TRUNCATE TABLE default." + tableName);
            }
            else {
                assertThatThrownBy(() -> onDelta().executeQuery("TRUNCATE TABLE default." + tableName))
                        .hasMessageContaining("Table does not support truncates");
                throw new SkipException("OSS Delta Lake doesn't support truncating tables with deletion vector");
            }
        });
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsDeleteFrom()
    {
        testDeletionVectorsDeleteAll(tableName -> onDelta().executeQuery("DELETE FROM default." + tableName));
    }

    private void testDeletionVectorsDeleteAll(Consumer<String> deleteRow)
    {
        String tableName = "test_deletion_vectors_delete_all_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " SELECT explode(sequence(1, 1000))");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasRowsCount(1000);

            deleteRow.accept(tableName);

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).hasNoRows();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsOptimize()
    {
        String tableName = "test_deletion_vectors_optimize_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);

            onDelta().executeQuery("OPTIMIZE default." + tableName);

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsAbsolutePath()
    {
        String baseTableName = "test_deletion_vectors_base_absolute_" + randomNameSuffix();
        String tableName = "test_deletion_vectors_absolute_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + baseTableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + baseTableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + baseTableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            // Ensure that the content of the table is coalesced in a larger file
            onDelta().executeQuery("OPTIMIZE default." + baseTableName);
            onDelta().executeQuery("DELETE FROM default." + baseTableName + " WHERE a = 1 OR a = 3");

            List<Row> expected = ImmutableList.of(row(2, 22), row(4, 44));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTableName)).contains(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTableName)).contains(expected);

            // The cloned table has 'p' (absolute path) storageType for deletion vector
            onDelta().executeQuery("" +
                    "CREATE TABLE default." + tableName + " SHALLOW CLONE " + baseTableName + " " +
                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-clone-" + baseTableName + "'");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).contains(expected);
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .hasMessageContaining("Unsupported storage type for deletion vector: p");
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTableName);
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeletionVectorsWithChangeDataFeed()
    {
        String tableName = "test_deletion_vectors_cdf_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(a INT, b INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "TBLPROPERTIES ('delta.enableDeletionVectors' = true, 'delta.enableChangeDataFeed' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2,22), (3,33), (4,44)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1 OR a = 3");

            assertThat(onDelta().executeQuery(
                    "SELECT a, b, _change_type, _commit_version FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(
                            row(1, 11, "insert", 1L),
                            row(2, 22, "insert", 1L),
                            row(3, 33, "insert", 1L),
                            row(4, 44, "insert", 1L),
                            row(1, 11, "delete", 2L),
                            row(3, 33, "delete", 2L));

            // TODO Fix table_changes function failure
            assertQueryFailure(() -> onTrino().executeQuery("SELECT a, b, _change_type, _commit_version FROM TABLE(delta.system.table_changes('default', '" + tableName + "', 0))"))
                    .hasMessageContaining("Change Data Feed is not enabled at version 2. Version contains 'remove' entries without 'cdc' entries");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public Object[][] columnMappingModeDataProvider()
    {
        return new Object[][] {
                {"none"},
                {"name"},
                {"id"}
        };
    }
}
