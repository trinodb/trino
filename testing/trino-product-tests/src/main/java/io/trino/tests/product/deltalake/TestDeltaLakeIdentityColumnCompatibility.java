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

import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnNamesOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeIdentityColumnCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumn()
    {
        // Databricks 7.3 & 9.1 and OSS Delta Lake don't support identity columns https://github.com/delta-io/delta/issues/1100
        String tableName = "test_identity_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                """
                CREATE TABLE default.%s (a INT, b BIGINT GENERATED ALWAYS AS IDENTITY)
                USING DELTA LOCATION 's3://%s/%s'
                """,
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test column comment'");
            assertThat(getColumnCommentOnDelta("default", tableName, "b")).isEqualTo("test column comment");

            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            assertThat(getTableCommentOnDelta("default", tableName)).isEqualTo("test table comment");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            assertThat(getColumnNamesOnDelta("default", tableName)).containsExactly("a", "b", "c");
            // Don't execute other column operations because column mapping mode 'none' doesn't support it. See other test methods in this class.

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("b BIGINT GENERATED ALWAYS AS IDENTITY");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a, c) VALUES (0, 2)");

            QueryAssert.Row expected = row(0, 1, 2);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumnTableFeature()
    {
        String tableName = "test_identity_column_feature_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.feature.identityColumns'='supported')");
        try {
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 1)"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET data = 1"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.data = s.data) WHEN MATCHED THEN UPDATE SET data = 1"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 7 which is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testWritesToTableWithIdentityColumnFails()
    {
        String tableName = "test_writes_into_table_with_identity_column_" + randomNameSuffix();
        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES (1), (2), (3)");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1), row(2, 2), row(3, 3));

            // Disallowing all statements just in case though some statements may not unrelated to identity columns
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 4)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET data = 3"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.data = s.data) WHEN MATCHED THEN UPDATE SET data = 1"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testRenameIdentityColumn(String mode)
    {
        String tableName = "test_rename_identity_column_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");
        try {
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN col_identity TO delta_col_identity");
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("delta_col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 10");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1));

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN delta_col_identity TO trino_col_identity");
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("trino_col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 20");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDropIdentityColumn(String mode)
    {
        String tableName = "test_drop_identity_column_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, first_identity BIGINT GENERATED ALWAYS AS IDENTITY, second_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");
        try {
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            onDelta().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN first_identity");
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("data", "second_identity");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " (data) VALUES (1)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN second_identity");
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("data");

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES 10");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 20");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10), row(20));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(10), row(20));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testVacuumProcedureWithIdentityColumn()
    {
        String tableName = "test_vacuum_identity_column_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 10");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 20");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE data = 20");

            onTrino().executeQuery("SET SESSION delta.vacuum_min_retention = '0s'");
            onTrino().executeQuery("CALL delta.system.vacuum('default', '" + tableName + "', '0s')");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("col_identity BIGINT GENERATED ALWAYS AS IDENTITY");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testOptimizeProcedureWithIdentityColumn()
    {
        String tableName = "test_optimize_identity_column_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 10");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES 20");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " EXECUTE OPTIMIZE");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("col_identity BIGINT GENERATED ALWAYS AS IDENTITY");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(10, 1), row(20, 2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumnCheckpointInterval()
    {
        String tableName = "test_identity_column_checkpoint_interval_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, col_identity BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/" + "databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.checkpointInterval' = 1)");
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col_identity IS 'test column comment'");
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("col_identity BIGINT GENERATED ALWAYS AS IDENTITY");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public Object[][] columnMappingDataProvider()
    {
        return new Object[][] {
                {"id"},
                {"name"},
        };
    }
}
