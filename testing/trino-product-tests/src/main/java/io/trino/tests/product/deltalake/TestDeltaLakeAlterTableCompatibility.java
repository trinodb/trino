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

import io.trino.testng.services.Flaky;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_91_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakeAlterTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testAddColumnWithCommentOnTrino()
    {
        String tableName = "test_dl_add_column_with_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT COMMENT 'new column comment'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "new_col"), "new column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "new_col"), "new column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testAddColumnUnsupportedWriterVersion()
    {
        String tableName = "test_dl_add_column_unsupported_writer_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='5')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col int"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 5 which is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testRenameColumn()
    {
        String tableName = "test_dl_rename_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col INT) " +
                        "USING DELTA LOCATION 's3://%s/%s' " +
                        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1)");
            assertThat(onTrino().executeQuery("SELECT col FROM delta.default." + tableName))
                    .containsOnly(row(1));

            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN col TO new_col");
            assertThat(onTrino().executeQuery("SELECT new_col FROM delta.default." + tableName))
                    .containsOnly(row(1));

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2)");
            assertThat(onTrino().executeQuery("SELECT new_col FROM delta.default." + tableName))
                    .containsOnly(row(1), row(2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testRenamePartitionedColumn()
    {
        String tableName = "test_dl_rename_partitioned_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col INT, part STRING) " +
                        "USING DELTA LOCATION 's3://%s/%s' " +
                        "PARTITIONED BY (part) " +
                        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'part1')");
            assertThat(onTrino().executeQuery("SELECT col, part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"));

            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN part TO new_part");
            assertThat(onTrino().executeQuery("SELECT col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"));

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 'part2')");
            assertThat(onTrino().executeQuery("SELECT col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, "part1"), row(2, "part2"));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCommentOnTable()
    {
        String tableName = "test_dl_comment_table_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment'");
            assertThat(onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = 'default' AND table_name = '" + tableName + "'"))
                    .containsOnly(row("test comment"));

            assertEquals(getTableCommentOnDelta("default", tableName), "test comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCommentOnTableUnsupportedWriterVersion()
    {
        String tableName = "test_dl_comment_table_unsupported_writer_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='5')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment'"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 5 which is not supported");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCommentOnColumn()
    {
        String tableName = "test_dl_comment_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "col"), "test column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "col"), "test column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCommentOnColumnUnsupportedWriterVersion()
    {
        String tableName = "test_dl_comment_column_unsupported_writer_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='5')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 5 which is not supported");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoAlterTablePreservesTableMetadata()
    {
        String tableName = "test_trino_alter_table_preserves_table_metadata_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.appendOnly' = true)",
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_column INT");

            List<?> properties = getOnlyElement(onDelta().executeQuery("SHOW TBLPROPERTIES " + tableName + "(delta.appendOnly)").rows());
            assertTrue(Boolean.parseBoolean((String) properties.get(1)));
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoAlterTablePreservesChangeDataFeed()
    {
        String tableName = "test_trino_alter_table_preserves_cdf_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("""
                        CREATE TABLE default.%s (col int)
                        USING DELTA LOCATION 's3://%s/%s'
                        TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
                        """,
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_column INT");

            Object enableChangeDataFeed = getOnlyElement(onDelta().executeQuery("SHOW TBLPROPERTIES " + tableName + "(delta.enableChangeDataFeed)").column(2));
            assertEquals(enableChangeDataFeed, "true");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoPreservesReaderAndWriterVersions()
    {
        // Databricks 7.3 doesn't support 'delta.minReaderVersion' and 'delta.minWriterVersion' table properties
        // Writing those properties to protocol entry in COMMENT and ADD COLUMN statements is fine
        String tableName = "test_trino_preserves_versions_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minReaderVersion'='1', 'delta.minWriterVersion'='1', 'delta.checkpointInterval' = 1)",
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET col = 2");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName);
            onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " + "ON (t.col = s.col) WHEN MATCHED THEN UPDATE SET new_col = 3");

            List<?> minReaderVersion = getOnlyElement(onDelta().executeQuery("SHOW TBLPROPERTIES " + tableName + "(delta.minReaderVersion)").rows());
            assertEquals((String) minReaderVersion.get(1), "1");
            List<?> minWriterVersion = getOnlyElement(onDelta().executeQuery("SHOW TBLPROPERTIES " + tableName + "(delta.minWriterVersion)").rows());
            assertEquals((String) minWriterVersion.get(1), "1");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoAlterTablePreservesGeneratedColumn()
    {
        // Databricks 7.3 doesn't support generated columns
        String tableName = "test_trino_alter_table_preserves_generated_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("""
                        CREATE TABLE default.%s (a INT, b INT GENERATED ALWAYS AS (a * 2))
                        USING DELTA LOCATION 's3://%s/%s'
                        """,
                tableName,
                bucketName,
                tableDirectory));
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");

            Assertions.assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                            .contains((getDatabricksRuntimeVersion().orElseThrow().equals(DATABRICKS_91_RUNTIME_VERSION) ? "`b`" : "b") + " INT GENERATED ALWAYS AS ( a * 2 )");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a, c) VALUES (1, 3)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2, 3));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
