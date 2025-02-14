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
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_104_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_113_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnTrino;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeDatabricksCreateTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private DatabricksVersion databricksRuntimeVersion;

    @BeforeMethodWithContext
    public void setup()
    {
        databricksRuntimeVersion = getDatabricksRuntimeVersion().orElseThrow();
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreateTable()
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) with (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM default." + tableName)).contains(row(0));
            String showCreateTable;
            if (databricksRuntimeVersion.isAtLeast(DATABRICKS_104_RUNTIME_VERSION)) {
                showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        bucketName,
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            }
            else {
                showCreateTable = format(
                        "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\nLOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n  'delta.enableDeletionVectors' = 'false')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE default." + tableName))
                    .containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of());
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreatePartitionedTable()
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(
                format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) " +
                                "with (location = 's3://%s/%s', partitioned_by = ARRAY['string'])",
                        tableName,
                        bucketName,
                        tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM " + tableName)).contains(row(0));
            String showCreateTable;
            if (databricksRuntimeVersion.isAtLeast(DATABRICKS_104_RUNTIME_VERSION)) {
                showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        bucketName,
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            }
            else {
                showCreateTable = format(
                        "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n  'delta.enableDeletionVectors' = 'false')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of());
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreateTableAs()
    {
        String tableName = "test_dl_create_table_as_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s') AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM default." + tableName)).contains(row(3));
            String showCreateTable;
            if (databricksRuntimeVersion.isAtLeast(DATABRICKS_104_RUNTIME_VERSION)) {
                showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        bucketName,
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            }
            else {
                showCreateTable = format(
                        "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\nLOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n  'delta.enableDeletionVectors' = 'false')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE default." + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreatePartitionedTableAs()
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s', partitioned_by = ARRAY['string']) AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM " + tableName)).contains(row(3));
            String showCreateTable;
            if (databricksRuntimeVersion.isAtLeast(DATABRICKS_104_RUNTIME_VERSION)) {
                showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        bucketName,
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            }
            else {
                showCreateTable = format(
                        "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n  'delta.enableDeletionVectors' = 'false')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    private void testInsert(String tableName, List<QueryAssert.Row> existingRows)
    {
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'one', TIMESTAMP '2960-10-31 01:00:00')");
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 'two', TIMESTAMP '2020-10-31 01:00:00')");
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'three', TIMESTAMP '1900-10-31 01:00:00')");

        ImmutableList.Builder<QueryAssert.Row> expected = ImmutableList.builder();
        expected.addAll(existingRows);
        expected.add(row(1, "one", "2960-10-31T01:00:00.000Z"));
        expected.add(row(2, "two", "2020-10-31T01:00:00.000Z"));
        expected.add(row(3, "three", "1900-10-31T01:00:00.000Z"));

        assertThat(onTrino().executeQuery("SELECT integer, string, to_iso8601(timetz) FROM delta.default." + tableName))
                .containsOnly(expected.build());
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithTableComment()
    {
        String tableName = "test_dl_create_table_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT) COMMENT 'test comment' WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(getTableCommentOnTrino("default", tableName)).isEqualTo("test comment");
            assertThat(getTableCommentOnDelta("default", tableName)).isEqualTo("test comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithColumnCommentOnTrino()
    {
        String tableName = "test_dl_create_column_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (col INT COMMENT 'test comment') WITH (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(getColumnCommentOnTrino("default", tableName, "col")).isEqualTo("test comment");
            assertThat(getColumnCommentOnDelta("default", tableName, "col")).isEqualTo("test comment");

            // Verify that adding a new column doesn't remove existing column comments
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            assertThat(getColumnCommentOnTrino("default", tableName, "col")).isEqualTo("test comment");
            assertThat(getColumnCommentOnDelta("default", tableName, "col")).isEqualTo("test comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithColumnCommentOnDelta()
    {
        String tableName = "test_dl_create_column_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("CREATE TABLE default.%s (col INT COMMENT 'test comment') USING DELTA LOCATION 's3://%s/%s'",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(getColumnCommentOnTrino("default", tableName, "col")).isEqualTo("test comment");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithDuplicatedColumnNames()
    {
        String tableName = "test_dl_create_table_with_duplicated_column_names_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + " (col INT, COL int) USING DELTA LOCATION 's3://" + bucketName + "/" + tableDirectory + "'"))
                    .hasMessageMatching("(?s).*(Found duplicate column|The column `col` already exists).*");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithUnsupportedPartitionType()
    {
        String tableName = "test_dl_create_table_with_unsupported_column_types_" + randomNameSuffix();
        String tableLocation = "s3://%s/databricks-compatibility-test-%s".formatted(bucketName, tableName);
        try {
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part ARRAY(INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part MAP(INT,INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part ROW(field INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");

            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + "(a INT, part ARRAY<INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + "(a INT, part MAP<INT,INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> onDelta().executeQuery(
                    "CREATE TABLE default." + tableName + "(a INT, part STRUCT<field: INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithAllPartitionColumns()
    {
        String tableName = "test_dl_create_table_with_all_partition_columns_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + "(part int)" +
                    "WITH (partitioned_by = ARRAY['part'], location = 's3://" + bucketName + "/" + tableDirectory + "')"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");
            assertThatThrownBy(() -> onTrino().executeQuery("" +
                    "CREATE TABLE delta.default." + tableName + "(part int, another_part int)" +
                    "WITH (partitioned_by = ARRAY['part', 'another_part'], location = 's3://" + bucketName + "/" + tableDirectory + "')"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");

            assertThatThrownBy(() -> onDelta().executeQuery("" +
                    "CREATE TABLE default." + tableName + "(part int)" +
                    "USING DELTA " +
                    "PARTITIONED BY (part)" +
                    "LOCATION 's3://" + bucketName + "/" + tableDirectory + "'"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
            assertThatThrownBy(() -> onDelta().executeQuery("" +
                    "CREATE TABLE default." + tableName + "(part int, another_part int)" +
                    "USING DELTA " +
                    "PARTITIONED BY (part, another_part)" +
                    "LOCATION 's3://" + bucketName + "/" + tableDirectory + "'"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    private String getDatabricksDefaultTableProperties()
    {
        if (databricksRuntimeVersion.equals(DATABRICKS_104_RUNTIME_VERSION)) {
            return "TBLPROPERTIES (\n" +
                    "  'Type' = 'EXTERNAL',\n" +
                    "  'delta.enableDeletionVectors' = 'false',\n" +
                    "  'delta.minReaderVersion' = '1',\n" +
                    "  'delta.minWriterVersion' = '2')\n";
        }
        if (databricksRuntimeVersion.isAtLeast(DATABRICKS_113_RUNTIME_VERSION)) {
            return "TBLPROPERTIES (\n" +
                    "  'delta.enableDeletionVectors' = 'false',\n" +
                    "  'delta.minReaderVersion' = '1',\n" +
                    "  'delta.minWriterVersion' = '2')\n";
        }
        throw new IllegalArgumentException("Unsupported databricks runtime version: " + databricksRuntimeVersion);
    }
}
