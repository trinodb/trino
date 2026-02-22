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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getColumnCommentOnDatabricks;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getTableCommentOnDatabricks;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getTableCommentOnTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeDatabricksCreateTableCompatibility
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreateTable(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) with (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(env.executeDatabricksSql("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(env.executeDatabricksSql("SELECT count(*) FROM default." + tableName)).contains(row(0));
            String showCreateTable = format(
                    "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\nLOCATION 's3://%s/%s'\n%s",
                    tableName,
                    env.getBucketName(),
                    tableDirectory,
                    getDatabricksDefaultTableProperties());
            assertThat(env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName))
                    .containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of(), env);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreatePartitionedTable(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(
                format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) " +
                                "with (location = 's3://%s/%s', partitioned_by = ARRAY['string'])",
                        tableName,
                        env.getBucketName(),
                        tableDirectory));

        try {
            assertThat(env.executeDatabricksSql("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(env.executeDatabricksSql("SELECT count(*) FROM " + tableName)).contains(row(0));
            String showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        env.getBucketName(),
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            assertThat(env.executeDatabricksSql("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of(), env);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreateTableAs(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_as_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s') AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(env.executeDatabricksSql("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(env.executeDatabricksSql("SELECT count(*) FROM default." + tableName)).contains(row(3));
            String showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        env.getBucketName(),
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            assertThat(env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)),
                    env);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadInitialCreatePartitionedTableAs(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s', partitioned_by = ARRAY['string']) AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(env.executeDatabricksSql("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(env.executeDatabricksSql("SELECT count(*) FROM " + tableName)).contains(row(3));
            String showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n  integer INT,\n  string STRING,\n  timetz TIMESTAMP)\nUSING delta\n" +
                                "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n%s",
                        tableName,
                        env.getBucketName(),
                        tableDirectory,
                        getDatabricksDefaultTableProperties());
            assertThat(env.executeDatabricksSql("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)),
                    env);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private void testInsert(String tableName, List<Row> existingRows, DeltaLakeDatabricksEnvironment env)
    {
        env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 'one', TIMESTAMP '2960-10-31 01:00:00')");
        env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (2, 'two', TIMESTAMP '2020-10-31 01:00:00')");
        env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, 'three', TIMESTAMP '1900-10-31 01:00:00')");

        ImmutableList.Builder<Row> expected = ImmutableList.builder();
        expected.addAll(existingRows);
        expected.add(row(1, "one", "2960-10-31T01:00:00.000Z"));
        expected.add(row(2, "two", "2020-10-31T01:00:00.000Z"));
        expected.add(row(3, "three", "1900-10-31T01:00:00.000Z"));

        assertThat(env.executeTrinoSql("SELECT integer, string, to_iso8601(timetz) FROM delta.default." + tableName))
                .containsOnly(expected.build());
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithTableComment(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (col INT) COMMENT 'test comment' WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(getTableCommentOnTrino(env, "default", tableName)).isEqualTo("test comment");
            assertThat(getTableCommentOnDatabricks(env, "default", tableName)).isEqualTo("test comment");
        }
        finally {
            env.executeTrinoSql("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithColumnCommentOnTrino(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_column_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (col INT COMMENT 'test comment') WITH (location = 's3://%s/%s')",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test comment");
            assertThat(getColumnCommentOnDatabricks(env, "default", tableName, "col")).isEqualTo("test comment");

            // Verify that adding a new column doesn't remove existing column comments
            env.executeTrinoSql("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test comment");
            assertThat(getColumnCommentOnDatabricks(env, "default", tableName, "col")).isEqualTo("test comment");
        }
        finally {
            env.executeTrinoSql("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithColumnCommentOnDelta(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_column_comment_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeDatabricksSql(format("CREATE TABLE default.%s (col INT COMMENT 'test comment') USING DELTA LOCATION 's3://%s/%s'",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test comment");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithDuplicatedColumnNames(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_with_duplicated_column_names_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> env.executeDatabricksSql(
                    "CREATE TABLE default." + tableName + " (col INT, COL int) USING DELTA LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'"))
                    .hasMessageMatching("(?s).*(Found duplicate column|The column `col` already exists).*");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithUnsupportedPartitionType(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_with_unsupported_column_types_" + randomNameSuffix();
        String tableLocation = "s3://%s/databricks-compatibility-test-%s".formatted(env.getBucketName(), tableName);
        try {
            assertThatThrownBy(() -> env.executeTrinoSql("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part ARRAY(INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoSql("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part MAP(INT,INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoSql("" +
                    "CREATE TABLE delta.default." + tableName + "(a INT, part ROW(field INT)) WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "')"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");

            assertThatThrownBy(() -> env.executeDatabricksSql(
                    "CREATE TABLE default." + tableName + "(a INT, part ARRAY<INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> env.executeDatabricksSql(
                    "CREATE TABLE default." + tableName + "(a INT, part MAP<INT,INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
            assertThatThrownBy(() -> env.executeDatabricksSql(
                    "CREATE TABLE default." + tableName + "(a INT, part STRUCT<field: INT>) USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "'"))
                    .hasMessageMatching("(?s).*(Cannot use .* for partition column|Using column part of type .* as a partition column is not supported).*");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCreateTableWithAllPartitionColumns(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_create_table_with_all_partition_columns_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> env.executeTrinoSql("" +
                    "CREATE TABLE delta.default." + tableName + "(part int)" +
                    "WITH (partitioned_by = ARRAY['part'], location = 's3://" + env.getBucketName() + "/" + tableDirectory + "')"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoSql("" +
                    "CREATE TABLE delta.default." + tableName + "(part int, another_part int)" +
                    "WITH (partitioned_by = ARRAY['part', 'another_part'], location = 's3://" + env.getBucketName() + "/" + tableDirectory + "')"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");

            assertThatThrownBy(() -> env.executeDatabricksSql("" +
                    "CREATE TABLE default." + tableName + "(part int)" +
                    "USING DELTA " +
                    "PARTITIONED BY (part)" +
                    "LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
            assertThatThrownBy(() -> env.executeDatabricksSql("" +
                    "CREATE TABLE default." + tableName + "(part int, another_part int)" +
                    "USING DELTA " +
                    "PARTITIONED BY (part, another_part)" +
                    "LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'"))
                    .hasMessageContaining("Cannot use all columns for partition columns");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private static String getDatabricksDefaultTableProperties()
    {
        return "TBLPROPERTIES (\n" +
                "  'delta.enableDeletionVectors' = 'false',\n" +
                "  'delta.minReaderVersion' = '1',\n" +
                "  'delta.minWriterVersion' = '2')\n";
    }
}
