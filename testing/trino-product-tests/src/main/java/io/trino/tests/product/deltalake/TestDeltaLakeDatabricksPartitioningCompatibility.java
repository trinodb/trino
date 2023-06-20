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
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDatabricksPartitioningCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn()
    {
        testDatabricksCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1);
        testDatabricksCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20);
    }

    private void testDatabricksCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (id, col_name)" +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = " + interval + ") " +
                        "AS VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCanReadFromCtasTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumn()
    {
        testTrinoCanReadFromCtasTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1);
        testTrinoCanReadFromCtasTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20);
    }

    private void testTrinoCanReadFromCtasTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        onDelta().executeQuery(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = " + interval + ") " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s' AS " +
                        "SELECT * FROM (VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')" +
                        ") t(id, col_name)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn()
    {
        testDatabricksCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1);
        testDatabricksCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20);
    }

    private void testDatabricksCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (id INTEGER, col_name VARCHAR) " +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = " + interval + ") ",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery(format("INSERT INTO delta.default.%s " +
                            "VALUES" +
                            "(1, 'with-hyphen'), " +
                            "(2, 'with.dot'), " +
                            "(3, 'with:colon'), " +
                            "(4, 'with/slash'), " +
                            "(5, 'with\\\\backslash'), " +
                            "(6, 'with=equal'), " +
                            "(7, 'with?question'), " +
                            "(8, 'with!exclamation'), " +
                            "(9, 'with%%percent')," +
                            "(10, 'with space')",
                    tableName));
            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCanReadTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumn()
    {
        testTrinoCanReadTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1);
        testTrinoCanReadTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20);
    }

    private void testTrinoCanReadTableCreatedByDatabricksWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        onDelta().executeQuery(format("CREATE TABLE default.%s (id INTEGER, col_name STRING) " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = " + interval + ") " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s'",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onDelta().executeQuery(format("INSERT INTO default.%s " +
                            "VALUES" +
                            "(1, 'with-hyphen'), " +
                            "(2, 'with.dot'), " +
                            "(3, 'with:colon'), " +
                            "(4, 'with/slash'), " +
                            "(5, 'with\\\\backslash'), " +
                            "(6, 'with=equal'), " +
                            "(7, 'with?question'), " +
                            "(8, 'with!exclamation'), " +
                            "(9, 'with%%percent')," +
                            "(10, 'with space')",
                    tableName));

            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCanReadFromTableUpdatedByTrino()
    {
        testDatabricksCanReadFromTableUpdatedByTrinoWithCpIntervalSet(1);
        testDatabricksCanReadFromTableUpdatedByTrinoWithCpIntervalSet(20);
    }

    private void testDatabricksCanReadFromTableUpdatedByTrinoWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(101, "with-hyphen"),
                row(102, "with.dot"),
                row(103, "with:colon"),
                row(104, "with/slash"),
                row(105, "with\\\\backslash"),
                row(106, "with=equal"),
                row(107, "with?question"),
                row(108, "with!exclamation"),
                row(109, "with%percent"),
                row(110, "with space"));

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (id, col_name) " +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = " + interval + ") " +
                        "AS VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onTrino().executeQuery(format("UPDATE delta.default.%s SET id = id + 100", tableName));

            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCanReadFromTableUpdatedByDatabricks()
    {
        testTrinoCanReadFromTableUpdatedByDatabricksWithCpIntervalSet(1);
        testTrinoCanReadFromTableUpdatedByDatabricksWithCpIntervalSet(20);
    }

    private void testTrinoCanReadFromTableUpdatedByDatabricksWithCpIntervalSet(int interval)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(
                row(101, "with-hyphen"),
                row(102, "with.dot"),
                row(103, "with:colon"),
                row(104, "with/slash"),
                row(105, "with\\backslash"),
                row(106, "with=equal"),
                row(107, "with?question"),
                row(108, "with!exclamation"),
                row(109, "with%percent"),
                row(110, "with space"));

        onDelta().executeQuery(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = " + interval + ") " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s' AS " +
                        "SELECT * FROM (VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')" +
                        ") t(id, col_name)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            onDelta().executeQuery(format("UPDATE default.%s SET id = id + 100", tableName));

            assertThat(onDelta().executeQuery("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCanReadFromTablePartitionChangedByDatabricks()
    {
        String tableName = "test_dl_create_table_partition_changed_by_databricks_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        ImmutableList<QueryAssert.Row> expected = ImmutableList.of(row(1, "part"));

        onDelta().executeQuery(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "PARTITIONED BY (`original_part_col`) LOCATION 's3://%s/%s' AS " +
                        "SELECT 1 AS original_part_col, 'part' AS new_part_col",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);

            onDelta().executeQuery("REPLACE TABLE default." + tableName + " USING DELTA PARTITIONED BY (new_part_col) AS SELECT * FROM " + tableName);

            // This 2nd SELECT query caused NPE when the connector had cache for partitions and the column was changed remotely
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedByNonLowercaseColumn()
    {
        String tableName = "test_dl_partitioned_by_non_lowercase_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "PARTITIONED BY (`PART`) LOCATION 's3://%s/%s' AS " +
                        "SELECT 1 AS data, 2 AS `PART`",
                tableName,
                bucketName,
                tableDirectory));
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(row(1, 2));

            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 4)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(row(1, 2), row(3, 4));

            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE data = 3");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(row(1, 2));

            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET part = 20");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).contains(row(1, 20));

            onTrino().executeQuery("MERGE INTO delta.default." + tableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
