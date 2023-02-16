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
import io.trino.testng.services.Flaky;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDeltaLakeColumnMappingMode
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnMappingModeNone()
    {
        String tableName = "test_dl_column_mapping_mode_none" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1)");

            List<Row> expectedRows = ImmutableList.of(row(1));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnMappingMode(String mode)
    {
        String tableName = "test_dl_column_mapping_mode_name_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>, a_string STRING, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            onDelta().executeQuery("" +
                    "INSERT INTO default." + tableName + " VALUES " +
                    "(1, array(struct('nested 1')), struct('databricks 1'),'ala', 'part1'), " +
                    "(2, array(struct('nested 2')), struct('databricks 2'), 'kota', 'part2')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "nested 1", "databricks 1", "ala", "part1"),
                    row(2, "nested 2", "databricks 2", "kota", "part2"));

            assertThat(onDelta().executeQuery("SELECT a_number, array_col[0].array_struct_element, nested.field1, a_string, part FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_number, array_col[1].array_struct_element, nested.field1, a_string, part FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_string FROM delta.default." + tableName + " WHERE a_number = 1"))
                    .containsOnly(ImmutableList.of(row("ala")));
            assertThat(onTrino().executeQuery("SELECT a_number FROM delta.default." + tableName + " WHERE nested.field1 = 'databricks 1'"))
                    .containsOnly(ImmutableList.of(row(1)));
            assertThat(onTrino().executeQuery("SELECT a_number FROM delta.default." + tableName + " WHERE part = 'part1'"))
                    .containsOnly(row(1));
            assertThat(onDelta().executeQuery("SELECT a_number FROM default." + tableName + " WHERE part = 'part1'"))
                    .containsOnly(row(1));

            // Verify the connector can read renamed columns correctly
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN a_number TO new_a_column");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN nested.field1 TO field2");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN part TO new_part");

            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("new_a_column", "integer", "", ""),
                            row("array_col", "array(row(array_struct_element varchar))", "", ""),
                            row("nested", "row(field2 varchar)", "", ""),
                            row("a_string", "varchar", "", ""),
                            row("new_part", "varchar", "", "")));

            assertThat(onDelta().executeQuery("SELECT new_a_column, array_col[0].array_struct_element, nested.field2, a_string, new_part FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT new_a_column, array_col[1].array_struct_element, nested.field2, a_string, new_part FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnMappingModeNameWithNonLowerCaseColumn(String mode)
    {
        String tableName = "test_dl_column_mapping_mode_name_non_loewr_case_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (`mIxEd_CaSe` INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (0), (9)");

            List<Row> expectedRows = ImmutableList.of(row(0), row(9));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("mixed_case", null, null, 0.0, null, "0", "9"),
                            row(null, null, null, null, 2.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnMappingModeAddColumn(String mode)
    {
        String tableName = "test_dl_column_mapping_mode_add_column_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1), (2)");

            List<Row> expectedRows = ImmutableList.of(row(1), row(2));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            // Verify the connector can read added columns correctly
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN another_varchar STRING");
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .containsOnly(
                            row("a_number", "integer", "", ""),
                            row("another_varchar", "varchar", "", ""));

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES  (3, 'new column')");
            expectedRows = ImmutableList.of(
                    row(1, null),
                    row(2, null),
                    row(3, "new column"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            // Replace table to partition by added column for testing predicate pushdown on the column
            onDelta().executeQuery("REPLACE TABLE default." + tableName + " USING DELTA PARTITIONED BY (another_varchar) AS SELECT * FROM " + tableName);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE another_varchar = 'new column'"))
                    .containsOnly(row(3, "new column"));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE another_varchar = 'expect no rows'"))
                    .hasNoRows();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE another_varchar <> 'new column'"))
                    .hasNoRows();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE another_varchar IS NOT NULL"))
                    .containsOnly(row(3, "new column"));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE another_varchar IS NULL"))
                    .containsOnly(row(1, null), row(2, null));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testShowStatsFromJsonForColumnMappingMode(String mode)
    {
        String tableName = "test_dl_show_stats_json_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1), (2), (null)");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, 0.33333333333, null, "1", "2"),
                            row(null, null, null, null, 3.0, null, null)));

            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.00000000000, 0.33333333333, null, "1", "2"),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testShowStatsFromParquetForColumnMappingMode(String mode)
    {
        String tableName = "test_dl_show_parquet_stats_parquet_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'," +
                " 'delta.checkpointInterval' = 3" +
                ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (0)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null)");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, 0.33333333333, null, "0", "1"),
                            row(null, null, null, null, 3.0, null, null)));

            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.00000000000, 0.33333333333, null, "0", "1"),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testShowStatsOnPartitionedForColumnMappingMode(String mode)
    {
        String tableName = "test_dl_show_stats_partitioned_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'," +
                " 'delta.checkpointInterval' = 3" +
                ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (0, 'a')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'b')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null, null)");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, 0.33333333333, null, "0", "1"),
                            row("part", null, 2.00000000000, 0.33333333333, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDropAndAddColumnShowStatsForColumnMappingMode(String mode)
    {
        String tableName = "test_dl_drop_add_column_show_stats_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, b_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 10), (2, 20), (null, null)");
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", null, 2.0, 0.33333333333, null, "10", "20"),
                            row(null, null, null, null, 3.0, null, null)));

            // Ensure SHOW STATS doesn't return stats for the restored column
            onDelta().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN b_number");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN b_number INT");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", null, null, null, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));

            // SHOW STATS returns the expected stats after executing ANALYZE
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", 0.0, 0.0, 1.0, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testChangeColumnMappingAndShowStatsForColumnMappingMode()
    {
        String tableName = "test_dl_change_column_mapping_and_show_stats_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, b_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='none'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 10), (2, 20), (null, null)");
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", null, 2.0, 0.33333333333, null, "10", "20"),
                            row(null, null, null, null, 3.0, null, null));

            onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='name')");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN b_number");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN b_number INT");

            // Ensure SHOW STATS doesn't return stats for the restored column
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", null, null, null, null, null, null),
                            row(null, null, null, null, 3.0, null, null));

            // SHOW STATS returns the expected stats after executing ANALYZE
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("b_number", 0.0, 0.0, 1.0, null, null, null),
                            row(null, null, null, null, 3.0, null, null));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "changeColumnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testChangeColumnMappingMode(String sourceMappingMode, String targetMappingMode, boolean supported)
    {
        String tableName = "test_dl_change_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + sourceMappingMode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");
        try {
            if (supported) {
                onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='" + targetMappingMode + "')");
            }
            else {
                assertQueryFailure(() -> onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='" + targetMappingMode + "')"))
                        .hasMessageContaining("Changing column mapping mode from '%s' to '%s' is not supported".formatted(sourceMappingMode, targetMappingMode));
            }
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public Object[][] changeColumnMappingDataProvider()
    {
        // Update testChangeColumnMappingAndShowStatsForColumnMappingMode if Delta Lake changes their behavior
        return new Object[][] {
                // sourceMappingMode targetMappingMode supported
                {"none", "id", false},
                {"none", "name", true},
                {"id", "none", false},
                {"id", "name", false},
                {"name", "none", false},
                {"name", "id", false},
        };
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testUnsupportedOperationsColumnMappingMode(String mode)
    {
        String tableName = "test_dl_unsupported_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            if (!mode.equals("id")) {
                assertThat(onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'one'), (2, 'two')"))
                        .updatedRowsCountIsEqualTo(2);
                assertThat(onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                        .updatedRowsCountIsEqualTo(2);
                assertThat(onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_string = 'test'"))
                        .updatedRowsCountIsEqualTo(0);
            }
            else {
                assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'one'), (2, 'two')"))
                        .hasMessageContaining("Writing with column mapping id is not supported");
                assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                        .hasMessageContaining("Writing with column mapping id is not supported");
                assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_string = 'test'"))
                        .hasMessageContaining("Writing with column mapping id is not supported");
            }
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment'"))
                    .hasMessageContaining("Setting a table comment with column mapping %s is not supported".formatted(mode));
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".a_number IS 'test comment'"))
                    .hasMessageContaining("Setting a column comment with column mapping %s is not supported".formatted(mode));
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " EXECUTE OPTIMIZE"))
                    .hasMessageContaining("Executing 'optimize' procedure with column mapping %s is not supported".formatted(mode));
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col varchar"))
                    .hasMessageContaining("Adding a column with column mapping %s is not supported".formatted(mode));
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN a_number TO renamed_column"))
                    .hasMessageContaining("This connector does not support renaming columns");
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN a_number"))
                    .hasMessageContaining("This connector does not support dropping columns");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testSpecialCharacterColumnNamesWithColumnMappingMode(String mode)
    {
        String tableName = "test_dl_special_character_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (`;{}()\\n\\t=` INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'," +
                " 'delta.checkpointInterval' = 3" +
                ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (0)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null)");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(";{}()\\n\\t=", null, null, 0.33333333333, null, "0", "1"),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingWithTrueAndFalseDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testSupportedNonPartitionedColumnMappingWrites(String mode, boolean statsAsJsonEnabled)
    {
        String tableName = "test_dl_dml_column_mapping_mode_" + mode + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.checkpointInterval' = 1, " +
                " 'delta.checkpoint.writeStatsAsJson' = " + statsAsJsonEnabled + ", " +
                " 'delta.checkpoint.writeStatsAsStruct' = " + !statsAsJsonEnabled + ", " +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");

        try {
            String trinoColumns = "a_number, a_string, array_col[1].array_struct_element, nested.field1";
            String deltaColumns = "a_number, a_string, array_col[0].array_struct_element, nested.field1";

            onTrino().executeQuery("INSERT INTO delta.default." + tableName +
                    " VALUES (1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                    "        (3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                    "        (4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))");
            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3, "third value", "nested 3", "databricks 3"),
                    row(4, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 4.0, 0.0, null, "1", "4"),
                            row("a_string", 29.0, 4.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 4.0, null, null)));

            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_number = a_number + 10 WHERE a_number in (3, 4)");
            onDelta().executeQuery("UPDATE default." + tableName + " SET a_number = a_number + 20 WHERE a_number in (1, 2)");
            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(21, "first value", "nested 1", "databricks 1"),
                    row(22, "two", "nested 2", "databricks 2"),
                    row(13, "third value", "nested 3", "databricks 3"),
                    row(14, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 4.0, 0.0, null, "13", "22"),
                            row("a_string", 29.0, 4.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 4.0, null, null)));

            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_number = 22");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_number = 13");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a_number = 21");
            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(14, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, "14", "14"),
                            row("a_string", 29.0, 1.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingWithTrueAndFalseDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testSupportedPartitionedColumnMappingWrites(String mode, boolean statsAsJsonEnabled)
    {
        String tableName = "test_dl_dml_column_mapping_mode_" + mode + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " PARTITIONED BY (a_string)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.checkpointInterval' = 1, " +
                " 'delta.checkpoint.writeStatsAsJson' = " + statsAsJsonEnabled + ", " +
                " 'delta.checkpoint.writeStatsAsStruct' = " + !statsAsJsonEnabled + ", " +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");

        try {
            String trinoColumns = "a_number, a_string, array_col[1].array_struct_element, nested.field1";
            String deltaColumns = "a_number, a_string, array_col[0].array_struct_element, nested.field1";

            onTrino().executeQuery("INSERT INTO delta.default." + tableName +
                    " VALUES (1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                    "        (3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                    "        (4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))");

            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3, "third value", "nested 3", "databricks 3"),
                    row(4, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 4.0, 0.0, null, "1", "4"),
                            row("a_string", null, 4.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 4.0, null, null)));

            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_number = a_number + 10 WHERE a_number in (3, 4)");
            onDelta().executeQuery("UPDATE default." + tableName + " SET a_number = a_number + 20 WHERE a_number in (1, 2)");
            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(21, "first value", "nested 1", "databricks 1"),
                    row(22, "two", "nested 2", "databricks 2"),
                    row(13, "third value", "nested 3", "databricks 3"),
                    row(14, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 4.0, 0.0, null, "13", "22"),
                            row("a_string", null, 4.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 4.0, null, null)));

            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_number = 22");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_number = 13");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a_number = 21");
            assertDeltaTrinoTableEquals(tableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(14, "four", "nested 4", "databricks 4")));

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, "14", "14"),
                            row("a_string", null, 1.0, 0.0, null, null, null),
                            row("array_col", null, null, null, null, null, null),
                            row("nested", null, null, null, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedColumnMappingForDmlDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testMergeUpdateWithColumnMapping(String mode)
    {
        String sourceTableName = "test_merge_update_source_column_mapping_mode_" + randomNameSuffix();
        String targetTableName = "test_merge_update_target_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + targetTableName + " (nationkey INT, name STRING, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + targetTableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        onDelta().executeQuery("CREATE TABLE default." + sourceTableName + " (nationkey INT, name STRING, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + sourceTableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + targetTableName + " VALUES (1, 'nation1', 100), (2, 'nation2', 200), (3, 'nation3', 300)");
            onDelta().executeQuery("INSERT INTO default." + sourceTableName + " VALUES (1000, 'nation1000', 1000), (2, 'nation2', 20000), (3000, 'nation3000', 3000)");

            onTrino().executeQuery("MERGE INTO delta.default." + targetTableName + " target USING delta.default." + sourceTableName + " source " +
                    "ON (target.nationkey = source.nationkey) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET nationkey = (target.nationkey + source.nationkey + source.regionkey) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (nationkey, name, regionkey) VALUES (source.nationkey, source.name, source.regionkey)");

            assertThat(onDelta().executeQuery("SELECT * FROM " + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300),
                            row(20004, "nation2", 200));
        }
        finally {
            dropDeltaTableWithRetry("default." + targetTableName);
            dropDeltaTableWithRetry("default." + sourceTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedColumnMappingForDmlDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testMergeDeleteWithColumnMapping(String mode)
    {
        String sourceTableName = "test_dl_merge_delete_source_column_mapping_mode_" + mode + randomNameSuffix();
        String targetTableName = "test_dl_merge_delete_target_column_mapping_mode_" + mode + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + sourceTableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " PARTITIONED BY (a_string)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + sourceTableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' ='" + mode + "')");

        onDelta().executeQuery("" +
                "CREATE TABLE default." + targetTableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " PARTITIONED BY (a_string)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + targetTableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' ='" + mode + "')");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + sourceTableName +
                    " VALUES (1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                    "        (3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                    "        (4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))");

            String trinoColumns = "a_number, a_string, array_col[1].array_struct_element, nested.field1";
            String deltaColumns = "a_number, a_string, array_col[0].array_struct_element, nested.field1";
            assertDeltaTrinoTableEquals(sourceTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3, "third value", "nested 3", "databricks 3"),
                    row(4, "four", "nested 4", "databricks 4")));

            onTrino().executeQuery("INSERT INTO delta.default." + targetTableName +
                    " VALUES (1000, '1000 value', ARRAY[ROW('nested 1000')], ROW('databricks 1000'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))");
            onDelta().executeQuery("INSERT INTO default." + targetTableName +
                    " VALUES (3000, '3000 value', array(struct('nested 3000')), struct('databricks 3000'))," +
                    "        (4, 'four', array(struct('nested 4')), struct('databricks 4'))");

            assertDeltaTrinoTableEquals(targetTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1000, "1000 value", "nested 1000", "databricks 1000"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3000, "3000 value", "nested 3000", "databricks 3000"),
                    row(4, "four", "nested 4", "databricks 4")));

            onTrino().executeQuery("MERGE INTO delta.default." + targetTableName + " t USING delta.default." + sourceTableName + " s " +
                    "ON (t.a_number = s.a_number) " +
                    "WHEN MATCHED " +
                    "THEN DELETE " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (a_number, a_string, array_col, nested) VALUES (s.a_number, s.a_string, s.array_col, s.nested)");

            assertDeltaTrinoTableEquals(targetTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1000, "1000 value", "nested 1000", "databricks 1000"),
                    row(3000, "3000 value", "nested 3000", "databricks 3000"),
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(3, "third value", "nested 3", "databricks 3")));
        }
        finally {
            dropDeltaTableWithRetry("default." + sourceTableName);
            dropDeltaTableWithRetry("default." + targetTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testUnsupportedColumnMappingModeChangeDataFeed(String mode)
    {
        String sourceTableName = "test_dl_cdf_target_column_mapping_mode_" + randomNameSuffix();
        String targetTableName = "test_dl_cdf_source_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + targetTableName +
                " (nationkey INT, name STRING, regionkey INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + targetTableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.enableChangeDataFeed' = true" +
                ")");
        onDelta().executeQuery("CREATE TABLE default." + sourceTableName + " (nationkey INT, name STRING, regionkey INT) " +
                " USING DELTA" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + sourceTableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + targetTableName + " VALUES (1, 'nation1', 100)");
            onDelta().executeQuery("INSERT INTO default." + targetTableName + " VALUES (2, 'nation2', 200)");
            onDelta().executeQuery("INSERT INTO default." + targetTableName + " VALUES (3, 'nation3', 300)");

            // Column mapping mode 'none' is tested in TestDeltaLakeDatabricksChangeDataFeedCompatibility
            // TODO: Remove these failure check and update TestDeltaLakeDatabricksChangeDataFeedCompatibility when adding support the column mapping mode
            if (mode.equals("id")) {
                assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + targetTableName + " SET regionkey = 10"))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: id");
                assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + targetTableName))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: id");
                assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + targetTableName + " cdf USING delta.default." + sourceTableName + " n " +
                        "ON (cdf.nationkey = n.nationkey) " +
                        "WHEN MATCHED " +
                        "THEN UPDATE SET nationkey = (cdf.nationkey + n.nationkey + n.regionkey) " +
                        "WHEN NOT MATCHED " +
                        "THEN INSERT (nationkey, name, regionkey) VALUES (n.nationkey, n.name, n.regionkey)"))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: id");
            }
            else if (mode.equals("name")) {
                assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + targetTableName + " SET regionkey = 10"))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: " + mode);
                assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + targetTableName))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: " + mode);
                assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + targetTableName + " cdf USING delta.default." + sourceTableName + " n " +
                        "ON (cdf.nationkey = n.nationkey) " +
                        "WHEN MATCHED " +
                        "THEN UPDATE SET nationkey = (cdf.nationkey + n.nationkey + n.regionkey) " +
                        "WHEN NOT MATCHED " +
                        "THEN INSERT (nationkey, name, regionkey) VALUES (n.nationkey, n.name, n.regionkey)"))
                        .hasMessageContaining("Unsupported column mapping mode for tables with change data feed enabled: " + mode);
            }

            assertThat(onDelta().executeQuery("SELECT nationkey, name, regionkey, _change_type, _commit_version " +
                    "FROM table_changes('default." + targetTableName + "', 0)"))
                    .containsOnly(
                            row(1, "nation1", 100, "insert", 1),
                            row(2, "nation2", 200, "insert", 2),
                            row(3, "nation3", 300, "insert", 3));
        }
        finally {
            dropDeltaTableWithRetry("default." + targetTableName);
        }
    }

    private void assertDeltaTrinoTableEquals(String tableName, String trinoQuery, String deltaQuery, List<Row> expectedRows)
    {
        assertThat(onDelta().executeQuery("SELECT " + deltaQuery + " FROM default." + tableName))
                .containsOnly(expectedRows);
        assertThat(onTrino().executeQuery("SELECT " + trinoQuery + " FROM delta.default." + tableName))
                .containsOnly(expectedRows);
    }

    @DataProvider
    public Object[][] columnMappingWithTrueAndFalseDataProvider()
    {
        return cartesianProduct(supportedColumnMappingForDmlDataProvider(), trueFalse());
    }

    @DataProvider
    public Object[][] columnMappingDataProvider()
    {
        return new Object[][] {
                {"id"},
                {"name"},
        };
    }

    @DataProvider
    public Object[][] supportedColumnMappingForDmlDataProvider()
    {
        // TODO: Support 'id' column mapping
        return new Object[][] {
                {"none"},
                {"name"},
        };
    }
}
