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
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDeltaLakeColumnMappingMode
        extends BaseTestDeltaLakeS3Storage
{
    // TODO: Add test with 'delta.columnMapping.mode'='id' table property. This requires Databricks runtime version 10.2 or higher version.

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNone()
    {
        String tableName = "test_dl_column_mapping_mode_none" + randomTableSuffix();

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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeName()
    {
        String tableName = "test_dl_column_mapping_mode_name_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>, a_string STRING)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='name'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            onDelta().executeQuery("" +
                    "INSERT INTO default." + tableName + " VALUES " +
                    "(1, array(struct('nested 1')), struct('databricks 1'),'ala'), " +
                    "(2, array(struct('nested 2')), struct('databricks 2'), 'kota')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "nested 1", "databricks 1", "ala"),
                    row(2, "nested 2", "databricks 2", "kota"));

            assertThat(onDelta().executeQuery("SELECT a_number, array_col[0].array_struct_element, nested.field1, a_string FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_number, array_col[1].array_struct_element, nested.field1, a_string FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_string FROM delta.default." + tableName + " WHERE a_number = 1"))
                    .containsOnly(ImmutableList.of(row("ala")));
            assertThat(onTrino().executeQuery("SELECT a_number FROM delta.default." + tableName + " WHERE nested.field1 = 'databricks 1'"))
                    .containsOnly(ImmutableList.of(row(1)));

            // Verify the connector can read renamed columns correctly
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN a_number TO new_a_column");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN nested.field1 TO field2");

            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("new_a_column", "integer", "", ""),
                            row("array_col", "array(row(array_struct_element varchar))", "", ""),
                            row("nested", "row(field2 varchar)", "", ""),
                            row("a_string", "varchar", "", "")));

            assertThat(onDelta().executeQuery("SELECT new_a_column, array_col[0].array_struct_element, nested.field2, a_string FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT new_a_column, array_col[1].array_struct_element, nested.field2, a_string FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_column_mapping_mode_name_non_loewr_case_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (`mIxEd_CaSe` INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='name'," +
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsFromJsonForColumnMappingModeName()
    {
        String tableName = "test_dl_show_stats_json_for_column_mapping_mode_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'" +
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsFromParquetForColumnMappingModeName()
    {
        String tableName = "test_dl_show_parquet_stats_parquet_for_column_mapping_mode_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'," +
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsOnPartitionedForColumnMappingModeName()
    {
        String tableName = "test_dl_show_stats_partitioned_for_column_mapping_mode_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'," +
                " 'checkpointInterval' = 3" +
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    public void testUnsupportedOperationsColumnMappingModeName()
    {
        String tableName = "test_dl_unsupported_column_mapping_mode_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='name'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'one'), (2, 'two')"))
                    .hasMessageContaining("Delta Lake writer version 5 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM default." + tableName))
                    .hasMessageContaining("Delta Lake writer version 5 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE default." + tableName + " SET a_string = 'test'"))
                    .hasMessageContaining("Delta Lake writer version 5 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE default." + tableName + " EXECUTE OPTIMIZE"))
                    .hasMessageContaining("Delta Lake writer version 5 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN new_col varchar"))
                    .hasMessageContaining("Delta Lake writer version 5 which is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN a_number TO renamed_column"))
                    .hasMessageContaining("This connector does not support renaming columns");
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN a_number"))
                    .hasMessageContaining("This connector does not support dropping columns");
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }
}
