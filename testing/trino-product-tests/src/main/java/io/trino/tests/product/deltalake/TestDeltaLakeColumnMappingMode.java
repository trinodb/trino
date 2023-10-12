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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_122;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnNamesOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTablePropertiesOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTablePropertyOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeColumnMappingMode
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_104, DELTA_LAKE_DATABRICKS_113, DELTA_LAKE_DATABRICKS_122, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnMappingModeNone()
    {
        String tableName = "test_dl_column_mapping_mode_none" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, struct('nested 1'))");

            List<Row> expectedRows = ImmutableList.of(row(1, "nested 1"));
            assertThat(onDelta().executeQuery("SELECT a_number, nested.field1 FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_number, nested.field1 FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedColumnMappingForDmlDataProvider")
    public void testColumnMappingModeTableFeature(String mode)
    {
        String tableName = "test_dl_column_mapping_mode_table_feature" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (col INT)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.feature.columnMapping'='supported', 'delta.columnMapping.mode'='" + mode + "')");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES 1");
            assertThat(getTablePropertiesOnDelta("default", tableName))
                    .contains(entry("delta.feature.columnMapping", "supported"))
                    .contains(entry("delta.columnMapping.mode", mode));

            // delta.feature.columnMapping still exists even after unsetting the property
            onDelta().executeQuery("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.feature.columnMapping')");
            assertThat(getTablePropertiesOnDelta("default", tableName))
                    .contains(entry("delta.feature.columnMapping", "supported"));

            // Unsetting delta.columnMapping.mode means changing to 'none' column mapping mode
            if (mode.equals("none")) {
                onDelta().executeQuery("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.columnMapping.mode')");
                assertThat(getTablePropertiesOnDelta("default", tableName))
                        .contains(entry("delta.feature.columnMapping", "supported"))
                        .doesNotContainKey("delta.columnMapping.mode");
                assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue())
                        .doesNotContain("column_mapping_mode =");
            }
            else {
                assertQueryFailure(() -> onDelta().executeQuery("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.columnMapping.mode')"))
                        .hasMessageContaining("Changing column mapping mode from '" + mode + "' to 'none' is not supported");
                assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue())
                        .contains("column_mapping_mode = '" + mode.toUpperCase(ENGLISH) + "'");
            }
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testTrinoColumnMappingModeReaderAndWriterVersion(String mode)
    {
        testColumnMappingModeReaderAndWriterVersion(tableName -> onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")"));
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testChangingColumnMappingModeViaCreateOrReplaceTableOnTrino(String mode)
    {
        String tableName = "test_cortas_column_mapping_" + randomNameSuffix();
        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                ")");

        assertTableReaderAndWriterVersion("default", tableName, "1", "2");

        // Replace table with a different column mode
        onTrino().executeQuery("" +
                "CREATE OR REPLACE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")");

        assertTableReaderAndWriterVersion("default", tableName, "2", "5");

        // Revert back to `none` column mode
        onTrino().executeQuery("" +
                "CREATE OR REPLACE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'none'" +
                ")");

        assertTableReaderAndWriterVersion("default", tableName, "2", "5");

        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testChangingColumnMappingModeViaCreateOrReplaceTableOnDelta(String mode)
    {
        String tableName = "test_cortas_column_mapping_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        assertTableReaderAndWriterVersion("default", tableName, "1", "2");

        // Replace table with a different column mode
        onDelta().executeQuery("" +
                "CREATE OR REPLACE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");

        assertTableReaderAndWriterVersion("default", tableName, "2", "5");

        // Revert back to `none` column mode
        onDelta().executeQuery("" +
                "CREATE OR REPLACE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='none')");

        assertTableReaderAndWriterVersion("default", tableName, "2", "5");

        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testDeltaColumnMappingModeReaderAndWriterVersion(String mode)
    {
        testColumnMappingModeReaderAndWriterVersion(tableName -> onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')"));
    }

    private void testColumnMappingModeReaderAndWriterVersion(Consumer<String> createTable)
    {
        String tableName = "test_dl_column_mapping_version_" + randomNameSuffix();

        createTable.accept(tableName);

        assertTableReaderAndWriterVersion("default", tableName, "2", "5");

        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_104, DELTA_LAKE_DATABRICKS_113, DELTA_LAKE_DATABRICKS_122, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoColumnMappingMode(String mode)
    {
        testColumnMappingMode(tableName -> onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                " (a_number INT, array_col ARRAY(ROW(array_struct_element VARCHAR)), nested ROW(field1 VARCHAR), a_string VARCHAR, part VARCHAR)" +
                " WITH (" +
                " partitioned_by = ARRAY['part']," +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")"));
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeltaColumnMappingMode(String mode)
    {
        testColumnMappingMode(tableName -> onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>, a_string STRING, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')"));
    }

    private void testColumnMappingMode(Consumer<String> createTable)
    {
        String tableName = "test_dl_column_mapping_mode_name_" + randomNameSuffix();

        createTable.accept(tableName);

        try {
            onDelta().executeQuery("" +
                    "INSERT INTO default." + tableName + " VALUES " +
                    "(1, array(struct('nested 1')), struct('databricks 1'),'ala', 'part1')");
            onTrino().executeQuery("" +
                    "INSERT INTO delta.default." + tableName + " VALUES " +
                    "(2, ARRAY[ROW('nested 2')], ROW('databricks 2'), 'kota', 'part2')");

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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameWithNonLowerCaseColumn()
    {
        String tableName = "test_dl_column_mapping_mode_name_non_loewr_case_" + randomNameSuffix();

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

            // Verify column comments
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".mixed_case IS 'test column comment'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "mixed_case"), "test column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "mixed_case"), "test column comment");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameCreatePartitionTableAsSelect()
    {
        String tableName = "test_dl_create_partition_table_as_select_" + randomNameSuffix();

        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName + " " +
                "WITH (" +
                "location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                "column_mapping_mode = 'name'" +
                ")" +
                "AS SELECT 1 AS id, 'part#1' AS part");
        try {
            Row expected = row(1, "part#1");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableWithCommentsColumnMappingModeName()
    {
        String tableName = "test_dl_create_table_with_comments_" + randomNameSuffix();

        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "(col INT COMMENT 'test column comment')" +
                "COMMENT 'test table comment'" +
                "WITH ( " +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'name'" +
                ")");
        try {
            assertEquals(getTableCommentOnTrino("default", tableName), "test table comment");
            assertEquals(getTableCommentOnDelta("default", tableName), "test table comment");

            assertEquals(getColumnCommentOnTrino("default", tableName, "col"), "test column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "col"), "test column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameCommentOnTable()
    {
        String tableName = "test_dl_column_mapping_mode_comment_on_table_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'" +
                ")");
        try {
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment by trino'");
            assertEquals(getTableCommentOnTrino("default", tableName), "test comment by trino");
            assertEquals(getTableCommentOnDelta("default", tableName), "test comment by trino");

            onDelta().executeQuery("COMMENT ON TABLE default." + tableName + " IS 'test comment by delta'");
            assertEquals(getTableCommentOnTrino("default", tableName), "test comment by delta");
            assertEquals(getTableCommentOnDelta("default", tableName), "test comment by delta");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameCommentOnColumn()
    {
        String tableName = "test_dl_column_mapping_mode_comment_on_column_" + randomNameSuffix();

        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                " (col INT)" +
                " WITH ( " +
                " location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'name'" +
                ")");

        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment by trino'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "col"), "test column comment by trino");
            assertEquals(getColumnCommentOnDelta("default", tableName, "col"), "test column comment by trino");

            onDelta().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN col COMMENT 'test column comment by delta'");
            assertEquals(getColumnCommentOnTrino("default", tableName, "col"), "test column comment by delta");
            assertEquals(getColumnCommentOnDelta("default", tableName, "col"), "test column comment by delta");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeNameAddColumn()
    {
        testColumnMappingModeAddColumn(
                "name",
                tableName -> {
                    onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN another_varchar VARCHAR");
                    onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN a_array array<integer>");
                    onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_map map(varchar, integer)");
                    onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN a_row struct<x integer>");
                });
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testColumnMappingModeIdAddColumn()
    {
        testColumnMappingModeAddColumn(
                "id",
                tableName -> {
                    onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN another_varchar STRING");
                    onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_array array(integer)");
                    onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN a_map map<string, integer>");
                    onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_row row(x integer)");
                });
    }

    private void testColumnMappingModeAddColumn(String mode, Consumer<String> addColumn)
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
            addColumn.accept(tableName);
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .containsOnly(
                            row("a_number", "integer", "", ""),
                            row("another_varchar", "varchar", "", ""),
                            row("a_array", "array(integer)", "", ""),
                            row("a_map", "map(varchar, integer)", "", ""),
                            row("a_row", "row(x integer)", "", ""));

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'new column', array(3), map('key', 4), named_struct('x', 5))");
            expectedRows = ImmutableList.of(
                    row(1, null, null, null, null),
                    row(2, null, null, null, null),
                    row(3, "new column", 3, 4, 5));
            assertThat(onDelta().executeQuery("SELECT a_number, another_varchar, a_array[0], a_map['key'], a_row.x FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT a_number, another_varchar, a_array[1], a_map['key'], a_row.x FROM delta.default." + tableName)).containsOnly(expectedRows);

            // 5 comes from 1 (a_number) + 1 (a_array) + 1 (a_map) + 2 (column & field of a_row)
            assertThat(getTablePropertyOnDelta("default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("6");

            // Replace table to partition by added column for testing predicate pushdown on the column
            onDelta().executeQuery("REPLACE TABLE default." + tableName + " USING DELTA PARTITIONED BY (another_varchar) AS SELECT * FROM " + tableName);
            assertThat(onTrino().executeQuery("SELECT a_number, another_varchar, a_array[1], a_map['key'], a_row.x FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoColumnMappingModeNameAddColumnWithExistingNonLowerCaseColumn()
    {
        String tableName = "test_dl_column_mapping_mode_add_column_existing_non_lowercase_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_CASE INT)" +
                " USING delta" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'" +
                ")");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES 1");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col VARCHAR");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, null));
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("UPPER_CASE", "new_col");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsFromJsonForColumnMappingMode()
    {
        String tableName = "test_dl_show_stats_json_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id'" +
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsFromParquetForColumnMappingModeName()
    {
        String tableName = "test_dl_show_parquet_stats_parquet_for_column_mapping_mode_" + randomNameSuffix();

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
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testShowStatsOnPartitionedForColumnMappingModeId()
    {
        String tableName = "test_dl_show_stats_partitioned_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id'," +
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

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testProjectionPushdownDmlWithColumnMappingMode(String mode)
    {
        String sourceTableName = "test_projection_pushdown_source_column_mapping_mode_" + randomNameSuffix();
        String targetTableName = "test_projection_pushdown_target_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + targetTableName + " (nation STRUCT<key INT, name STRING>, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + targetTableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        onDelta().executeQuery("CREATE TABLE default." + sourceTableName + "  (nation STRUCT<key INT, name STRING>, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + sourceTableName + "'");
        try {
            onDelta().executeQuery("INSERT INTO default." + targetTableName + " VALUES (struct(1, 'nation1'), 100), (struct(2, 'nation2'), 200), (struct(3, 'nation3'), 300), (struct(4, 'nation4'), 400)");
            onDelta().executeQuery("INSERT INTO default." + sourceTableName + " VALUES (struct(1000, 'nation1000'), 1000), (struct(2, 'nation2'), 20000), (struct(3000, 'nation3000'), 3000), (struct(4, 'nation4'), 40000)");

            onDelta().executeQuery("MERGE INTO default." + targetTableName + " target USING default." + sourceTableName + " source " +
                    "ON (target.nation.key = source.nation.key) " +
                    "WHEN MATCHED AND source.nation.name = 'nation4' THEN DELETE " +
                    "WHEN MATCHED THEN UPDATE SET nation.key = (target.nation.key + source.nation.key + source.regionkey) " +
                    "WHEN NOT MATCHED THEN INSERT (nation, regionkey) VALUES (source.nation, source.regionkey)");

            assertThat(onTrino().executeQuery("SELECT nation.key, nation.name, regionkey FROM " + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300),
                            row(20004, "nation2", 200));

            onDelta().executeQuery("DELETE FROM " + targetTableName + " WHERE regionkey = 100");
            onDelta().executeQuery("UPDATE " + targetTableName + " SET nation.name = 'nation20004' WHERE regionkey = 200");

            assertThat(onTrino().executeQuery("SELECT nation.key, nation.name, regionkey FROM " + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(3, "nation3", 300),
                            row(20004, "nation20004", 200));
        }
        finally {
            dropDeltaTableWithRetry("default." + targetTableName);
            dropDeltaTableWithRetry("default." + sourceTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "changeColumnMappingDataProvider")
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testRecalculateStatsForColumnMappingModeIdAndNoInitialStatistics()
    {
        String tableName = "test_recalculate_stats_for_column_mapping_mode_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id', " +
                " 'delta.dataSkippingNumIndexedCols' = 0" +
                ")");

        try {
            List<Row> expectedRows = ImmutableList.of(
                    row(1, "a"),
                    row(2, "bc"),
                    row(null, null));

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'a'), (2, 'bc'), (null, null)");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, null, null, null, null, null),
                            row("a_string", null, null, null, null, null, null),
                            row(null, null, null, null, 3.0, null, null));

            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, 0.33333333333, null, "1", "2"),
                            row("a_string", 3.0, 2.0, 0.33333333333, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedColumnMappingForDmlDataProvider")
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "supportedColumnMappingForDmlDataProvider")
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

    private void assertDeltaTrinoTableEquals(String tableName, String trinoQuery, String deltaQuery, List<Row> expectedRows)
    {
        assertThat(onDelta().executeQuery("SELECT " + deltaQuery + " FROM default." + tableName))
                .containsOnly(expectedRows);
        assertThat(onTrino().executeQuery("SELECT " + trinoQuery + " FROM delta.default." + tableName))
                .containsOnly(expectedRows);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testDropLastNonPartitionColumnWithColumnMappingMode(String mode)
    {
        String tableName = "test_drop_column_" + randomNameSuffix();
        String tableLocation = "s3://" + bucketName + "/databricks-compatibility-test-" + tableName;

        onTrino().executeQuery("CREATE TABLE delta.default." + tableName +
                " WITH (column_mapping_mode = '" + mode + "', partitioned_by = ARRAY['part'], location = '" + tableLocation + "')" +
                "AS SELECT 1 data, 'part#1' part");
        try {
            assertThatThrownBy(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN data"))
                    .hasMessageContaining("Dropping the last non-partition column is unsupported");

            // TODO https://github.com/delta-io/delta/issues/1929 Delta Lake disallows creating tables with all partitioned column, but allows dropping the non-partition column
            onDelta().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN data");

            Row expected = row("part#1");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testTrinoExtendedStatisticsDropAndAddColumnWithColumnMappingMode(String mode)
    {
        String tableName = "test_drop_and_add_column_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (a INT, b INT)" +
                " USING delta " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2)");
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row("b", null, 1.0, 0.0, null, "2", "2"),
                            row(null, null, null, null, 1.0, null, null));

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN b");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row(null, null, null, null, 1.0, null, null));

            // TODO: Add a new column on Trino once the connector supports adding a column with the column mapping mode
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN b INTEGER");

            // Verify column statistics of dropped column isn't restored
            onTrino().executeQuery("ANALYZE delta.default." + tableName);
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row("b", 0.0, 0.0, 1.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testDropNonLowercaseColumnWithColumnMappingMode(String mode)
    {
        String tableName = "test_drop_non_lowercase_column_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_ID INT, UPPER_DATA INT, UPPER_PART STRING)" +
                " USING delta " +
                " PARTITIONED BY (UPPER_PART) " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("UPPER_ID", "UPPER_DATA", "UPPER_PART");

            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 10, 'part#1')");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_data", null, 1.0, 0.0, null, "10", "10"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN upper_data");
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("UPPER_ID", "UPPER_PART");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, "part#1"));
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));

            assertThatThrownBy(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN upper_part"))
                    .hasMessageContaining("Cannot drop partition column");

            // Verify adding a column with the same name doesn't restore the old statistics
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMN UPPER_DATA INT");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_data", null, null, null, null, null, null),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoRenameColumnWithColumnMappingModeName()
    {
        testRenameColumnWithColumnMappingMode(
                "name",
                (tableName, column) -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN " + column.sourceColumn + " TO " + column.newColumn));
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSparkRenameColumnWithColumnMappingModeId()
    {
        testRenameColumnWithColumnMappingMode(
                "id",
                (tableName, column) -> onDelta().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN " + column.sourceColumn + " TO " + column.newColumn));
    }

    private void testRenameColumnWithColumnMappingMode(String mode, BiConsumer<String, RenameColumn> renameColumns)
    {
        String tableName = "test_rename_column_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (id INT, data INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            assertThat(getTablePropertyOnDelta("default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("3");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 10, 'part#1')");

            renameColumns.accept(tableName, new RenameColumn("data", "new_data"));
            renameColumns.accept(tableName, new RenameColumn("part", "new_part"));
            assertThat(getTablePropertyOnDelta("default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("3");

            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .containsOnly(
                            row("id", "integer", "", ""),
                            row("new_data", "integer", "", ""),
                            row("new_part", "varchar", "", ""));

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 10, "part#1"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 10, "part#1"));

            // Ensure renaming to the dropped column doesn't restore the old data
            onDelta().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN id");
            renameColumns.accept(tableName, new RenameColumn("new_data", "id"));

            assertThat(onTrino().executeQuery("SELECT id, new_part FROM delta.default." + tableName))
                    .containsOnly(row(10, "part#1"));
            assertThat(onDelta().executeQuery("SELECT id, new_part FROM default." + tableName))
                    .containsOnly(row(10, "part#1"));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    private record RenameColumn(String sourceColumn, String newColumn)
    {
        private RenameColumn
        {
            requireNonNull(sourceColumn, "sourceColumn is null");
            requireNonNull(newColumn, "newColumn is null");
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "columnMappingDataProvider")
    public void testRenameNonLowercaseColumn(String mode)
    {
        String tableName = "test_rename_non_lowercase_column_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_COL INT NOT NULL COMMENT 'test comment', UPPER_PART INT)" +
                " USING delta" +
                " PARTITIONED BY (UPPER_PART)" +
                " LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("upper_col", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));

            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("UPPER_COL", "UPPER_PART");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN upper_col TO new_col");
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("new_col", "UPPER_PART");
            assertEquals(getColumnCommentOnDelta("default", tableName, "new_col"), "test comment");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " (new_col) VALUES NULL"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: new_col");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN upper_part TO new_part");
            assertThat(getColumnNamesOnDelta("default", tableName))
                    .containsExactly("new_col", "new_part");

            assertThat(onTrino().executeQuery("SELECT new_col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("new_col", null, 1.0, 0.0, null, "1", "1"),
                            row("new_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    private void assertTableReaderAndWriterVersion(String schemaName, String tableName, String minReaderVersion, String minWriterVersion)
    {
        assertThat(getTablePropertyOnDelta(schemaName, tableName, "delta.minReaderVersion"))
                .isEqualTo(minReaderVersion);
        assertThat(getTablePropertyOnDelta(schemaName, tableName, "delta.minWriterVersion"))
                .isEqualTo(minWriterVersion);
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
        return new Object[][] {
                {"none"},
                {"name"},
                {"id"}
        };
    }
}
