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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests Delta Lake column mapping mode compatibility between Trino and Spark.
 * <p>
 * Ported from the Tempto-based TestDeltaLakeColumnMappingMode (DELTA_LAKE_OSS tests).
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeColumnMappingMode
{
    private static final double ONE_THIRD = 1.0 / 3;

    @Test
    void testColumnMappingModeNone(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_none" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, struct('nested 1'))");

            List<Row> expectedRows = ImmutableList.of(row(1, "nested 1"));
            assertThat(env.executeSpark("SELECT a_number, nested.field1 FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT a_number, nested.field1 FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("supportedColumnMappingForDmlDataProvider")
    void testColumnMappingModeTableFeature(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_table_feature" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (col INT)" +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.feature.columnMapping'='supported', 'delta.columnMapping.mode'='" + mode + "')");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.columnMapping", "supported"))
                    .contains(entry("delta.columnMapping.mode", mode));

            // delta.feature.columnMapping still exists even after unsetting the property
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.feature.columnMapping')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.columnMapping", "supported"));

            // Unsetting delta.columnMapping.mode means changing to 'none' column mapping mode
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " UNSET TBLPROPERTIES ('delta.columnMapping.mode')");
            assertThat(getTablePropertiesOnSpark(env, "default", tableName))
                    .contains(entry("delta.feature.columnMapping", "supported"))
                    .doesNotContainKey("delta.columnMapping.mode");
            assertThat((String) env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue())
                    .doesNotContain("column_mapping_mode =");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testTrinoColumnMappingModeReaderAndWriterVersion(String mode, DeltaLakeMinioEnvironment env)
    {
        testColumnMappingModeReaderAndWriterVersion(env, tableName -> env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")"),
                5);
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testChangingColumnMappingModeViaCreateOrReplaceTableOnTrino(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_cortas_column_mapping_" + randomNameSuffix();
        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                ")");

        assertTableReaderAndWriterVersion(env, "default", tableName, "1", "2");

        // Replace table with a different column mode
        env.executeTrinoUpdate("" +
                "CREATE OR REPLACE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")");

        assertTableReaderAndWriterVersion(env, "default", tableName, "2", "5");

        // Revert back to `none` column mode
        env.executeTrinoUpdate("" +
                "CREATE OR REPLACE TABLE delta.default." + tableName +
                "(x INT) " +
                "WITH (" +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'none'" +
                ")");

        assertTableReaderAndWriterVersion(env, "default", tableName, "2", "5");

        env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testChangingColumnMappingModeViaCreateOrReplaceTableOnDelta(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_cortas_column_mapping_" + randomNameSuffix();
        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        assertTableReaderAndWriterVersion(env, "default", tableName, "1", "2");

        // Replace table with a different column mode
        env.executeSparkUpdate("" +
                "CREATE OR REPLACE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')");

        assertTableReaderAndWriterVersion(env, "default", tableName, "2", "7");

        // Revert back to `none` column mode
        env.executeSparkUpdate("" +
                "CREATE OR REPLACE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='none')");

        assertTableReaderAndWriterVersion(env, "default", tableName, "2", "7");

        env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testDeltaColumnMappingModeReaderAndWriterVersion(String mode, DeltaLakeMinioEnvironment env)
    {
        testColumnMappingModeReaderAndWriterVersion(env, tableName -> env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "(x INT) " +
                "USING delta " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + mode + "')"),
                7);
    }

    private void testColumnMappingModeReaderAndWriterVersion(DeltaLakeMinioEnvironment env, Consumer<String> createTable, int expectedMinWriterVersion)
    {
        String tableName = "test_dl_column_mapping_version_" + randomNameSuffix();

        createTable.accept(tableName);

        assertTableReaderAndWriterVersion(env, "default", tableName, "2", Integer.toString(expectedMinWriterVersion));

        env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testTrinoColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        testColumnMappingMode(env, tableName -> env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                " (a_number INT, array_col ARRAY(ROW(array_struct_element VARCHAR)), nested ROW(field1 VARCHAR), a_string VARCHAR, part VARCHAR)" +
                " WITH (" +
                " partitioned_by = ARRAY['part']," +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = '" + mode + "'" +
                ")"));
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testDeltaColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        testColumnMappingMode(env, tableName -> env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>, a_string STRING, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part)" +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + mode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')"));
    }

    private void testColumnMappingMode(DeltaLakeMinioEnvironment env, Consumer<String> createTable)
    {
        String tableName = "test_dl_column_mapping_mode_name_" + randomNameSuffix();

        createTable.accept(tableName);

        try {
            env.executeSparkUpdate("" +
                    "INSERT INTO default." + tableName + " VALUES " +
                    "(1, array(struct('nested 1')), struct('databricks 1'),'ala', 'part1')");
            env.executeTrinoUpdate("" +
                    "INSERT INTO delta.default." + tableName + " VALUES " +
                    "(2, ARRAY[ROW('nested 2')], ROW('databricks 2'), 'kota', 'part2')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "nested 1", "databricks 1", "ala", "part1"),
                    row(2, "nested 2", "databricks 2", "kota", "part2"));

            assertThat(env.executeSpark("SELECT a_number, array_col[0].array_struct_element, nested.field1, a_string, part FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT a_number, array_col[1].array_struct_element, nested.field1, a_string, part FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT a_string FROM delta.default." + tableName + " WHERE a_number = 1"))
                    .containsOnly(ImmutableList.of(row("ala")));
            assertThat(env.executeTrino("SELECT a_number FROM delta.default." + tableName + " WHERE nested.field1 = 'databricks 1'"))
                    .containsOnly(ImmutableList.of(row(1)));
            assertThat(env.executeTrino("SELECT a_number FROM delta.default." + tableName + " WHERE part = 'part1'"))
                    .containsOnly(row(1));
            assertThat(env.executeSpark("SELECT a_number FROM default." + tableName + " WHERE part = 'part1'"))
                    .containsOnly(row(1));

            // Verify the connector can read renamed columns correctly
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN a_number TO new_a_column");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN nested.field1 TO field2");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN part TO new_part");

            assertThat(env.executeTrino("DESCRIBE delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("new_a_column", "integer", "", ""),
                            row("array_col", "array(row(\"array_struct_element\" varchar))", "", ""),
                            row("nested", "row(\"field2\" varchar)", "", ""),
                            row("a_string", "varchar", "", ""),
                            row("new_part", "varchar", "", "")));

            assertThat(env.executeSpark("SELECT new_a_column, array_col[0].array_struct_element, nested.field2, a_string, new_part FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT new_a_column, array_col[1].array_struct_element, nested.field2, a_string, new_part FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testColumnMappingModeNameWithNonLowerCaseColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_name_non_loewr_case_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (`mIxEd_CaSe` INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='name'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (0), (9)");

            List<Row> expectedRows = ImmutableList.of(row(0), row(9));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("mixed_case", null, null, 0.0, null, "0", "9"),
                            row(null, null, null, null, 2.0, null, null)));

            // Verify column comments
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".mixed_case IS 'test column comment'");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "mixed_case")).isEqualTo("test column comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "mixed_case")).isEqualTo("test column comment");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testColumnMappingModeNameCreatePartitionTableAsSelect(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_create_partition_table_as_select_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName + " " +
                "WITH (" +
                "location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                "column_mapping_mode = 'name'" +
                ")" +
                "AS SELECT 1 AS id, 'part#1' AS part");
        try {
            Row expected = row(1, "part#1");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testCreateTableWithCommentsColumnMappingModeName(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_create_table_with_comments_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(col INT COMMENT 'test column comment')" +
                "COMMENT 'test table comment'" +
                "WITH ( " +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'name'" +
                ")");
        try {
            assertThat(getTableCommentOnTrino(env, "default", tableName)).isEqualTo("test table comment");
            assertThat(getTableCommentOnSpark(env, "default", tableName)).isEqualTo("test table comment");

            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test column comment");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "col")).isEqualTo("test column comment");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testColumnMappingModeNameCommentOnTable(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_comment_on_table_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'" +
                ")");
        try {
            env.executeTrinoUpdate("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment by trino'");
            assertThat(getTableCommentOnTrino(env, "default", tableName)).isEqualTo("test comment by trino");
            assertThat(getTableCommentOnSpark(env, "default", tableName)).isEqualTo("test comment by trino");

            env.executeSparkUpdate("COMMENT ON TABLE default." + tableName + " IS 'test comment by delta'");
            assertThat(getTableCommentOnTrino(env, "default", tableName)).isEqualTo("test comment by delta");
            assertThat(getTableCommentOnSpark(env, "default", tableName)).isEqualTo("test comment by delta");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testColumnMappingModeNameCommentOnColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_comment_on_column_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                " (col INT)" +
                " WITH ( " +
                " location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'," +
                " column_mapping_mode = 'name'" +
                ")");

        try {
            env.executeTrinoUpdate("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment by trino'");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test column comment by trino");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "col")).isEqualTo("test column comment by trino");

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ALTER COLUMN col COMMENT 'test column comment by delta'");
            assertThat(getColumnCommentOnTrino(env, "default", tableName, "col")).isEqualTo("test column comment by delta");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "col")).isEqualTo("test column comment by delta");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testColumnMappingModeNameAddColumn(DeltaLakeMinioEnvironment env)
    {
        testColumnMappingModeAddColumn(
                env,
                "name",
                tableName -> {
                    env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN another_varchar VARCHAR");
                    env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN a_array array<integer>");
                    env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_map map(varchar, integer)");
                    env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN a_row struct<x integer>");
                });
    }

    @Test
    void testColumnMappingModeIdAddColumn(DeltaLakeMinioEnvironment env)
    {
        testColumnMappingModeAddColumn(
                env,
                "id",
                tableName -> {
                    env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN another_varchar STRING");
                    env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_array array(integer)");
                    env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN a_map map<string, integer>");
                    env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN a_row row(x integer)");
                });
    }

    private void testColumnMappingModeAddColumn(DeltaLakeMinioEnvironment env, String mode, Consumer<String> addColumn)
    {
        String tableName = "test_dl_column_mapping_mode_add_column_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = '" + mode + "'" +
                ")");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1), (2)");

            List<Row> expectedRows = ImmutableList.of(row(1), row(2));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            // Verify the connector can read added columns correctly
            addColumn.accept(tableName);
            assertThat(env.executeTrino("DESCRIBE delta.default." + tableName))
                    .containsOnly(
                            row("a_number", "integer", "", ""),
                            row("another_varchar", "varchar", "", ""),
                            row("a_array", "array(integer)", "", ""),
                            row("a_map", "map(varchar, integer)", "", ""),
                            row("a_row", "row(\"x\" integer)", "", ""));

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3, 'new column', array(3), map('key', 4), named_struct('x', 5))");
            expectedRows = ImmutableList.of(
                    row(1, null, null, null, null),
                    row(2, null, null, null, null),
                    row(3, "new column", 3, 4, 5));
            assertThat(env.executeSpark("SELECT a_number, another_varchar, a_array[0], a_map['key'], a_row.x FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT a_number, another_varchar, a_array[1], a_map['key'], a_row.x FROM delta.default." + tableName)).containsOnly(expectedRows);

            // 5 comes from 1 (a_number) + 1 (a_array) + 1 (a_map) + 2 (column & field of a_row)
            assertThat(getTablePropertyOnSpark(env, "default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("6");

            // Replace table to partition by added column for testing predicate pushdown on the column
            env.executeSparkUpdate("REPLACE TABLE default." + tableName + " USING DELTA PARTITIONED BY (another_varchar) AS SELECT * FROM " + tableName);
            assertThat(env.executeTrino("SELECT a_number, another_varchar, a_array[1], a_map['key'], a_row.x FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoColumnMappingModeNameAddColumnWithExistingNonLowerCaseColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_column_mapping_mode_add_column_existing_non_lowercase_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_CASE INT)" +
                " USING delta" +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'" +
                ")");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES 1");

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col VARCHAR");

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, null));
            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("UPPER_CASE", "new_col");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testShowStatsFromJsonForColumnMappingMode(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_show_stats_json_for_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id'" +
                ")");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1), (2), (null)");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, ONE_THIRD, null, "1", "2"),
                            row(null, null, null, null, 3.0, null, null)));

            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, ONE_THIRD, null, "1", "2"),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testShowStatsFromParquetForColumnMappingModeName(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_show_parquet_stats_parquet_for_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'name'," +
                " 'delta.checkpointInterval' = 3" +
                ")");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (0)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (null)");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, ONE_THIRD, null, "0", "1"),
                            row(null, null, null, null, 3.0, null, null)));

            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, ONE_THIRD, null, "0", "1"),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testShowStatsOnPartitionedForColumnMappingModeId(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_show_stats_partitioned_for_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id'," +
                " 'delta.checkpointInterval' = 3" +
                ")");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (0, 'a')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'b')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (null, null)");

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, ONE_THIRD, null, "0", "1"),
                            row("part", null, 2.0, ONE_THIRD, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testProjectionPushdownDmlWithColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        String sourceTableName = "test_projection_pushdown_source_column_mapping_mode_" + randomNameSuffix();
        String targetTableName = "test_projection_pushdown_target_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + targetTableName + " (nation STRUCT<key INT, name STRING>, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        env.executeSparkUpdate("CREATE TABLE default." + sourceTableName + "  (nation STRUCT<key INT, name STRING>, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName + "'");
        try {
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (struct(1, 'nation1'), 100), (struct(2, 'nation2'), 200), (struct(3, 'nation3'), 300), (struct(4, 'nation4'), 400)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (struct(1000, 'nation1000'), 1000), (struct(2, 'nation2'), 20000), (struct(3000, 'nation3000'), 3000), (struct(4, 'nation4'), 40000)");

            env.executeSparkUpdate("MERGE INTO default." + targetTableName + " target USING default." + sourceTableName + " source " +
                    "ON (target.nation.key = source.nation.key) " +
                    "WHEN MATCHED AND source.nation.name = 'nation4' THEN DELETE " +
                    "WHEN MATCHED THEN UPDATE SET nation.key = (target.nation.key + source.nation.key + source.regionkey) " +
                    "WHEN NOT MATCHED THEN INSERT (nation, regionkey) VALUES (source.nation, source.regionkey)");

            assertThat(env.executeTrino("SELECT nation.key, nation.name, regionkey FROM delta.default." + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300),
                            row(20004, "nation2", 200));

            env.executeSparkUpdate("DELETE FROM default." + targetTableName + " WHERE regionkey = 100");
            env.executeSparkUpdate("UPDATE default." + targetTableName + " SET nation.name = 'nation20004' WHERE regionkey = 200");

            assertThat(env.executeTrino("SELECT nation.key, nation.name, regionkey FROM delta.default." + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(3, "nation3", 300),
                            row(20004, "nation20004", 200));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
        }
    }

    @Test
    void testChangeColumnMappingAndShowStatsForColumnMappingMode(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_change_column_mapping_and_show_stats_for_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, b_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='none'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 10), (2, 20), (null, null)");
            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, ONE_THIRD, null, "1", "2"),
                            row("b_number", null, 2.0, ONE_THIRD, null, "10", "20"),
                            row(null, null, null, null, 3.0, null, null));

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='name')");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " DROP COLUMN b_number");
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN b_number INT");

            // Ensure SHOW STATS doesn't return stats for the restored column
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, ONE_THIRD, null, "1", "2"),
                            row("b_number", null, null, null, null, null, null),
                            row(null, null, null, null, 3.0, null, null));

            // SHOW STATS returns the expected stats after executing ANALYZE
            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, 2.0, ONE_THIRD, null, "1", "2"),
                            row("b_number", 0.0, 0.0, 1.0, null, null, null),
                            row(null, null, null, null, 3.0, null, null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("changeColumnMappingDataProvider")
    void testChangeColumnMappingMode(String sourceMappingMode, String targetMappingMode, boolean supported, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_change_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode'='" + sourceMappingMode + "'," +
                " 'delta.minReaderVersion'='2'," +
                " 'delta.minWriterVersion'='5')");
        try {
            if (supported) {
                env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='" + targetMappingMode + "')");
            }
            else {
                assertThatThrownBy(() -> env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES('delta.columnMapping.mode'='" + targetMappingMode + "')"))
                        .hasStackTraceContaining("Changing column mapping mode from '%s' to '%s' is not supported".formatted(sourceMappingMode, targetMappingMode));
            }
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    static Stream<Arguments> changeColumnMappingDataProvider()
    {
        // Update testChangeColumnMappingAndShowStatsForColumnMappingMode if Delta Lake changes their behavior
        return Stream.of(
                // sourceMappingMode targetMappingMode supported
                Arguments.of("none", "id", false),
                Arguments.of("none", "name", true),
                Arguments.of("id", "none", true),
                Arguments.of("id", "name", false),
                Arguments.of("name", "none", true),
                Arguments.of("name", "id", false));
    }

    @Test
    void testRecalculateStatsForColumnMappingModeIdAndNoInitialStatistics(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_recalculate_stats_for_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a_number INT, a_string STRING)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' = 'id', " +
                " 'delta.dataSkippingNumIndexedCols' = 0" +
                ")");

        try {
            List<Row> expectedRows = ImmutableList.of(
                    row(1, "a"),
                    row(2, "bc"),
                    row(null, null));

            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'a'), (2, 'bc'), (null, null)");
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a_number", null, null, null, null, null, null),
                            row("a_string", null, null, null, null, null, null),
                            row(null, null, null, null, 3.0, null, null));

            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 2.0, ONE_THIRD, null, "1", "2"),
                            row("a_string", 3.0000000000000004, 2.0, ONE_THIRD, null, null, null),
                            row(null, null, null, null, 3.0, null, null)));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("supportedColumnMappingForDmlDataProvider")
    void testMergeUpdateWithColumnMapping(String mode, DeltaLakeMinioEnvironment env)
    {
        String sourceTableName = "test_merge_update_source_column_mapping_mode_" + randomNameSuffix();
        String targetTableName = "test_merge_update_target_column_mapping_mode_" + randomNameSuffix();

        env.executeSparkUpdate("CREATE TABLE default." + targetTableName + " (nationkey INT, name STRING, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        env.executeSparkUpdate("CREATE TABLE default." + sourceTableName + " (nationkey INT, name STRING, regionkey INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName + "'");
        try {
            env.executeSparkUpdate("INSERT INTO default." + targetTableName + " VALUES (1, 'nation1', 100), (2, 'nation2', 200), (3, 'nation3', 300)");
            env.executeSparkUpdate("INSERT INTO default." + sourceTableName + " VALUES (1000, 'nation1000', 1000), (2, 'nation2', 20000), (3000, 'nation3000', 3000)");

            env.executeTrinoUpdate("MERGE INTO delta.default." + targetTableName + " target USING delta.default." + sourceTableName + " source " +
                    "ON (target.nationkey = source.nationkey) " +
                    "WHEN MATCHED " +
                    "THEN UPDATE SET nationkey = (target.nationkey + source.nationkey + source.regionkey) " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (nationkey, name, regionkey) VALUES (source.nationkey, source.name, source.regionkey)");

            assertThat(env.executeSpark("SELECT * FROM default." + targetTableName))
                    .containsOnly(
                            row(1000, "nation1000", 1000),
                            row(3000, "nation3000", 3000),
                            row(1, "nation1", 100),
                            row(3, "nation3", 300),
                            row(20004, "nation2", 200));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
        }
    }

    @ParameterizedTest
    @MethodSource("supportedColumnMappingForDmlDataProvider")
    void testMergeDeleteWithColumnMapping(String mode, DeltaLakeMinioEnvironment env)
    {
        String sourceTableName = "test_dl_merge_delete_source_column_mapping_mode_" + mode + randomNameSuffix();
        String targetTableName = "test_dl_merge_delete_target_column_mapping_mode_" + mode + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + sourceTableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " PARTITIONED BY (a_string)" +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + sourceTableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' ='" + mode + "')");

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + targetTableName +
                " (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)" +
                " USING delta " +
                " PARTITIONED BY (a_string)" +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + targetTableName + "'" +
                " TBLPROPERTIES (" +
                " 'delta.columnMapping.mode' ='" + mode + "')");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + sourceTableName +
                    " VALUES (1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                    "        (3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                    "        (4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))");

            String trinoColumns = "a_number, a_string, array_col[1].array_struct_element, nested.field1";
            String deltaColumns = "a_number, a_string, array_col[0].array_struct_element, nested.field1";
            assertDeltaTrinoTableEquals(env, sourceTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3, "third value", "nested 3", "databricks 3"),
                    row(4, "four", "nested 4", "databricks 4")));

            env.executeTrinoUpdate("INSERT INTO delta.default." + targetTableName +
                    " VALUES (1000, '1000 value', ARRAY[ROW('nested 1000')], ROW('databricks 1000'))," +
                    "        (2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))");
            env.executeSparkUpdate("INSERT INTO default." + targetTableName +
                    " VALUES (3000, '3000 value', array(struct('nested 3000')), struct('databricks 3000'))," +
                    "        (4, 'four', array(struct('nested 4')), struct('databricks 4'))");

            assertDeltaTrinoTableEquals(env, targetTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1000, "1000 value", "nested 1000", "databricks 1000"),
                    row(2, "two", "nested 2", "databricks 2"),
                    row(3000, "3000 value", "nested 3000", "databricks 3000"),
                    row(4, "four", "nested 4", "databricks 4")));

            env.executeTrinoUpdate("MERGE INTO delta.default." + targetTableName + " t USING delta.default." + sourceTableName + " s " +
                    "ON (t.a_number = s.a_number) " +
                    "WHEN MATCHED " +
                    "THEN DELETE " +
                    "WHEN NOT MATCHED " +
                    "THEN INSERT (a_number, a_string, array_col, nested) VALUES (s.a_number, s.a_string, s.array_col, s.nested)");

            assertDeltaTrinoTableEquals(env, targetTableName, trinoColumns, deltaColumns, ImmutableList.of(
                    row(1000, "1000 value", "nested 1000", "databricks 1000"),
                    row(3000, "3000 value", "nested 3000", "databricks 3000"),
                    row(1, "first value", "nested 1", "databricks 1"),
                    row(3, "third value", "nested 3", "databricks 3")));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + sourceTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + targetTableName);
        }
    }

    private void assertDeltaTrinoTableEquals(DeltaLakeMinioEnvironment env, String tableName, String trinoQuery, String deltaQuery, List<Row> expectedRows)
    {
        assertThat(env.executeSpark("SELECT " + deltaQuery + " FROM default." + tableName))
                .containsOnly(expectedRows);
        assertThat(env.executeTrino("SELECT " + trinoQuery + " FROM delta.default." + tableName))
                .containsOnly(expectedRows);
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testDropLastNonPartitionColumnWithColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_drop_column_" + randomNameSuffix();
        String tableLocation = "s3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName;

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName +
                " WITH (column_mapping_mode = '" + mode + "', partitioned_by = ARRAY['part'], location = '" + tableLocation + "')" +
                "AS SELECT 1 data, 'part#1' part");
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " DROP COLUMN data"))
                    .hasMessageContaining("Dropping the last non-partition column is unsupported");

            // TODO https://github.com/delta-io/delta/issues/1929 Delta Lake disallows creating tables with all partitioned column, but allows dropping the non-partition column
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " DROP COLUMN data");

            Row expected = row("part#1");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testTrinoExtendedStatisticsDropAndAddColumnWithColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_drop_and_add_column_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (a INT, b INT)" +
                " USING delta " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 2)");
            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row("b", null, 1.0, 0.0, null, "2", "2"),
                            row(null, null, null, null, 1.0, null, null));

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " DROP COLUMN b");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row(null, null, null, null, 1.0, null, null));

            // TODO: Add a new column on Trino once the connector supports adding a column with the column mapping mode
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN b INTEGER");

            // Verify column statistics of dropped column isn't restored
            env.executeTrinoUpdate("ANALYZE delta.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("a", null, 1.0, 0.0, null, "1", "1"),
                            row("b", 0.0, 0.0, 1.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testDropNonLowercaseColumnWithColumnMappingMode(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_drop_non_lowercase_column_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_ID INT, UPPER_DATA INT, UPPER_PART STRING)" +
                " USING delta " +
                " PARTITIONED BY (UPPER_PART) " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("UPPER_ID", "UPPER_DATA", "UPPER_PART");

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 10, 'part#1')");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_data", null, 1.0, 0.0, null, "10", "10"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " DROP COLUMN upper_data");
            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("UPPER_ID", "UPPER_PART");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, "part#1"));
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));

            assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " DROP COLUMN upper_part"))
                    .hasMessageContaining("Cannot drop partition column");

            // Verify adding a column with the same name doesn't restore the old statistics
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " ADD COLUMN UPPER_DATA INT");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("upper_id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_data", null, null, null, null, null, null),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoRenameColumnWithColumnMappingModeName(DeltaLakeMinioEnvironment env)
    {
        testRenameColumnWithColumnMappingMode(
                env,
                "name",
                (tableName, column) -> env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " RENAME COLUMN " + column.sourceColumn + " TO " + column.newColumn));
    }

    @Test
    void testSparkRenameColumnWithColumnMappingModeId(DeltaLakeMinioEnvironment env)
    {
        testRenameColumnWithColumnMappingMode(
                env,
                "id",
                (tableName, column) -> env.executeSparkUpdate("ALTER TABLE default." + tableName + " RENAME COLUMN " + column.sourceColumn + " TO " + column.newColumn));
    }

    private void testRenameColumnWithColumnMappingMode(DeltaLakeMinioEnvironment env, String mode, BiConsumer<String, RenameColumn> renameColumns)
    {
        String tableName = "test_rename_column_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (id INT, data INT, part STRING)" +
                " USING delta " +
                " PARTITIONED BY (part) " +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");

        try {
            assertThat(getTablePropertyOnSpark(env, "default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("3");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 10, 'part#1')");

            renameColumns.accept(tableName, new RenameColumn("data", "new_data"));
            renameColumns.accept(tableName, new RenameColumn("part", "new_part"));
            assertThat(getTablePropertyOnSpark(env, "default", tableName, "delta.columnMapping.maxColumnId"))
                    .isEqualTo("3");

            assertThat(env.executeTrino("DESCRIBE delta.default." + tableName))
                    .containsOnly(
                            row("id", "integer", "", ""),
                            row("new_data", "integer", "", ""),
                            row("new_part", "varchar", "", ""));

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 10, "part#1"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 10, "part#1"));

            // Ensure renaming to the dropped column doesn't restore the old data
            env.executeSparkUpdate("ALTER TABLE default." + tableName + " DROP COLUMN id");
            renameColumns.accept(tableName, new RenameColumn("new_data", "id"));

            assertThat(env.executeTrino("SELECT id, new_part FROM delta.default." + tableName))
                    .containsOnly(row(10, "part#1"));
            assertThat(env.executeSpark("SELECT id, new_part FROM default." + tableName))
                    .containsOnly(row(10, "part#1"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
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

    @ParameterizedTest
    @MethodSource("columnMappingDataProvider")
    void testRenameNonLowercaseColumn(String mode, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_rename_non_lowercase_column_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                " (UPPER_COL INT NOT NULL COMMENT 'test comment', UPPER_PART INT)" +
                " USING delta" +
                " PARTITIONED BY (UPPER_PART)" +
                " LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'" +
                " TBLPROPERTIES ('delta.columnMapping.mode' = '" + mode + "')");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("upper_col", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));

            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("UPPER_COL", "UPPER_PART");

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " RENAME COLUMN upper_col TO new_col");
            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("new_col", "UPPER_PART");
            assertThat(getColumnCommentOnSpark(env, "default", tableName, "new_col")).isEqualTo("test comment");
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " (new_col) VALUES NULL"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: new_col");

            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " RENAME COLUMN upper_part TO new_part");
            assertThat(getColumnNamesOnSpark(env, "default", tableName))
                    .containsExactly("new_col", "new_part");

            assertThat(env.executeTrino("SELECT new_col, new_part FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("new_col", null, 1.0, 0.0, null, "1", "1"),
                            row("new_part", null, 1.0, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private void assertTableReaderAndWriterVersion(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String minReaderVersion, String minWriterVersion)
    {
        assertThat(getTablePropertyOnSpark(env, schemaName, tableName, "delta.minReaderVersion"))
                .isEqualTo(minReaderVersion);
        assertThat(getTablePropertyOnSpark(env, schemaName, tableName, "delta.minWriterVersion"))
                .isEqualTo(minWriterVersion);
    }

    static Stream<Arguments> columnMappingDataProvider()
    {
        return Stream.of(
                Arguments.of("id"),
                Arguments.of("name"));
    }

    static Stream<Arguments> supportedColumnMappingForDmlDataProvider()
    {
        return Stream.of(
                Arguments.of("none"),
                Arguments.of("name"),
                Arguments.of("id"));
    }

    // Helper methods for accessing table metadata

    @SuppressWarnings("unchecked")
    private List<String> getColumnNamesOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return (List<String>) (List<?>) env.executeSpark("SHOW COLUMNS IN " + schemaName + "." + tableName).column(1);
    }

    private String getColumnCommentOnTrino(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeTrino("SELECT comment FROM delta.information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    private String getColumnCommentOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeSpark(format("DESCRIBE %s.%s %s", schemaName, tableName, columnName)).rows().get(2).get(1);
    }

    private String getTableCommentOnTrino(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return (String) env.executeTrino("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'")
                .getOnlyValue();
    }

    private String getTableCommentOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return (String) env.executeSpark(format("DESCRIBE EXTENDED %s.%s", schemaName, tableName)).getRows().stream()
                .filter(row -> row.getValues().get(0).equals("Comment"))
                .map(row -> row.getValues().get(1))
                .collect(onlyElement());
    }

    private Map<String, String> getTablePropertiesOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName)
    {
        return env.executeSpark("SHOW TBLPROPERTIES %s.%s".formatted(schemaName, tableName)).rows().stream()
                .collect(toImmutableMap(
                        row -> (String) row.get(0),
                        row -> (String) row.get(1)));
    }

    private String getTablePropertyOnSpark(DeltaLakeMinioEnvironment env, String schemaName, String tableName, String propertyName)
    {
        return (String) env.executeSpark("SHOW TBLPROPERTIES %s.%s(%s)".formatted(schemaName, tableName, propertyName)).rows().get(0).get(1);
    }
}
