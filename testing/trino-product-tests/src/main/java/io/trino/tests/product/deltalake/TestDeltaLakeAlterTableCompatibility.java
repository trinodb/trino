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

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_143_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTablePropertyOnDelta;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeAlterTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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
            assertThat(getColumnCommentOnTrino("default", tableName, "new_col")).isEqualTo("new column comment");
            assertThat(getColumnCommentOnDelta("default", tableName, "new_col")).isEqualTo("new column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDropNotNullConstraint()
    {
        testDropNotNullConstraint("id");
        testDropNotNullConstraint("name");
        testDropNotNullConstraint("none");
    }

    private void testDropNotNullConstraint(String columnMappingMode)
    {
        String tableName = "test_dl_drop_not_null_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data1 int NOT NULL, data2 int NOT NULL, part1 int NOT NULL, part2 int NOT NULL) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "PARTITIONED BY (part1, part2)" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='" + columnMappingMode + "')");
        try {
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN data1 DROP NOT NULL");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN part1 DROP NOT NULL");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ALTER COLUMN data2 DROP NOT NULL");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ALTER COLUMN part2 DROP NOT NULL");

            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (NULL, NULL, NULL, NULL)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, NULL, NULL)");

            List<QueryAssert.Row> expected = ImmutableList.of(row(null, null, null, null), row(null, null, null, null));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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
            assertThat(getTableCommentOnTrino("default", tableName)).isEqualTo("test comment");
            assertThat(getTableCommentOnDelta("default", tableName)).isEqualTo("test comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
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
            assertThat(getColumnCommentOnTrino("default", tableName, "col")).isEqualTo("test column comment");
            assertThat(getColumnCommentOnDelta("default", tableName, "col")).isEqualTo("test column comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoPreservesReaderAndWriterVersions()
    {
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
            assertThat((String) minReaderVersion.get(1)).isEqualTo("1");
            List<?> minWriterVersion = getOnlyElement(onDelta().executeQuery("SHOW TBLPROPERTIES " + tableName + "(delta.minWriterVersion)").rows());
            assertThat((String) minWriterVersion.get(1)).isEqualTo("1");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_113, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoPreservesTableFeature()
    {
        String tableName = "test_trino_preserves_table_feature_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName + " (col int)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.checkpointInterval'=1, 'delta.feature.columnMapping'='supported')");
        try {
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col INT");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET col = 2");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName);
            onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " + "ON (t.col = s.col) WHEN MATCHED THEN UPDATE SET new_col = 3");

            assertThat(getTablePropertyOnDelta("default", tableName, "delta.feature.columnMapping"))
                    .isEqualTo("supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoAlterTablePreservesGeneratedColumn()
    {
        if (getDatabricksRuntimeVersion().orElseThrow().isAtLeast(DATABRICKS_143_RUNTIME_VERSION)) {
            // The following COMMENT statement throws an exception (expected) because version >= 14.3 stores 'generatedColumns' writer feature
            return;
        }

        String tableName = "test_trino_alter_table_preserves_generated_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                """
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

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("b INT GENERATED ALWAYS AS ( a * 2 )");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a, c) VALUES (1, 3)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2, 3));

            assertThat(onTrino().executeQuery("SELECT column_name, extra_info FROM delta.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .containsOnly(row("a", null), row("b", "generated: a * 2"), row("c", null));
            assertThat(onTrino().executeQuery("DESCRIBE delta.default." + tableName).project(1, 3))
                    .containsOnly(row("a", ""), row("b", "generated: a * 2"), row("c", ""));
            assertThat(onTrino().executeQuery("SHOW COLUMNS FROM delta.default." + tableName).project(1, 3))
                    .containsOnly(row("a", ""), row("b", "generated: a * 2"), row("c", ""));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTypeWideningInteger()
    {
        String tableName = "test_dl_type_widening_integer_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName + "" +
                "(a byte, b byte) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.enableTypeWidening'=true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (127, -128)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128));

            // byte -> short
            onDelta().executeQuery("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE short");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128));
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a) VALUES 32767");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, null));

            // byte -> integer
            onDelta().executeQuery("ALTER TABLE default." + tableName + " CHANGE COLUMN b TYPE integer");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, null));
            onDelta().executeQuery("UPDATE default." + tableName + " SET b = -32768 WHERE b IS NULL");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768));

            // short -> integer
            onDelta().executeQuery("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE integer");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768));
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a) VALUES 2147483647");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(127, -128), row(32767, -32768), row(2147483647, null));

            assertQueryFailure(() -> onDelta().executeQuery("ALTER TABLE default." + tableName + " CHANGE COLUMN a TYPE long"))
                    .hasMessageContaining("ALTER TABLE CHANGE COLUMN is not supported for changing column a from INT to BIGINT");
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }
}
