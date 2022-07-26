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

import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getTableCommentOnDelta;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakeAlterTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testAddColumnWithCommentOnTrino()
    {
        String tableName = "test_dl_add_column_with_comment_" + randomTableSuffix();
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
    public void testAddColumnUnsupportedWriterVersion()
    {
        String tableName = "test_dl_add_column_unsupported_writer_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='3')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col int"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 3 which is not supported");
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCommentOnTable()
    {
        String tableName = "test_dl_comment_table_" + randomTableSuffix();
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
    public void testCommentOnTableUnsupportedWriterVersion()
    {
        String tableName = "test_dl_comment_table_unsupported_writer_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='3')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test comment'"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 3 which is not supported");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCommentOnColumn()
    {
        String tableName = "test_dl_comment_column_" + randomTableSuffix();
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
    public void testCommentOnColumnUnsupportedWriterVersion()
    {
        String tableName = "test_dl_comment_column_unsupported_writer_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='3')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".col IS 'test column comment'"))
                    .hasMessageMatching(".* Table .* requires Delta Lake writer version 3 which is not supported");
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoAlterTablePreservesTableMetadata()
    {
        String tableName = "test_trino_alter_table_preserves_table_metadata_" + randomTableSuffix();
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
}
