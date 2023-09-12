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

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeCaseInsensitiveMapping
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testNonLowercaseColumnNames()
    {
        String tableName = "test_dl_non_lowercase_column" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE_INT INT, Camel_Case_String STRING, PART INT)" +
                "USING delta " +
                "PARTITIONED BY (PART)" +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'a', 10)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (2, 'ab', 20)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (null, null, null)");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("upper_case_int", null, 1.0, 0.33333333333, null, "1", "2"),
                            row("camel_case_string", 2.0, 1.0, 0.33333333333, null, null, null),
                            row("part", null, 2.0, 0.33333333333, null, null, null),
                            row(null, null, null, null, 3.0, null, null));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testNonLowercaseFieldNames()
    {
        String tableName = "test_dl_non_lowercase_field" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(id int, UPPER_CASE STRUCT<UPPER_FIELD: string>, Mixed_Case struct<Mixed_Nested: struct<Mixed_Field: string>>)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " SELECT 1, row('test uppercase'), row(row('test mixedcase'))");
            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("id", null, 1.0, 0.0, null, "1", "1"),
                            row("upper_case", null, null, null, null, null, null),
                            row("mixed_case", null, null, null, null, null, null),
                            row(null, null, null, null, 1.0, null, null));

            // Specify field names to test projection pushdown
            List<QueryAssert.Row> expectedRows = ImmutableList.of(row(1, "test uppercase", "test mixedcase"));
            assertThat(onDelta().executeQuery("SELECT id, upper_case.upper_field, mixed_case.mixed_nested.mixed_field FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, upper_case.upper_field, mixed_case.mixed_nested.mixed_field FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(onTrino().executeQuery("SELECT id FROM delta.default." + tableName + " WHERE upper_case.upper_field = 'test uppercase'"))
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT id FROM delta.default." + tableName + " WHERE mixed_case.mixed_nested.mixed_field = 'test mixedcase'"))
                    .containsOnly(row(1));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnCommentWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_column_comment_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT COMMENT 'test column comment')" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            assertEquals(getColumnCommentOnTrino("default", tableName, "upper_case"), "test column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "UPPER_CASE"), "test column comment");

            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test updated comment'");

            assertEquals(getColumnCommentOnTrino("default", tableName, "upper_case"), "test updated comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "UPPER_CASE"), "test updated comment");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testNotNullColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_notnull_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT NOT NULL)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete NOT NULL constraint
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test comment'");

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES NULL"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testGeneratedColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_generated_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(A INT, B INT GENERATED ALWAYS AS (A * 2))" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete generated expressions
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".a IS 'test comment for a'");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test comment for b'");

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // Exclude 10.4 because it throws MISSING_COLUMN when executing INSERT statement
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_identity_column_case_sensitivity_" + randomNameSuffix();
        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(data INT, UPPERCASE_IDENTITY BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                "TBLPROPERTIES ('delta.columnMapping.mode'='name')");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (data) VALUES (1), (2), (3)");

            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1), row(2, 2), row(3, 3));

            // Verify identify column is recognized even when the name contains uppercase characters
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 4)"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET data = 3"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.data = s.data) WHEN MATCHED THEN UPDATE SET data = 1"))
                    .hasMessageContaining("Writing to tables with identity columns is not supported");

            // Verify column operations preserves the identify column name and property
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN new_col integer");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN new_col TO renamed_col");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".uppercase_identity IS 'test comment'");
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " DROP COLUMN renamed_col");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("UPPERCASE_IDENTITY BIGINT GENERATED ALWAYS AS IDENTITY");

            // Verify the connector preserves column identity property when renaming columns containing uppercase characters
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME COLUMN uppercase_identity TO renamed_identity");
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("renamed_identity BIGINT GENERATED ALWAYS AS IDENTITY");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
