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

import io.trino.testng.services.Flaky;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeCheckConstraintCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS}, dataProvider = "checkConstraints")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCheckConstraintCompatibility(String columnDefinition, String checkConstraint, String validInput, Object insertedValue, String invalidInput)
    {
        String tableName = "test_check_constraint_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(" + columnDefinition + ") " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (" + checkConstraint + ")");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (" + validInput + ")");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(insertedValue));
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (" + validInput + ")");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(insertedValue), row(insertedValue));

            assertThatThrownBy(() -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (" + invalidInput + ")"))
                    .hasMessageMatching("(?s).* CHECK constraint .* violated by row with values.*");
            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (" + invalidInput + ")"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(insertedValue), row(insertedValue));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public static Object[][] checkConstraints()
    {
        return new Object[][] {
                // columnDefinition, checkConstraint, validInput, insertedValue, invalidInput
                // Operand
                {"a INT", "a = 1", "1", 1, "2"},
                {"a INT", "a > 1", "2", 2, "1"},
                {"a INT", "a >= 1", "1", 1, "0"},
                {"a INT", "a < 1", "0", 0, "1"},
                {"a INT", "a <= 1", "1", 1, "2"},
                {"a INT", "a <> 1", "2", 2, "1"},
                {"a INT", "a != 1", "2", 2, "1"},
                // Supported types
                {"a INT", "a < 100", "1", 1, "100"},
                {"a STRING", "a = 'valid'", "'valid'", "valid", "'invalid'"},
                {"a STRING", "a = \"double-quote\"", "'double-quote'", "double-quote", "'invalid'"},
                // Identifier
                {"`a.dot` INT", "`a.dot` = 1", "1", 1, "2"},
        };
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCheckConstraintAcrossColumns()
    {
        String tableName = "test_check_constraint_across_columns_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT a_constraint CHECK (a = b)");
        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 1)");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1));

            assertThatThrownBy(() -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2)"))
                    .hasMessageMatching("(?s).* CHECK constraint .* violated by row with values.*");
            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Check constraint violation");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 1));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testWritesToTableWithCheckConstraintFails()
    {
        String tableName = "test_check_constraint_unsupported_operatins_" + randomNameSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT aIsPositive CHECK (a > 0)");
        try {
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3 WHERE b = 3"))
                    .hasMessageContaining("Updating a table with a check constraint is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 3"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = 42"))
                    .hasMessageContaining("Cannot merge into a table with check constraints");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testMetadataOperationsRetainCheckConstraint()
    {
        String tableName = "test_metadata_operations_retain_check_constraints_" + randomNameSuffix();
        onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT aIsPositive CHECK (a > 0)");
        try {
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".c IS 'example column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'example table comment'");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
