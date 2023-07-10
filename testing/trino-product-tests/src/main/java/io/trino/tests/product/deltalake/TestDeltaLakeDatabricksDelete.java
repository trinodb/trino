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
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDatabricksDelete
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteOnAppendOnlyTableFails()
    {
        String tableName = "test_delete_on_append_only_table_fails_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.appendOnly' = true)");

        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 12)");
        assertQueryFailure(() -> onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1"))
                .hasMessageContaining("This table is configured to only allow appends");
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM default." + tableName + " WHERE a = 1"))
                .hasMessageContaining("Cannot modify rows from a table with 'delta.appendOnly' set to true");

        assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                .containsOnly(row(1, 11), row(2, 12));
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    // Databricks 12.1 added support for deletion vectors
    // TODO: Add DELTA_LAKE_OSS group once they support creating a table with deletion vectors
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeletionVectors()
    {
        // TODO https://github.com/trinodb/trino/issues/16903 Add support for deletionVectors reader features
        String tableName = "test_deletion_vectors_" + randomNameSuffix();
        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a INT, b INT)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "' " +
                "         TBLPROPERTIES ('delta.enableDeletionVectors' = true)");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,11), (2, 22)");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a = 2");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 11));

            assertThat(onTrino().executeQuery("SHOW TABLES FROM delta.default"))
                    .contains(row(tableName));
            assertThat(onTrino().executeQuery("SELECT comment FROM information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "'"))
                    .hasNoRows();
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default.\"" + tableName + "$history\""))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("SHOW COLUMNS FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("DESCRIBE delta.default." + tableName))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 33)"))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3"))
                    .hasMessageMatching(".* Table .* does not exist");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = -1"))
                    .hasMessageMatching(".* Table .* does not exist");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
