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

import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeIdentityColumnCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumn()
    {
        // Databricks 7.3 & 9.1 and OSS Delta Lake don't support identity columns https://github.com/delta-io/delta/issues/1100
        String tableName = "test_identity_column_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                """
                CREATE TABLE default.%s (a INT, b BIGINT GENERATED ALWAYS AS IDENTITY)
                USING DELTA LOCATION 's3://%s/%s'
                """,
                tableName,
                bucketName,
                tableDirectory));
        try {
            String failureMessage = ".* Table .* requires Delta Lake writer version 6 which is not supported";
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test column comment'"))
                    .hasMessageMatching(failureMessage);
            assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'test table comment'"))
                    .hasMessageMatching(failureMessage);
            assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT"))
                    .hasMessageMatching(failureMessage);
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + "(a) VALUES (0)"))
                    .hasMessageMatching(failureMessage);
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName))
                    .hasMessageMatching(failureMessage);
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " + "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = 1"))
                    .hasMessageMatching(failureMessage);

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("b BIGINT GENERATED ALWAYS AS IDENTITY");
            onDelta().executeQuery("INSERT INTO default." + tableName + " (a) VALUES (0)");

            QueryAssert.Row expected = row(0, 1);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
