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
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeProceduresCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testUnregisterTable()
    {
        String tableName = "test_dl_unregister_table" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s WITH (location = 's3://%s/%s') AS SELECT 123 AS col",
                tableName,
                bucketName,
                tableDirectory));
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(123));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName)).containsOnly(row(123));

            onTrino().executeQuery("CALL delta.system.unregister_table('default', '" + tableName + "')");

            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .hasMessageMatching(".* Table '.*' does not exist");
            assertQueryFailure(() -> onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .hasMessageMatching("(?s).*(Table or view not found|The table or view .* cannot be found).*");
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }
}
