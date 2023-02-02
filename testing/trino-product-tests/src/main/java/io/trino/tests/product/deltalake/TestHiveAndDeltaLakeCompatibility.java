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

import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHiveAndDeltaLakeCompatibility
        extends ProductTest
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testInformationSchemaColumnsOnPresenceOfHiveView()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_information_schema_columns_schema_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String hiveViewName = "delta_schema_columns_hive_view_" + randomNameSuffix();
        String hiveViewQualifiedName = format("hive.%s.%s", schemaName, hiveViewName);

        onTrino().executeQuery("CREATE VIEW " + hiveViewQualifiedName + " AS SELECT 1 AS col_one");

        try {
            assertThat(onTrino().executeQuery(format("SELECT table_name FROM delta.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(row(hiveViewName));
        }
        finally {
            onTrino().executeQuery("DROP VIEW IF EXISTS " + hiveViewQualifiedName);
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }
}
