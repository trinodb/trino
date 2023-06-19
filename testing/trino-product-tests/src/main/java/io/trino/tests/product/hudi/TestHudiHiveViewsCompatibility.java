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
package io.trino.tests.product.hudi;

import io.trino.tempto.ProductTest;
import io.trino.tests.product.deltalake.TestHiveAndDeltaLakeCompatibility;
import io.trino.tests.product.iceberg.TestIcebergHiveViewsCompatibility;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

/**
 * Tests interactions between Hudi and Hive connectors, when one tries to read a view created by the other.
 *
 * @see TestIcebergHiveViewsCompatibility
 * @see TestHiveAndDeltaLakeCompatibility
 */
public class TestHudiHiveViewsCompatibility
        extends ProductTest
{
    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHudiSelectFromHiveView()
    {
        String tableName = "hudi_from_hive_table_" + randomNameSuffix();
        String viewName = "hudi_from_trino_hive_view_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " AS SELECT 1 a");
        onTrino().executeQuery("CREATE VIEW hive.default." + viewName + " AS TABLE hive.default." + tableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default." + viewName))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Not a Hudi table: default." + viewName);

        onTrino().executeQuery("DROP VIEW hive.default." + viewName);
        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHiveSelectFromHudiView()
    {
        assertQueryFailure(() -> onTrino().executeQuery("CREATE VIEW hudi.default.a_new_view AS SELECT 1 a"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q This connector does not support creating views");
        // TODO test reading via Hive once Hudi supports CREATE VIEW
    }
}
