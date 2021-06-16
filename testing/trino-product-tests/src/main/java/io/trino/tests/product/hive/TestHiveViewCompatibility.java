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
package io.trino.tests.product.hive;

import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.HIVE_VIEW_COMPATIBILITY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onCompatibilityTestServer;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

@Requires(ImmutableNationTable.class)
public class TestHiveViewCompatibility
        extends HiveProductTest
{
    @Test(groups = {HIVE_VIEW_COMPATIBILITY, PROFILE_SPECIFIC_TESTS})
    public void testSelectOnView()
    {
        onCompatibilityTestServer().executeQuery("DROP VIEW IF EXISTS hive_test_view");
        onCompatibilityTestServer().executeQuery("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        assertViewQuery(onCompatibilityTestServer(), "SELECT * FROM hive_test_view", queryAssert -> queryAssert.hasRowsCount(25));
        assertViewQuery(
                onCompatibilityTestServer(),
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive_test_view WHERE n_nationkey < 3",
                queryAssert -> queryAssert.containsOnly(
                        row(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"),
                        row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                        row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ")));
    }

    @Test(groups = {HIVE_VIEW_COMPATIBILITY, PROFILE_SPECIFIC_TESTS})
    public void testSelectOnViewFromDifferentSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE");
        onHive().executeQuery("CREATE SCHEMA test_schema");
        onCompatibilityTestServer().executeQuery(
                "CREATE VIEW test_schema.hive_test_view_1 AS SELECT * FROM " +
                        // no schema is specified by purpose
                        "nation");

        assertViewQuery(onCompatibilityTestServer(), "SELECT * FROM test_schema.hive_test_view_1", queryAssert -> queryAssert.hasRowsCount(25));
    }

    @Test(groups = {HIVE_VIEW_COMPATIBILITY, PROFILE_SPECIFIC_TESTS})
    public void testExistingView()
    {
        onCompatibilityTestServer().executeQuery("DROP VIEW IF EXISTS hive_duplicate_view");
        onCompatibilityTestServer().executeQuery("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertThat(() -> query("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation"))
                .failsWithMessage("View already exists");
    }

    protected static void assertViewQuery(QueryExecutor queryExecutor, String query, Consumer<QueryAssert> assertion)
    {
        // Ensure view compatibility by comparing the results
        assertion.accept(assertThat(onTrino().executeQuery(query)));
        assertion.accept(assertThat(queryExecutor.executeQuery(query)));
    }
}
