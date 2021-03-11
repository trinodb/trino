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
package io.trino.tests.hive;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.Requires;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.TestGroups.HIVE_VIEWS;
import static io.trino.tests.utils.QueryExecutors.onHive;
import static io.trino.tests.utils.QueryExecutors.onPresto;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Requires({
        ImmutableNationTable.class,
        ImmutableOrdersTable.class,
})
public class TestHiveViews
        extends AbstractTestHiveViews
{
    @Test(groups = HIVE_VIEWS)
    public void testFailingHiveViewsForInformationSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_list_failing_views CASCADE");
        onHive().executeQuery("CREATE SCHEMA test_list_failing_views");
        onHive().executeQuery("CREATE VIEW test_list_failing_views.correct_view AS SELECT * FROM nation limit 5");

        // Create a view for which the translation is guaranteed to fail
        onPresto().executeQuery("CREATE TABLE test_list_failing_views.table_dropped (col0 BIGINT)");
        onHive().executeQuery("CREATE VIEW test_list_failing_views.failing_view AS SELECT * FROM test_list_failing_views.table_dropped");
        onPresto().executeQuery("DROP TABLE test_list_failing_views.table_dropped");

        // The expected behavior is different across hive versions. For hive 3, the call "getTableNamesByType" is
        // used in ThriftHiveMetastore#getAllViews. For older versions, the fallback to doGetTablesWithParameter
        // is used, so Trino's information_schema.views table does not include translated Hive views.
        String withSchemaFilter = "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views'";
        String withNoFilter = "SELECT table_name FROM information_schema.views";
        if (getHiveVersionMajor() == 3) {
            assertThat(query(withSchemaFilter)).containsOnly(row("correct_view"));
            assertThat(query(withSchemaFilter)).contains(row("correct_view"));
        }
        else {
            assertThat(query(withSchemaFilter)).hasNoRows();
            Assertions.assertThat(query(withNoFilter).rows()).doesNotContain(ImmutableList.of("correct_view"));
        }

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        // Listing fails when metadata for the problematic view is queried specifically
        assertThatThrownBy(() -> query("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");

        // Queries on information_schema.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore views.
        assertThat(query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThatThrownBy(() -> query("SELECT * FROM information_schema.columns WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");
    }
}
