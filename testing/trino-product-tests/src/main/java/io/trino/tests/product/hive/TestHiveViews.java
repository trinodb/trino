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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.Requires;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.HIVE_VIEWS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
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
        onTrino().executeQuery("CREATE TABLE test_list_failing_views.table_dropped (col0 BIGINT)");
        onHive().executeQuery("CREATE VIEW test_list_failing_views.failing_view AS SELECT * FROM test_list_failing_views.table_dropped");
        onTrino().executeQuery("DROP TABLE test_list_failing_views.table_dropped");

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

    @Test(groups = HIVE_VIEWS)
    public void testLateralViewExplode()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS pageAds");
        onTrino().executeQuery("CREATE TABLE pageAds(pageid, adid_list) WITH (format='TEXTFILE') AS " +
                "VALUES " +
                "  (VARCHAR 'two', ARRAY[11, 22]), " +
                "  ('nothing', NULL), " +
                "  ('zero', ARRAY[]), " +
                "  ('one', ARRAY[42])");

        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view");
        onHive().executeQuery("CREATE VIEW hive_lateral_view as SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid");

        assertViewQuery(
                "SELECT * FROM hive_lateral_view",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11),
                        row("two", 22),
                        row("one", 42)));

        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view_outer_explode");
        onHive().executeQuery("CREATE VIEW hive_lateral_view_outer_explode as " +
                "SELECT pageid, adid FROM pageAds LATERAL VIEW OUTER explode(adid_list) adTable AS adid");

        assertViewQuery(
                "SELECT * FROM hive_lateral_view_outer_explode",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11),
                        row("two", 22),
                        row("one", 42),
                        row("nothing", null),
                        row("zero", null)));
    }

    @Test(groups = HIVE_VIEWS)
    public void testLateralViewExplodeArrayOfStructs()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS pageAdsStructs");
        onTrino().executeQuery("CREATE TABLE pageAdsStructs(pageid VARCHAR, adid_list ARRAY(ROW(a INTEGER, b INTEGER))) WITH (format='TEXTFILE')");
        onTrino().executeQuery("INSERT INTO pageAdsStructs " +
                "VALUES " +
                "  ('two', ARRAY[ROW(11, 12), ROW(13, 14)]), " +
                "  ('nothing', NULL), " +
                "  ('zero', ARRAY[]), " +
                "  ('one', ARRAY[ROW(42, 43)])");

        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view_structs");
        onHive().executeQuery("CREATE VIEW hive_lateral_view_structs as SELECT pageid, adid FROM pageAdsStructs LATERAL VIEW explode(adid_list) adTable AS adid");

        assertViewQuery(
                "SELECT pageid, adid.a, adid.b FROM hive_lateral_view_structs",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11, 12),
                        row("two", 13, 14),
                        row("one", 42, 43)));

        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view_structs_outer_explode");
        onHive().executeQuery("CREATE VIEW hive_lateral_view_structs_outer_explode as " +
                "SELECT pageid, adid FROM pageAdsStructs LATERAL VIEW OUTER explode(adid_list) adTable AS adid");

        assertViewQuery(
                "SELECT pageid, adid.a, adid.b FROM hive_lateral_view_structs_outer_explode",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11, 12),
                        row("two", 13, 14),
                        row("one", 42, 43),
                        row("nothing", null, null),
                        row("zero", null, null)));
    }

    @Test(groups = HIVE_VIEWS)
    public void testLateralViewJsonTupleAs()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_json_tuple_table");
        onTrino().executeQuery("" +
                "CREATE TABLE test_json_tuple_table WITH (format='TEXTFILE') AS " +
                "SELECT 3 id, CAST('{\"user_id\": 1000, \"first.name\": \"Mateusz\", \"Last Name\": \"Gajewski\", \".NET\": true, \"aNull\": null}' AS varchar) jsonstr");

        onHive().executeQuery("DROP VIEW IF EXISTS test_json_tuple_view");
        onHive().executeQuery("CREATE VIEW test_json_tuple_view AS " +
                "SELECT `t`.`id`, `x`.`a`, `x`.`b`, `x`.`c`, `x`.`d`, `x`.`e`, `x`.`f` FROM test_json_tuple_table AS `t` " +
                "LATERAL VIEW json_tuple(`t`.`jsonstr`, \"first.name\", \"Last Name\", '.NET', \"user_id\", \"aNull\", \"nonexistentField\") `x` AS `a`, `b`, `c`, `d`, `e`, `f`");

        assertViewQuery(
                "SELECT * FROM test_json_tuple_view",
                queryAssert -> queryAssert.containsOnly(row(3, "Mateusz", "Gajewski", "true", "1000", null, null)));
    }
}
/**/
