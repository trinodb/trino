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
    public void testFailingHiveViewsWithMetadataListing()
    {
        setupBrokenView();
        testFailingHiveViewsWithInformationSchema();
        testFailingHiveViewsWithSystemJdbc();
        // cleanup
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_list_failing_views CASCADE");
    }

    private void testFailingHiveViewsWithInformationSchema()
    {
        // The expected behavior is different across hive versions. For hive 3, the call "getTableNamesByType" is
        // used in ThriftHiveMetastore#getAllViews. For older versions, the fallback to doGetTablesWithParameter
        // is used, so Trino's information_schema.views table does not include translated Hive views.
        String withSchemaFilter = "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views'";
        String withNoFilter = "SELECT table_name FROM information_schema.views";
        if (getHiveVersionMajor() == 3) {
            assertThat(query(withSchemaFilter)).containsOnly(row("correct_view"));
            assertThat(query(withNoFilter)).contains(row("correct_view"));
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

    private void testFailingHiveViewsWithSystemJdbc()
    {
        // The expected behavior is different across hive versions. For hive 3, the call "getTableNamesByType" is
        // used in ThriftHiveMetastore#getAllViews. For older versions, the fallback to doGetTablesWithParameter
        // is used, so Trino's system.jdbc.tables table does not include translated Hive views.
        String withSchemaFilter = "SELECT table_name FROM system.jdbc.tables WHERE " +
                "table_cat = 'hive' AND " +
                "table_schem = 'test_list_failing_views' AND " +
                "table_type = 'VIEW'";
        String withNoFilter = "SELECT table_name FROM system.jdbc.tables WHERE table_cat = 'hive' AND table_type = 'VIEW'";
        if (getHiveVersionMajor() == 3) {
            assertThat(query(withSchemaFilter)).containsOnly(row("correct_view"), row("failing_view"));
            assertThat(query(withNoFilter)).contains(row("correct_view"), row("failing_view"));
        }
        else {
            assertThat(query(withSchemaFilter)).hasNoRows();
            Assertions.assertThat(query(withNoFilter).rows()).doesNotContain(ImmutableList.of("correct_view"));
        }

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(query(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        // Listing fails when metadata for the problematic view is queried specifically
        assertThatThrownBy(() -> query(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");

        // Queries on system.jdbc.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore views.
        assertThat(query("SELECT table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThatThrownBy(() -> query("SELECT * FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");
    }

    private static void setupBrokenView()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_list_failing_views CASCADE");
        onHive().executeQuery("CREATE SCHEMA test_list_failing_views");
        onHive().executeQuery("CREATE VIEW test_list_failing_views.correct_view AS SELECT * FROM nation limit 5");

        // Create a view for which the translation is guaranteed to fail
        onTrino().executeQuery("CREATE TABLE test_list_failing_views.table_dropped (col0 BIGINT)");
        onHive().executeQuery("CREATE VIEW test_list_failing_views.failing_view AS SELECT * FROM test_list_failing_views.table_dropped");
        onTrino().executeQuery("DROP TABLE test_list_failing_views.table_dropped");
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

    @Test(groups = HIVE_VIEWS)
    public void testFromUtcTimestamp()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_from_utc_timestamp_source");
        onHive().executeQuery("CREATE TABLE test_from_utc_timestamp_source (" +
                "  source_tinyint tinyint, " +
                "  source_smallint smallint, " +
                "  source_integer int, " +
                "  source_bigint bigint, " +
                "  source_float float, " +
                "  source_double double, " +
                "  source_decimal_three decimal(10,3), " +
                "  source_decimal_zero decimal(10,0), " +
                "  source_timestamp timestamp, " +
                "  source_date date" +
                ")");

        // insert via Trino as we noticed problems with creating test table in Hive using CTAS at one go for some Hive distributions
        onTrino().executeQuery("INSERT INTO test_from_utc_timestamp_source VALUES ( " +
                "  123, " +
                "  10123, " +
                "  259200123, " +
                "  2592000123, " +
                "  2592000.0, " +
                "  2592000.123, " +
                "  2592000.123," +
                "  2592000," +
                "  timestamp '1970-01-30 16:00:00.000', " +
                "  date '1970-01-30'" +
                ")");

        onHive().executeQuery("DROP VIEW IF EXISTS test_from_utc_timestamp_view");
        onHive().executeQuery("CREATE VIEW " +
                "test_from_utc_timestamp_view " +
                "AS SELECT " +
                // TODO(https://github.com/trinodb/trino/issues/8853) add testcases with 3-letter tz names (like PST) when we have $canonicalize_hive_timezone_id logic in place
                "   CAST(from_utc_timestamp(source_tinyint, 'America/Los_Angeles') AS STRING) ts_tinyint, " +
                "   CAST(from_utc_timestamp(source_smallint, 'America/Los_Angeles') AS STRING) ts_smallint, " +
                "   CAST(from_utc_timestamp(source_integer, 'America/Los_Angeles') AS STRING) ts_integer, " +
                "   CAST(from_utc_timestamp(source_bigint, 'America/Los_Angeles') AS STRING) ts_bigint, " +
                "   CAST(from_utc_timestamp(source_float, 'America/Los_Angeles') AS STRING) ts_float, " +
                "   CAST(from_utc_timestamp(source_double, 'America/Los_Angeles') AS STRING) ts_double, " +
                "   CAST(from_utc_timestamp(source_decimal_three, 'America/Los_Angeles') AS STRING) ts_decimal_three, " +
                "   CAST(from_utc_timestamp(source_decimal_zero, 'America/Los_Angeles') AS STRING) ts_decimal_zero, " +
                "   CAST(from_utc_timestamp(source_timestamp, 'America/Los_Angeles') AS STRING) ts_timestamp, " +
                "   CAST(from_utc_timestamp(source_date, 'America/Los_Angeles') AS STRING) ts_date " +
                "FROM test_from_utc_timestamp_source");

        // check result on Trino
        assertThat(query("SELECT * FROM test_from_utc_timestamp_view"))
                .containsOnly(row(
                        "1969-12-31 16:00:00.123",
                        "1969-12-31 16:00:10.123",
                        "1970-01-03 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 08:00:00.000",
                        "1970-01-29 16:00:00.000"));

        // check result on Hive
        if (isObsoleteFromUtcTimestampSemantics()) {
            // For older hive version we expect different results on Hive side; as from_utc_timestamp semantics changed over time.
            // Currently view transformation logic always follows new semantics.
            // Leaving Hive assertions as documentation.
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_view"))
                    .containsOnly(row(
                            "1969-12-31 21:30:00.123",
                            "1969-12-31 21:30:10.123",
                            "1970-01-03 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00",
                            "1970-01-30 08:00:00",
                            "1970-01-29 16:00:00"));
        }
        else {
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_view"))
                    .containsOnly(row(
                            "1969-12-31 16:00:00.123",
                            "1969-12-31 16:00:10.123",
                            "1970-01-03 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00",
                            "1970-01-30 08:00:00",
                            "1970-01-29 16:00:00"));
        }
    }

    @Test(groups = HIVE_VIEWS)
    public void testFromUtcTimestampCornerCases()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_from_utc_timestamp_corner_cases_source");
        onTrino().executeQuery("CREATE TABLE test_from_utc_timestamp_corner_cases_source AS SELECT * FROM (VALUES " +
                "  CAST(-5000000000001 AS BIGINT)," +
                "  CAST(-1000000000001 AS BIGINT)," +
                "  -1," +
                "  1," +
                "  5000000000001" +
                ")" +
                "AS source(source_bigint)");

        onHive().executeQuery("DROP VIEW IF EXISTS test_from_utc_timestamp_corner_cases_view");
        onHive().executeQuery("CREATE VIEW " +
                "test_from_utc_timestamp_corner_cases_view " +
                "AS SELECT " +
                "   CAST(from_utc_timestamp(source_bigint, 'America/Los_Angeles') as STRING) ts_bigint " +
                "FROM test_from_utc_timestamp_corner_cases_source");

        // check result on Trino
        assertThat(query("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
                .containsOnly(
                        row("1811-07-23 07:13:41.999"),
                        row("1938-04-24 14:13:19.999"),
                        row("1969-12-31 15:59:59.999"),
                        row("1969-12-31 16:00:00.001"),
                        row("2128-06-11 01:53:20.001"));

        // check result on Hive
        if (isObsoleteFromUtcTimestampSemantics()) {
            // For older hive version we expect different results on Hive side; as from_utc_timestamp semantics changed over time.
            // Currently view transformation logic always follows new semantics.
            // Leaving Hive assertions as documentation.
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
                    .containsOnly(
                            row("1811-07-23 12:51:39.999"), // ???
                            row("1938-04-24 19:43:19.999"),
                            row("1969-12-31 21:29:59.999"),
                            row("1969-12-31 21:30:00.001"),
                            row("2128-06-11 07:38:20.001"));
        }
        else {
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
                    .containsOnly(
                            row("1811-07-23 07:13:41.999"),
                            row("1938-04-24 14:13:19.999"),
                            row("1969-12-31 15:59:59.999"),
                            row("1969-12-31 16:00:00.001"),
                            row("2128-06-11 01:53:20.001"));
        }
    }

    private boolean isObsoleteFromUtcTimestampSemantics()
    {
        // It appears from_utc_timestamp semantics in Hive changes some time on the way. The guess is that it happened
        // together with change of timestamp semantics at version 3.1.
        return getHiveVersionMajor() < 3 || (getHiveVersionMajor() == 3 && getHiveVersionMinor() < 1);
    }
}
