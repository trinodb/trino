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
import io.trino.jdbc.Row;
import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
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
    // Currently, Trino columns type signature does not correspond one to one with the Hive view columns type in case of `varchar` and `string` columns
    @Override
    protected List<QueryAssert.Row> getExpectedHiveViewTextualColumnsTypes()
    {
        return ImmutableList.of(
                row("a_char_1", "char(1)"),
                row("a_char_255", "char(255)"),
                row("a_varchar_1", "varchar"),
                row("a_varchar_65535", "varchar"),
                row("a_string", "varchar"));
    }

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
            assertThat(onTrino().executeQuery(withSchemaFilter)).containsOnly(row("correct_view"));
            assertThat(onTrino().executeQuery(withNoFilter)).contains(row("correct_view"));
        }
        else {
            assertThat(onTrino().executeQuery(withSchemaFilter)).hasNoRows();
            Assertions.assertThat(onTrino().executeQuery(withNoFilter).rows()).doesNotContain(ImmutableList.of("correct_view"));
        }

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(onTrino().executeQuery("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        // Listing fails when metadata for the problematic view is queried specifically
        assertThatThrownBy(() -> onTrino().executeQuery("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");

        // Queries on information_schema.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore views.
        assertThat(onTrino().executeQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM information_schema.columns WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
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
            assertThat(onTrino().executeQuery(withSchemaFilter)).containsOnly(row("correct_view"), row("failing_view"));
            assertThat(onTrino().executeQuery(withNoFilter)).contains(row("correct_view"), row("failing_view"));
        }
        else {
            assertThat(onTrino().executeQuery(withSchemaFilter)).hasNoRows();
            Assertions.assertThat(onTrino().executeQuery(withNoFilter).rows()).doesNotContain(ImmutableList.of("correct_view"));
        }

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(onTrino().executeQuery(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        // Listing fails when metadata for the problematic view is queried specifically
        assertThatThrownBy(() -> onTrino().executeQuery(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");

        // Queries on system.jdbc.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore views.
        assertThat(onTrino().executeQuery("SELECT table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views' AND table_name = 'failing_view'"))
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
                "   CAST(from_utc_timestamp(source_tinyint, 'America/Los_Angeles') AS STRING) ts_tinyint, " +
                "   CAST(from_utc_timestamp(source_tinyint, 'PST') AS STRING) ts_tinyint_short_tz, " +
                "   CAST(from_utc_timestamp(source_smallint, 'America/Los_Angeles') AS STRING) ts_smallint, " +
                "   CAST(from_utc_timestamp(source_smallint, 'PST') AS STRING) ts_smallint_short_tz, " +
                "   CAST(from_utc_timestamp(source_integer, 'America/Los_Angeles') AS STRING) ts_integer, " +
                "   CAST(from_utc_timestamp(source_integer, 'PST') AS STRING) ts_integer_short_tz, " +
                "   CAST(from_utc_timestamp(source_bigint, 'America/Los_Angeles') AS STRING) ts_bigint, " +
                "   CAST(from_utc_timestamp(source_bigint, 'PST') AS STRING) ts_bigint_short_tz, " +
                "   CAST(from_utc_timestamp(source_float, 'America/Los_Angeles') AS STRING) ts_float, " +
                "   CAST(from_utc_timestamp(source_float, 'PST') AS STRING) ts_float_short_tz, " +
                "   CAST(from_utc_timestamp(source_double, 'America/Los_Angeles') AS STRING) ts_double, " +
                "   CAST(from_utc_timestamp(source_double, 'PST') AS STRING) ts_double_short_tz, " +
                "   CAST(from_utc_timestamp(source_decimal_three, 'America/Los_Angeles') AS STRING) ts_decimal_three, " +
                "   CAST(from_utc_timestamp(source_decimal_three, 'PST') AS STRING) ts_decimal_three_short_tz, " +
                "   CAST(from_utc_timestamp(source_decimal_zero, 'America/Los_Angeles') AS STRING) ts_decimal_zero, " +
                "   CAST(from_utc_timestamp(source_decimal_zero, 'PST') AS STRING) ts_decimal_zero_short_tz, " +
                "   CAST(from_utc_timestamp(source_timestamp, 'America/Los_Angeles') AS STRING) ts_timestamp, " +
                "   CAST(from_utc_timestamp(source_timestamp, 'PST') AS STRING) ts_timestamp_short_tz, " +
                "   CAST(from_utc_timestamp(source_date, 'America/Los_Angeles') AS STRING) ts_date, " +
                "   CAST(from_utc_timestamp(source_date, 'PST') AS STRING) ts_date_short_tz " +
                "FROM test_from_utc_timestamp_source");

        // check result on Trino
        assertThat(onTrino().executeQuery("SELECT * FROM test_from_utc_timestamp_view"))
                .containsOnly(row(
                        "1969-12-31 16:00:00.123",
                        "1969-12-31 16:00:00.123",
                        "1969-12-31 16:00:10.123",
                        "1969-12-31 16:00:10.123",
                        "1970-01-03 16:00:00.123",
                        "1970-01-03 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.123",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 16:00:00.000",
                        "1970-01-30 08:00:00.000",
                        "1970-01-30 08:00:00.000",
                        "1970-01-29 16:00:00.000",
                        "1970-01-29 16:00:00.000"));

        // check result on Hive
        if (isObsoleteFromUtcTimestampSemantics()) {
            // For older hive version we expect different results on Hive side; as from_utc_timestamp semantics changed over time.
            // Currently view transformation logic always follows new semantics.
            // Leaving Hive assertions as documentation.
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_view"))
                    .containsOnly(row(
                            "1969-12-31 21:30:00.123",
                            "1969-12-31 21:30:00.123",
                            "1969-12-31 21:30:10.123",
                            "1969-12-31 21:30:10.123",
                            "1970-01-03 21:30:00.123",
                            "1970-01-03 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00",
                            "1970-01-30 21:30:00",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00.123",
                            "1970-01-30 21:30:00",
                            "1970-01-30 21:30:00",
                            "1970-01-30 08:00:00",
                            "1970-01-30 08:00:00",
                            "1970-01-29 16:00:00",
                            "1970-01-29 16:00:00"));
        }
        else {
            assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_view"))
                    .containsOnly(row(
                            "1969-12-31 16:00:00.123",
                            "1969-12-31 16:00:00.123",
                            "1969-12-31 16:00:10.123",
                            "1969-12-31 16:00:10.123",
                            "1970-01-03 16:00:00.123",
                            "1970-01-03 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00",
                            "1970-01-30 16:00:00",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00.123",
                            "1970-01-30 16:00:00",
                            "1970-01-30 16:00:00",
                            "1970-01-30 08:00:00",
                            "1970-01-30 08:00:00",
                            "1970-01-29 16:00:00",
                            "1970-01-29 16:00:00"));
        }
    }

    @Test(groups = HIVE_VIEWS)
    public void testFromUtcTimestampInvalidTimeZone()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_from_utc_timestamp_invalid_time_zone_source");
        onHive().executeQuery("CREATE TABLE test_from_utc_timestamp_invalid_time_zone_source (source_timestamp timestamp)");

        // insert via Trino as we noticed problems with creating test table in Hive using CTAS at one go for some Hive distributions
        onTrino().executeQuery("INSERT INTO test_from_utc_timestamp_invalid_time_zone_source VALUES (timestamp '1970-01-30 16:00:00.000')");

        onHive().executeQuery("DROP VIEW IF EXISTS test_from_utc_timestamp_invalid_time_zone_view");
        onHive().executeQuery("CREATE VIEW " +
                "test_from_utc_timestamp_invalid_time_zone_view " +
                "AS SELECT " +
                "   CAST(from_utc_timestamp(source_timestamp, 'Matrix/Zion') AS STRING) ts_timestamp " +
                "FROM test_from_utc_timestamp_invalid_time_zone_source");

        // check result on Trino
        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM test_from_utc_timestamp_invalid_time_zone_view"))
                .hasMessageContaining("'Matrix/Zion' is not a valid time zone");
        // check result on Hive - Hive falls back to GMT in case of dealing with an invalid time zone
        assertThat(onHive().executeQuery("SELECT * FROM test_from_utc_timestamp_invalid_time_zone_view"))
                .containsOnly(row("1970-01-30 16:00:00"));
        onTrino().executeQuery("DROP TABLE test_from_utc_timestamp_invalid_time_zone_source");
    }

    @Test(groups = HIVE_VIEWS)
    public void testNestedFieldWithReservedKeyNames()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_nested_field_with_reserved_key_names_source");
        onHive().executeQuery("CREATE TABLE test_nested_field_with_reserved_key_names_source (nested_field_key_word_lower_case struct<`from`:BIGINT>, " +
                "nested_field_key_word_upper_case struct<`FROM`:BIGINT>, nested_field_quote struct<`do...from`:BIGINT>)");

        onTrino().executeQuery("INSERT INTO test_nested_field_with_reserved_key_names_source VALUES (row(row(1), row(2), row(3)))");

        onHive().executeQuery("DROP VIEW IF EXISTS test_nested_field_with_reserved_key_names_view");
        onHive().executeQuery("CREATE VIEW " +
                "test_nested_field_with_reserved_key_names_view " +
                "AS SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote " +
                "FROM test_nested_field_with_reserved_key_names_source");

        assertThat(onHive().executeQuery("SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row("{\"from\":1}", "{\"from\":2}", "{\"do...from\":3}"));

        assertThat(onHive().executeQuery("SELECT nested_field_key_word_lower_case.`from`, nested_field_key_word_upper_case.`from`, nested_field_quote.`do...from` FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(1L, 2L, 3L));

        assertThat(onTrino().executeQuery("SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(Row.builder().addField("from", 1L).build(),
                        Row.builder().addField("from", 2L).build(),
                        Row.builder().addField("do...from", 3L).build()));

        assertThat(onTrino().executeQuery("SELECT nested_field_key_word_lower_case.\"from\", nested_field_key_word_upper_case.\"from\", nested_field_quote.\"do...from\" FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(1L, 2L, 3L));

        onHive().executeQuery("DROP VIEW test_nested_field_with_reserved_key_names_view");
        onHive().executeQuery("DROP TABLE test_nested_field_with_reserved_key_names_source");
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
        assertThat(onTrino().executeQuery("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
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

    @Test(groups = HIVE_VIEWS)
    public void testCastTimestampAsDecimal()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS cast_timestamp_as_decimal");
        onHive().executeQuery("CREATE TABLE cast_timestamp_as_decimal (a_timestamp TIMESTAMP)");
        onHive().executeQuery("INSERT INTO cast_timestamp_as_decimal VALUES ('1990-01-02 12:13:14.123456789')");
        onHive().executeQuery("DROP VIEW IF EXISTS cast_timestamp_as_decimal_view");
        onHive().executeQuery("CREATE VIEW cast_timestamp_as_decimal_view AS SELECT CAST(a_timestamp as DECIMAL(10,0)) a_cast_timestamp FROM cast_timestamp_as_decimal");

        String testQuery = "SELECT * FROM cast_timestamp_as_decimal_view";
        if (getHiveVersionMajor() > 3 || (getHiveVersionMajor() == 3 && getHiveVersionMinor() >= 1)) {
            assertViewQuery(
                    testQuery,
                    queryAssert -> queryAssert.containsOnly(row(new BigDecimal("631282394"))));
        }
        else {
            // For Hive versions older than 3.1 semantics of cast timestamp to decimal is different and it takes into account timezone Hive VM uses.
            // We cannot replicate the behaviour in Trino, hence test only documents different expected results.
            assertThat(onTrino().executeQuery(testQuery)).containsOnly(row(new BigDecimal("631282394")));
            assertThat(onHive().executeQuery(testQuery)).containsOnly(row(new BigDecimal("631261694")));
        }

        onHive().executeQuery("DROP VIEW cast_timestamp_as_decimal_view");
        onHive().executeQuery("DROP TABLE cast_timestamp_as_decimal");
    }
}
