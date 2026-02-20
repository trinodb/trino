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

import io.trino.jdbc.Row;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.QueryResultAssert;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Strings.padEnd;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveViews.
 * <p>
 * Tests Hive view functionality including view creation, view queries, and
 * compatibility between Hive and Trino view interpretations.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveViews
{
    // ==================== Tests from TestHiveViews ====================

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFailingHiveViewsWithMetadataListing(HiveBasicEnvironment env)
    {
        setupBrokenView(env);
        testFailingHiveViewsWithInformationSchema(env);
        testFailingHiveViewsWithSystemJdbc(env);
        // cleanup
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_list_failing_views CASCADE");
    }

    private void testFailingHiveViewsWithInformationSchema(HiveBasicEnvironment env)
    {
        // The expected behavior is different across hive versions. For hive 3, the call "getTableNamesByType" is
        // used in ThriftHiveMetastore#getAllViews. For older versions, the fallback to doGetTablesWithParameter
        // is used, so Trino's information_schema.views table does not include translated Hive views.
        String withSchemaFilter = "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views'";
        String withNoFilter = "SELECT table_name FROM information_schema.views";
        assertThat(env.executeTrino(withSchemaFilter)).containsOnly(row("correct_view"));
        assertThat(env.executeTrino(withNoFilter)).contains(row("correct_view"));

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(env.executeTrino("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        assertThat(env.executeTrino("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasNoRows();

        // Queries on information_schema.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore Hive views.
        assertThat(env.executeTrino("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThat(env.executeTrino("SELECT * FROM information_schema.columns WHERE table_schema = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasNoRows();
    }

    private void testFailingHiveViewsWithSystemJdbc(HiveBasicEnvironment env)
    {
        // The expected behavior is different across hive versions. For hive 3, the call "getTableNamesByType" is
        // used in ThriftHiveMetastore#getAllViews. For older versions, the fallback to doGetTablesWithParameter
        // is used, so Trino's system.jdbc.tables table does not include translated Hive views.
        String withSchemaFilter = "SELECT table_name FROM system.jdbc.tables WHERE " +
                "table_cat = 'hive' AND " +
                "table_schem = 'test_list_failing_views' AND " +
                "table_type = 'VIEW'";
        String withNoFilter = "SELECT table_name FROM system.jdbc.tables WHERE table_cat = 'hive' AND table_type = 'VIEW'";
        assertThat(env.executeTrino(withSchemaFilter)).containsOnly(row("correct_view"), row("failing_view"));
        assertThat(env.executeTrino(withNoFilter)).contains(row("correct_view"), row("failing_view"));

        // Queries with filters on table_schema and table_name are optimized to only fetch the specified table and uses
        // a different API. so the Hive version does not matter here.
        assertThat(env.executeTrino(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'correct_view'"))
                .containsOnly(row("correct_view"));

        // Listing fails when metadata for the problematic view is queried specifically
        assertThatThrownBy(() -> env.executeTrino(
                "SELECT table_name FROM system.jdbc.tables WHERE " +
                        "table_cat = 'hive' AND " +
                        "table_schem = 'test_list_failing_views' AND " +
                        "table_name = 'failing_view'"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");

        // Queries on system.jdbc.columns also trigger ConnectorMetadata#getViews. Columns from failing_view are
        // listed too since HiveMetadata#listTableColumns does not ignore Hive views.
        assertThat(env.executeTrino("SELECT table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views'"))
                .containsOnly(
                        row("correct_view", "n_nationkey"),
                        row("correct_view", "n_name"),
                        row("correct_view", "n_regionkey"),
                        row("correct_view", "n_comment"),
                        row("failing_view", "col0"));

        assertThat(env.executeTrino("SELECT * FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'test_list_failing_views' AND table_name = 'failing_view'"))
                .hasNoRows();
    }

    private static void setupBrokenView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_list_failing_views CASCADE");
        env.executeHiveUpdate("CREATE SCHEMA test_list_failing_views");
        env.executeHiveUpdate("CREATE VIEW test_list_failing_views.correct_view AS SELECT * FROM nation limit 5");

        // Create a view for which the translation is guaranteed to fail
        env.executeTrinoUpdate("CREATE TABLE test_list_failing_views.table_dropped (col0 BIGINT)");
        env.executeHiveUpdate("CREATE VIEW test_list_failing_views.failing_view AS SELECT * FROM test_list_failing_views.table_dropped");
        env.executeTrinoUpdate("DROP TABLE test_list_failing_views.table_dropped");

        // Make sure that failing_view throws exception when queried explicitly.
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM test_list_failing_views.failing_view"))
                .hasMessageContaining("Failed to translate Hive view 'test_list_failing_views.failing_view'");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testLateralViewExplode(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS pageAds");
        env.executeTrinoUpdate("CREATE TABLE pageAds(pageid, adid_list) WITH (format='TEXTFILE') AS " +
                "VALUES " +
                "  (VARCHAR 'two', ARRAY[11, 22]), " +
                "  ('nothing', NULL), " +
                "  ('zero', ARRAY[]), " +
                "  ('one', ARRAY[42])");

        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_lateral_view");
        env.executeHiveUpdate("CREATE VIEW hive_lateral_view as SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid");

        assertViewQuery(env,
                "SELECT * FROM hive_lateral_view",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11),
                        row("two", 22),
                        row("one", 42)));

        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_lateral_view_outer_explode");
        env.executeHiveUpdate("CREATE VIEW hive_lateral_view_outer_explode as " +
                "SELECT pageid, adid FROM pageAds LATERAL VIEW OUTER explode(adid_list) adTable AS adid");

        assertViewQuery(env,
                "SELECT * FROM hive_lateral_view_outer_explode",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11),
                        row("two", 22),
                        row("one", 42),
                        row("nothing", null),
                        row("zero", null)));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testLateralViewExplodeArrayOfStructs(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS pageAdsStructs");
        env.executeTrinoUpdate("CREATE TABLE pageAdsStructs(pageid VARCHAR, adid_list ARRAY(ROW(a INTEGER, b INTEGER))) WITH (format='TEXTFILE')");
        env.executeTrinoUpdate("INSERT INTO pageAdsStructs " +
                "VALUES " +
                "  ('two', ARRAY[ROW(11, 12), ROW(13, 14)]), " +
                "  ('nothing', NULL), " +
                "  ('zero', ARRAY[]), " +
                "  ('one', ARRAY[ROW(42, 43)])");

        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_lateral_view_structs");
        env.executeHiveUpdate("CREATE VIEW hive_lateral_view_structs as SELECT pageid, adid FROM pageAdsStructs LATERAL VIEW explode(adid_list) adTable AS adid");

        assertViewQuery(env,
                "SELECT pageid, adid.a, adid.b FROM hive_lateral_view_structs",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11, 12),
                        row("two", 13, 14),
                        row("one", 42, 43)));

        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_lateral_view_structs_outer_explode");
        env.executeHiveUpdate("CREATE VIEW hive_lateral_view_structs_outer_explode as " +
                "SELECT pageid, adid FROM pageAdsStructs LATERAL VIEW OUTER explode(adid_list) adTable AS adid");

        assertViewQuery(env,
                "SELECT pageid, adid.a, adid.b FROM hive_lateral_view_structs_outer_explode",
                queryAssert -> queryAssert.containsOnly(
                        row("two", 11, 12),
                        row("two", 13, 14),
                        row("one", 42, 43),
                        row("nothing", null, null),
                        row("zero", null, null)));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testLateralViewJsonTupleAs(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_json_tuple_table");
        env.executeTrinoUpdate("" +
                "CREATE TABLE test_json_tuple_table WITH (format='TEXTFILE') AS " +
                "SELECT 3 id, CAST('{\"user_id\": 1000, \"first.name\": \"Mateusz\", \"Last Name\": \"Gajewski\", \".NET\": true, \"aNull\": null}' AS varchar) jsonstr");

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_json_tuple_view");
        env.executeHiveUpdate("CREATE VIEW test_json_tuple_view AS " +
                "SELECT `t`.`id`, `x`.`a`, `x`.`b`, `x`.`c`, `x`.`d`, `x`.`e`, `x`.`f` FROM test_json_tuple_table AS `t` " +
                "LATERAL VIEW json_tuple(`t`.`jsonstr`, \"first.name\", \"Last Name\", '.NET', \"user_id\", \"aNull\", \"nonexistentField\") `x` AS `a`, `b`, `c`, `d`, `e`, `f`");

        assertViewQuery(env,
                "SELECT * FROM test_json_tuple_view",
                queryAssert -> queryAssert.containsOnly(row(3, "Mateusz", "Gajewski", "true", "1000", null, null)));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFromUtcTimestamp(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_from_utc_timestamp_source");
        env.executeHiveUpdate("CREATE TABLE test_from_utc_timestamp_source (" +
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
        env.executeTrinoUpdate("INSERT INTO test_from_utc_timestamp_source VALUES ( " +
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

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_from_utc_timestamp_view");
        env.executeHiveUpdate("CREATE VIEW " +
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
        assertThat(env.executeTrino("SELECT * FROM test_from_utc_timestamp_view"))
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
        assertThat(env.executeHive("SELECT * FROM test_from_utc_timestamp_view"))
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

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFromUtcTimestampInvalidTimeZone(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_from_utc_timestamp_invalid_time_zone_source");
        env.executeHiveUpdate("CREATE TABLE test_from_utc_timestamp_invalid_time_zone_source (source_timestamp timestamp)");

        // insert via Trino as we noticed problems with creating test table in Hive using CTAS at one go for some Hive distributions
        env.executeTrinoUpdate("INSERT INTO test_from_utc_timestamp_invalid_time_zone_source VALUES (timestamp '1970-01-30 16:00:00.000')");

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_from_utc_timestamp_invalid_time_zone_view");
        env.executeHiveUpdate("CREATE VIEW " +
                "test_from_utc_timestamp_invalid_time_zone_view " +
                "AS SELECT " +
                "   CAST(from_utc_timestamp(source_timestamp, 'Matrix/Zion') AS STRING) ts_timestamp " +
                "FROM test_from_utc_timestamp_invalid_time_zone_source");

        // check result on Trino
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM test_from_utc_timestamp_invalid_time_zone_view"))
                .hasMessageContaining("'Matrix/Zion' is not a valid time zone");
        // check result on Hive - Hive falls back to GMT in case of dealing with an invalid time zone
        assertThat(env.executeHive("SELECT * FROM test_from_utc_timestamp_invalid_time_zone_view"))
                .containsOnly(row("1970-01-30 16:00:00"));
        env.executeTrinoUpdate("DROP TABLE test_from_utc_timestamp_invalid_time_zone_source");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedFieldWithReservedKeyNames(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_nested_field_with_reserved_key_names_source");
        env.executeHiveUpdate("CREATE TABLE test_nested_field_with_reserved_key_names_source (nested_field_key_word_lower_case struct<`from`:BIGINT>, " +
                "nested_field_key_word_upper_case struct<`FROM`:BIGINT>, nested_field_quote struct<`do...from`:BIGINT>)");

        env.executeTrinoUpdate("INSERT INTO test_nested_field_with_reserved_key_names_source VALUES (row(row(1), row(2), row(3)))");

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_nested_field_with_reserved_key_names_view");
        env.executeHiveUpdate("CREATE VIEW " +
                "test_nested_field_with_reserved_key_names_view " +
                "AS SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote " +
                "FROM test_nested_field_with_reserved_key_names_source");

        assertThat(env.executeHive("SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row("{\"from\":1}", "{\"from\":2}", "{\"do...from\":3}"));

        assertThat(env.executeHive("SELECT nested_field_key_word_lower_case.`from`, nested_field_key_word_upper_case.`from`, nested_field_quote.`do...from` FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(1L, 2L, 3L));

        assertThat(env.executeTrino("SELECT nested_field_key_word_lower_case, nested_field_key_word_upper_case, nested_field_quote FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(Row.builder().addField("from", 1L).build(),
                        Row.builder().addField("from", 2L).build(),
                        Row.builder().addField("do...from", 3L).build()));

        assertThat(env.executeTrino("SELECT nested_field_key_word_lower_case.\"from\", nested_field_key_word_upper_case.\"from\", nested_field_quote.\"do...from\" FROM test_nested_field_with_reserved_key_names_view"))
                .containsOnly(row(1L, 2L, 3L));

        env.executeHiveUpdate("DROP VIEW test_nested_field_with_reserved_key_names_view");
        env.executeHiveUpdate("DROP TABLE test_nested_field_with_reserved_key_names_source");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFromUtcTimestampCornerCases(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS test_from_utc_timestamp_corner_cases_source");
        env.executeTrinoUpdate("CREATE TABLE test_from_utc_timestamp_corner_cases_source AS SELECT * FROM (VALUES " +
                "  CAST(-5000000000001 AS BIGINT)," +
                "  CAST(-1000000000001 AS BIGINT)," +
                "  -1," +
                "  1," +
                "  5000000000001" +
                ")" +
                "AS source(source_bigint)");

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_from_utc_timestamp_corner_cases_view");
        env.executeHiveUpdate("CREATE VIEW " +
                "test_from_utc_timestamp_corner_cases_view " +
                "AS SELECT " +
                "   CAST(from_utc_timestamp(source_bigint, 'America/Los_Angeles') as STRING) ts_bigint " +
                "FROM test_from_utc_timestamp_corner_cases_source");

        // check result on Trino
        assertThat(env.executeTrino("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
                .containsOnly(
                        row("1811-07-23 07:13:41.999"),
                        row("1938-04-24 14:13:19.999"),
                        row("1969-12-31 15:59:59.999"),
                        row("1969-12-31 16:00:00.001"),
                        row("2128-06-11 01:53:20.001"));

        // check result on Hive
        assertThat(env.executeHive("SELECT * FROM test_from_utc_timestamp_corner_cases_view"))
                .containsOnly(
                        row("1811-07-23 07:13:41.999"),
                        row("1938-04-24 14:13:19.999"),
                        row("1969-12-31 15:59:59.999"),
                        row("1969-12-31 16:00:00.001"),
                        row("2128-06-11 01:53:20.001"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCastTimestampAsDecimal(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS cast_timestamp_as_decimal");
        env.executeHiveUpdate("CREATE TABLE cast_timestamp_as_decimal (a_timestamp TIMESTAMP)");
        env.executeHiveUpdate("INSERT INTO cast_timestamp_as_decimal VALUES ('1990-01-02 12:13:14.123456789')");
        env.executeHiveUpdate("DROP VIEW IF EXISTS cast_timestamp_as_decimal_view");
        env.executeHiveUpdate("CREATE VIEW cast_timestamp_as_decimal_view AS SELECT CAST(a_timestamp as DECIMAL(10,0)) a_cast_timestamp FROM cast_timestamp_as_decimal");

        String testQuery = "SELECT * FROM cast_timestamp_as_decimal_view";
        assertViewQuery(env, testQuery, queryAssert -> queryAssert.containsOnly(row(new BigDecimal("631282394"))));

        env.executeHiveUpdate("DROP VIEW cast_timestamp_as_decimal_view");
        env.executeHiveUpdate("DROP TABLE cast_timestamp_as_decimal");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUnionBetweenCharAndVarchar(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS union_char_varchar");
        env.executeHiveUpdate("CREATE TABLE union_char_varchar (a_char char(1), a_varchar varchar(1024))");
        env.executeHiveUpdate("INSERT INTO union_char_varchar VALUES ('a', 'Trino')");
        env.executeHiveUpdate("INSERT INTO union_char_varchar VALUES (NULL, NULL)");
        env.executeHiveUpdate("DROP VIEW IF EXISTS union_char_varchar_view");
        env.executeHiveUpdate(
                "CREATE VIEW union_char_varchar_view AS " +
                        "SELECT a_char AS col FROM union_char_varchar " +
                        "UNION ALL " +
                        "SELECT a_varchar AS col FROM union_char_varchar");

        List<io.trino.testing.containers.environment.Row> expected = List.of(
                row("a"),
                row("Trino"),
                row((Object) null),
                row((Object) null));
        assertThat(env.executeTrino("SELECT * FROM union_char_varchar_view")).containsOnly(expected);
        assertThat(env.executeHive("SELECT * FROM union_char_varchar_view")).containsOnly(expected);

        env.executeHiveUpdate("DROP VIEW union_char_varchar_view");
        env.executeHiveUpdate("DROP TABLE union_char_varchar");
    }

    // ==================== Tests from AbstractTestHiveViews ====================

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectOnView(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_test_view");
        env.executeHiveUpdate("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        assertViewQuery(env, "SELECT * FROM hive_test_view", queryAssert -> queryAssert.hasRowsCount(25));
        assertViewQuery(env,
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive_test_view WHERE n_nationkey < 3",
                queryAssert -> queryAssert.containsOnly(
                        row(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"),
                        row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                        row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testArrayIndexingInView(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_hive_view_array_index_table");
        env.executeHiveUpdate("CREATE TABLE test_hive_view_array_index_table(an_index int, an_array array<string>)");
        env.executeHiveUpdate("INSERT INTO TABLE test_hive_view_array_index_table SELECT 1, array('trino','hive') FROM nation WHERE n_nationkey = 1");

        // literal array index
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_hive_view_array_index_view");
        env.executeHiveUpdate("CREATE VIEW test_hive_view_array_index_view AS SELECT an_array[1] AS sql_dialect FROM test_hive_view_array_index_table");
        assertViewQuery(env,
                "SELECT * FROM test_hive_view_array_index_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));

        // expression array index
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_hive_view_expression_array_index_view");
        env.executeHiveUpdate("CREATE VIEW test_hive_view_expression_array_index_view AS SELECT an_array[an_index] AS sql_dialect FROM test_hive_view_array_index_table");
        assertViewQuery(env,
                "SELECT * FROM test_hive_view_expression_array_index_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCommonTableExpression(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8) SELECT * FROM t");

        assertViewQuery(env, "SELECT * FROM test_common_table_expression",
                queryAssert -> queryAssert.containsOnly(row(8, 2)));

        env.executeHiveUpdate("DROP VIEW test_common_table_expression");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedCommonTableExpression(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_nested_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8), " +
                        "t2 AS (SELECT n_nationkey * 2 AS nationkey, n_regionkey * 2 AS regionkey FROM t) SELECT * FROM t2");

        assertViewQuery(env, "SELECT * FROM test_nested_common_table_expression",
                queryAssert -> queryAssert.containsOnly(row(16, 4)));

        env.executeHiveUpdate("DROP VIEW test_nested_common_table_expression");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testArrayConstructionInView(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_array_construction_view");
        env.executeHiveUpdate("CREATE VIEW test_array_construction_view AS SELECT n_nationkey, array(n_nationkey, n_regionkey) AS a FROM nation");

        assertThat(env.executeHive("SELECT a[0], a[1] FROM test_array_construction_view WHERE n_nationkey = 8"))
                .containsOnly(row(8, 2));
        assertThat(env.executeTrino("SELECT a[1], a[2] FROM test_array_construction_view WHERE n_nationkey = 8"))
                .containsOnly(row(8, 2));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testMapConstructionInView(HiveBasicEnvironment env)
    {
        createOrdersTable(env);
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_map_construction_view AS " +
                        "SELECT" +
                        "  o_orderkey" +
                        ", MAP(o_clerk, o_orderpriority) AS simple_map" +
                        ", MAP(o_clerk, MAP(o_orderpriority, o_shippriority)) AS nested_map" +
                        " FROM orders");

        assertViewQuery(env, "SELECT simple_map['Clerk#000000951'] FROM test_map_construction_view WHERE o_orderkey = 1",
                queryAssert -> queryAssert.containsOnly(row("5-LOW")));
        assertViewQuery(env, "SELECT nested_map['Clerk#000000951']['5-LOW'] FROM test_map_construction_view WHERE o_orderkey = 1",
                queryAssert -> queryAssert.containsOnly(row(0)));

        env.executeHiveUpdate("DROP VIEW test_map_construction_view");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectOnViewFromDifferentSchema(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_schema CASCADE");
        env.executeHiveUpdate("CREATE SCHEMA test_schema");
        env.executeHiveUpdate(
                "CREATE VIEW test_schema.hive_test_view_1 AS SELECT * FROM " +
                        // no schema is specified in purpose
                        "nation");

        assertViewQuery(env, "SELECT * FROM test_schema.hive_test_view_1", queryAssert -> queryAssert.hasRowsCount(25));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testViewReferencingTableInDifferentSchema(HiveBasicEnvironment env)
    {
        String schemaX = "test_view_table_in_different_schema_x" + randomNameSuffix();
        String schemaY = "test_view_table_in_different_schema_y" + randomNameSuffix();
        String tableName = "test_table";
        String viewName = "test_view";

        env.executeHiveUpdate(format("CREATE SCHEMA %s", schemaX));
        env.executeHiveUpdate(format("CREATE SCHEMA %s", schemaY));

        env.executeTrinoUpdate(format("CREATE TABLE %s.%s AS SELECT * FROM tpch.tiny.nation", schemaY, tableName));
        env.executeHiveUpdate(format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s", schemaX, viewName, schemaY, tableName));

        assertThat(env.executeTrino(format("SELECT COUNT(*) FROM %s.%s", schemaX, viewName))).containsOnly(row(25));

        env.executeHiveUpdate(format("DROP SCHEMA %s CASCADE", schemaX));
        env.executeHiveUpdate(format("DROP SCHEMA %s CASCADE", schemaY));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testViewReferencingTableInTheSameSchemaWithoutQualifier(HiveBasicEnvironment env)
    {
        String schemaX = "test_view_table_same_schema_without_qualifier_schema" + randomNameSuffix();
        String tableName = "test_table";
        String viewName = "test_view";

        env.executeHiveUpdate(format("CREATE SCHEMA %s", schemaX));

        env.executeTrinoUpdate(format("CREATE TABLE %s.%s AS SELECT * FROM tpch.tiny.nation", schemaX, tableName));
        env.executeHiveUpdate(format("USE %s", schemaX));
        env.executeHiveUpdate(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));

        assertThat(env.executeTrino(format("SELECT COUNT(*) FROM %s.%s", schemaX, viewName))).containsOnly(row(25));

        env.executeHiveUpdate(format("DROP SCHEMA %s CASCADE", schemaX));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    void testViewWithUnsupportedCoercion(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_unsupported_coercion");
        env.executeHiveUpdate("CREATE VIEW view_with_unsupported_coercion AS SELECT length(n_comment) FROM nation");

        assertThatThrownBy(() -> env.executeTrino("SELECT COUNT(*) FROM view_with_unsupported_coercion"))
                .hasMessageContaining("View 'hive.default.view_with_unsupported_coercion' is stale or in invalid state: a column of type bigint projected from query view at position 0 has no name");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testOuterParentheses(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("CREATE OR REPLACE VIEW view_outer_parentheses AS (SELECT 'parentheses' AS col FROM nation LIMIT 1)");

        assertViewQuery(env, "SELECT * FROM view_outer_parentheses",
                queryAssert -> queryAssert.containsOnly(row("parentheses")));

        env.executeHiveUpdate("DROP VIEW view_outer_parentheses");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testDateFunction(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_date_function");
        env.executeHiveUpdate("CREATE TABLE hive_table_date_function(s string)");
        env.executeHiveUpdate("INSERT INTO hive_table_date_function (s) values ('2021-08-21')");
        env.executeHiveUpdate("CREATE OR REPLACE VIEW hive_view_date_function AS SELECT date(s) AS col FROM hive_table_date_function");

        assertViewQuery(env, "SELECT * FROM hive_view_date_function",
                queryAssert -> queryAssert.containsOnly(row(sqlDate(2021, 8, 21))));

        env.executeHiveUpdate("DROP VIEW hive_view_date_function");
        env.executeHiveUpdate("DROP TABLE hive_table_date_function");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testPmodFunction(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_pmod_function");
        env.executeHiveUpdate("CREATE TABLE hive_table_pmod_function(n int, m int)");
        env.executeHiveUpdate("INSERT INTO hive_table_pmod_function (n, m) values (-5, 2)");
        env.executeHiveUpdate("CREATE OR REPLACE VIEW hive_view_pmod_function AS SELECT pmod(n, m) AS col FROM hive_table_pmod_function");

        assertViewQuery(env, "SELECT * FROM hive_view_pmod_function",
                queryAssert -> queryAssert.containsOnly(row(1)));

        env.executeHiveUpdate("DROP VIEW hive_view_pmod_function");
        env.executeHiveUpdate("DROP TABLE hive_table_pmod_function");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    void testWithUnsupportedFunction(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_repeat_function");
        env.executeHiveUpdate("CREATE VIEW view_with_repeat_function AS SELECT REPEAT(n_comment,2) FROM nation");

        assertThatThrownBy(() -> env.executeTrino("SELECT COUNT(*) FROM view_with_repeat_function"))
                .hasMessageContaining("View 'hive.default.view_with_repeat_function' is stale or in invalid state: a column of type array(varchar(152)) projected from query view at position 0 has no name");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testExistingView(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_duplicate_view");
        env.executeHiveUpdate("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertThatThrownBy(() -> env.executeTrino("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation"))
                .hasMessageContaining("View already exists");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testShowCreateView(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_show_view");
        env.executeHiveUpdate("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        try {
            String showCreateViewSql = "SHOW CREATE VIEW %s.default.hive_show_view";
            String expectedResult = "CREATE VIEW %s.default.hive_show_view SECURITY DEFINER AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  \"default\".\"nation\" \"nation\"";

            QueryResult actualResult = env.executeTrino(format(showCreateViewSql, "hive"));
            assertThat(actualResult).hasRowsCount(1);
            assertThat((String) actualResult.getOnlyValue()).isEqualTo(format(expectedResult, "hive"));

            // Verify the translated view sql for a catalog other than "hive", which is configured to the same metastore
            actualResult = env.executeTrino(format(showCreateViewSql, "hive_with_external_writes"));
            assertThat(actualResult).hasRowsCount(1);
            assertThat((String) actualResult.getOnlyValue()).isEqualTo(format(expectedResult, "hive_with_external_writes"));
        }
        finally {
            env.executeHiveUpdate("DROP VIEW IF EXISTS hive_show_view");
        }
    }

    /**
     * Test view containing IF, IN, LIKE, BETWEEN, CASE, COALESCE, operators, delimited and non-delimited columns, an inline comment
     */
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testRichSqlSyntax(HiveBasicEnvironment env)
    {
        createNationTable(env);
        createOrdersTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_rich_syntax");
        env.executeHiveUpdate("CREATE VIEW view_with_rich_syntax AS " +
                "SELECT \n" +
                "   `n_nationkey`, \n" + // grave accent
                "   n_name, \n" + // no grave accent
                "   `n_regionkey` AS `n_regionkey`, \n" + // alias
                "   n_regionkey BETWEEN 1 AND 2 AS region_between_1_2, \n" + // BETWEEN, boolean
                "   IF(`n`.`n_name` IN ('ALGERIA', 'ARGENTINA'), 1, 0) AS `starts_with_a`, \n" +
                "   IF(`n`.`n_name` != 'PERU', 1, 0) `not_peru`, \n" + // no "AS" here
                "   IF(`n`.`n_name` LIKE '%N%', 1, 0) `CONTAINS_N`, \n" + // LIKE, uppercase column name
                // TODO (https://github.com/trinodb/trino/issues/5837) "   CASE n_regionkey WHEN 0 THEN 'Africa' WHEN 1 THEN 'America' END region_name, \n" + // simple CASE
                "   CASE WHEN n_name = \"BRAZIL\" THEN 'is BRAZIL' WHEN n_name = \"ALGERIA\" THEN 'is ALGERIA' ELSE \"\" END is_something,\n" + // searched CASE, double quote string literals
                "   COALESCE(IF(n_name LIKE 'A%', NULL, n_name), 'A%') AS coalesced_name, \n" + // coalesce
                "   round(tan(n_nationkey), 3) AS rounded_tan, \n" + // functions
                "   o_orderdate AS the_orderdate, \n" +
                "   `n`.`n_nationkey` + `n_nationkey` + n.n_nationkey + n_nationkey + 10000 - -1 AS arithmetic--some comment without leading space \n" +
                "FROM `default`.`nation` AS `n` \n" +
                // join, subquery
                "LEFT JOIN (SELECT * FROM orders WHERE o_custkey > 1000) `o` ON `o`.`o_orderkey` = `n`.`n_nationkey` ");
        assertViewQuery(env, "" +
                        "SELECT" +
                        "   n_nationkey, n_name, region_between_1_2, starts_with_a, not_peru, contains_n, is_something, coalesced_name," +
                        "   rounded_tan, the_orderdate, arithmetic " +
                        "FROM view_with_rich_syntax " +
                        "WHERE n_regionkey < 3 AND (n_nationkey < 5 OR n_nationkey IN (12, 17))",
                queryAssert -> queryAssert.containsOnly(
                        row(0, "ALGERIA", false, 1, 1, 0, "is ALGERIA", "A%", 0.0, null, 10001),
                        row(1, "ARGENTINA", true, 1, 1, 1, "", "A%", 1.557, sqlDate(1996, 1, 2), 10005),
                        row(2, "BRAZIL", true, 0, 1, 0, "is BRAZIL", "BRAZIL", -2.185, sqlDate(1996, 12, 1), 10009),
                        row(3, "CANADA", true, 0, 1, 1, "", "CANADA", -0.143, sqlDate(1993, 10, 14), 10013),
                        row(12, "JAPAN", true, 0, 1, 1, "", "JAPAN", -0.636, null, 10049),
                        row(17, "PERU", true, 0, 0, 0, "", "PERU", 3.494, null, 10069)));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testIdentifierThatStartWithDigit(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS \"7_table_with_number\"");
        env.executeTrinoUpdate("CREATE TABLE \"7_table_with_number\" WITH (format='TEXTFILE') AS SELECT VARCHAR 'abc' x");

        env.executeHiveUpdate("DROP VIEW IF EXISTS view_on_identifiers_starting_with_numbers");
        env.executeHiveUpdate("CREATE VIEW view_on_identifiers_starting_with_numbers AS SELECT * FROM 7_table_with_number");

        assertViewQuery(env,
                "SELECT * FROM view_on_identifiers_starting_with_numbers",
                queryAssert -> queryAssert.contains(row("abc")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveViewInInformationSchema(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_schema CASCADE");

        env.executeHiveUpdate("CREATE SCHEMA test_schema");
        env.executeHiveUpdate("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        env.executeHiveUpdate("CREATE TABLE test_schema.hive_table(a string)");
        env.executeTrinoUpdate("CREATE TABLE test_schema.trino_table(a int)");
        env.executeTrinoUpdate("CREATE VIEW test_schema.trino_test_view AS SELECT * FROM nation");

        assertThat(env.executeTrino("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'")).containsOnly(
                row("hive", "test_schema", "trino_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_test_view", "VIEW"),
                row("hive", "test_schema", "trino_test_view", "VIEW"));

        assertThat(env.executeTrino("SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'")).containsOnly(
                row("SELECT *\nFROM \"default\".\"nation\" AS \"nation\""));

        assertThat(env.executeTrino("DESCRIBE test_schema.hive_test_view"))
                .contains(row("n_nationkey", "bigint", "", ""));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveViewWithParametrizedTypes(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_view_parametrized");
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_parametrized");

        env.executeHiveUpdate("CREATE TABLE hive_table_parametrized(a decimal(20,4), b bigint, c varchar(20))");
        env.executeHiveUpdate("CREATE VIEW hive_view_parametrized AS SELECT * FROM hive_table_parametrized");
        env.executeHiveUpdate("INSERT INTO TABLE hive_table_parametrized VALUES (1.2345, 42, 'bar')");

        assertViewQuery(env,
                "SELECT * FROM hive_view_parametrized",
                queryAssert -> queryAssert.containsOnly(row(new BigDecimal("1.2345"), 42, "bar")));

        assertThat(env.executeTrino("SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'")).containsOnly(
                row("decimal(20,4)"),
                row("bigint"),
                row("varchar"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHiveViewWithTextualTypes(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_view_textual");
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_textual");

        // In Hive, columns with `char` type have a fixed length between 1 and 255, columns with `varchar` type can have a length between 1 and 65535
        env.executeHiveUpdate("CREATE TABLE hive_table_textual(a_char_1 char(1), a_char_255 char(255), a_varchar_1 varchar(1), a_varchar_65535 varchar(65535), a_string string)");
        env.executeHiveUpdate("CREATE VIEW hive_view_textual AS SELECT * FROM hive_table_textual");
        env.executeHiveUpdate("INSERT INTO TABLE hive_table_textual VALUES ('a', 'rainy', 'i', 'calendar', 'Boston Red Sox')");

        assertViewQuery(env,
                "SELECT * FROM hive_view_textual",
                queryAssert -> queryAssert.containsOnly(row("a", padEnd("rainy", 255, ' '), "i", "calendar", "Boston Red Sox")));
        assertViewQuery(env,
                "SELECT a_char_1, a_varchar_65535 FROM hive_view_textual WHERE a_string = 'Boston Red Sox'",
                queryAssert -> queryAssert.containsOnly(row("a", "calendar")));

        // Expected types for Trino (not legacy) - varchar and string are unbounded varchar
        assertThat(env.executeTrino("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'hive_view_textual'"))
                .containsOnly(
                        row("a_char_1", "char(1)"),
                        row("a_char_255", "char(255)"),
                        row("a_varchar_1", "varchar"),
                        row("a_varchar_65535", "varchar"),
                        row("a_string", "varchar"));

        env.executeHiveUpdate("DROP VIEW hive_view_textual");
        env.executeHiveUpdate("DROP TABLE hive_table_textual");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedHiveViews(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_base_view");
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_middle_view");
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_top_view");

        env.executeHiveUpdate("CREATE VIEW nested_base_view AS SELECT n_nationkey as k, n_name as n, n_regionkey as r, n_comment as c FROM nation");
        env.executeHiveUpdate("CREATE VIEW nested_middle_view AS SELECT n, c FROM nested_base_view WHERE k = 14");
        env.executeHiveUpdate("CREATE VIEW nested_top_view AS SELECT n AS n_renamed FROM nested_middle_view");

        assertViewQuery(env,
                "SELECT n_renamed FROM nested_top_view",
                queryAssert -> queryAssert.containsOnly(row("KENYA")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectFromHiveViewWithoutDefaultCatalogAndSchema(HiveBasicEnvironment env)
            throws SQLException
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS no_catalog_schema_view");
        env.executeHiveUpdate("CREATE VIEW no_catalog_schema_view AS SELECT * FROM nation WHERE n_nationkey = 1");

        // Test with connection without default catalog/schema
        try (Connection conn = env.createTrinoConnectionWithoutDefaultCatalog();
                Statement stmt = conn.createStatement()) {
            // Query without schema should fail
            assertThatThrownBy(() -> stmt.executeQuery("SELECT count(*) FROM no_catalog_schema_view"))
                    .hasMessageMatching(".*Schema must be specified when session schema is not set.*");
            // Query with fully qualified name should succeed
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM hive.default.no_catalog_schema_view")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(1L);
            }
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testTimestampHiveView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS timestamp_hive_table");
        env.executeHiveUpdate("CREATE TABLE timestamp_hive_table (ts timestamp)");
        env.executeHiveUpdate("INSERT INTO timestamp_hive_table (ts) values ('1990-01-02 12:13:14.123456789')");
        env.executeHiveUpdate("DROP VIEW IF EXISTS timestamp_hive_view");
        env.executeHiveUpdate("CREATE VIEW timestamp_hive_view AS SELECT * FROM timestamp_hive_table");

        // Default catalog truncates to milliseconds
        assertThat(env.executeTrino("SELECT CAST(ts AS varchar) FROM timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123"));
        // Nanoseconds catalog preserves full precision
        assertThat(env.executeTrino("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123456789"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCurrentUser(HiveBasicEnvironment env)
            throws SQLException
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS current_user_hive_view");
        env.executeHiveUpdate("CREATE VIEW current_user_hive_view as SELECT current_user() AS cu FROM nation LIMIT 1");

        String testQuery = "SELECT cu FROM current_user_hive_view";
        // Default connection uses 'hive' user
        assertThat(env.executeTrino(testQuery)).containsOnly(row("hive"));
        // Test with alice user
        try (Connection conn = env.createTrinoConnectionAsUser("alice");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(testQuery)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("alice");
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedGroupBy(HiveBasicEnvironment env)
    {
        createNationTable(env);
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_nested_group_by_view");
        env.executeHiveUpdate("CREATE VIEW test_nested_group_by_view AS SELECT n_regionkey, count(1) count FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey ) t GROUP BY n_regionkey");

        assertViewQuery(env,
                "SELECT * FROM test_nested_group_by_view",
                queryAssert -> queryAssert.containsOnly(
                        row(0, 1),
                        row(1, 1),
                        row(2, 1),
                        row(3, 1),
                        row(4, 1)));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUnionAllViews(HiveBasicEnvironment env)
    {
        createRegionTable(env);
        env.executeHiveUpdate("DROP TABLE IF EXISTS union_helper");
        env.executeHiveUpdate("CREATE TABLE union_helper (\n"
                + "r_regionkey BIGINT,\n"
                + "r_name VARCHAR(25),\n"
                + "r_comment VARCHAR(152)\n"
                + ")");
        env.executeHiveUpdate("INSERT INTO union_helper\n"
                + "SELECT r_regionkey % 3, r_name, r_comment FROM region");

        env.executeHiveUpdate("DROP VIEW IF EXISTS union_all_view");
        env.executeHiveUpdate("CREATE VIEW union_all_view AS\n"
                + "SELECT r_regionkey FROM region\n"
                + "UNION ALL\n"
                + "SELECT r_regionkey FROM union_helper\n");

        assertThat(env.executeTrino("SELECT r_regionkey FROM union_all_view"))
                // Copy the keys 5 times because there are 5 nations per region
                .containsOnly(
                        // base rows
                        row(0),
                        row(1),
                        row(2),
                        row(3),
                        row(4),
                        // mod 3
                        row(0),
                        row(1),
                        row(2),
                        row(0),
                        row(1));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUnionDistinctViews(HiveBasicEnvironment env)
    {
        createRegionTable(env);
        env.executeHiveUpdate("DROP TABLE IF EXISTS union_helper");
        env.executeHiveUpdate("CREATE TABLE union_helper (\n"
                + "r_regionkey BIGINT,\n"
                + "r_name VARCHAR(25),\n"
                + "r_comment VARCHAR(152)\n"
                + ")");
        env.executeHiveUpdate("INSERT INTO union_helper\n"
                + "SELECT r_regionkey % 3, r_name, r_comment FROM region");

        for (String operator : List.of("UNION", "UNION DISTINCT")) {
            String name = format("%s_view", operator.replace(" ", "_"));
            env.executeHiveUpdate(format("DROP VIEW IF EXISTS %s", name));
            // Add mod to one side to add duplicate and non-overlapping values
            env.executeHiveUpdate(format(
                    "CREATE VIEW %s AS\n"
                            + "SELECT r_regionkey FROM region\n"
                            + "%s\n"
                            + "SELECT r_regionkey FROM union_helper\n",
                    name,
                    operator));

            assertViewQuery(env,
                    format("SELECT r_regionkey FROM %s", name),
                    assertion -> assertion.as("View with %s", operator)
                            .containsOnly(row(0), row(1), row(2), row(3), row(4)));
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHivePartitionViews(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_view_partitioned_column");
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_table_partitioned_column");
        env.executeTrinoUpdate("CREATE TABLE test_table_partitioned_column(some_id VARCHAR(25), ds VARCHAR(25)) WITH (partitioned_by=array['ds'])");
        env.executeTrinoUpdate("INSERT INTO test_table_partitioned_column VALUES ('1', '2022-09-17')");
        env.executeHiveUpdate("CREATE VIEW test_view_partitioned_column PARTITIONED ON (ds) AS SELECT some_id, ds FROM test_table_partitioned_column");

        String testQuery = "SELECT some_id, ds FROM test_view_partitioned_column";
        assertThat(env.executeTrino(testQuery)).containsOnly(row("1", "2022-09-17"));
        env.executeHiveUpdate("DROP VIEW test_view_partitioned_column");
        env.executeHiveUpdate("DROP TABLE test_table_partitioned_column");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testViewWithColumnAliasesDifferingInCase(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_hive_namesake_column_name_a");
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_hive_namesake_column_name_b");
        env.executeHiveUpdate("CREATE TABLE test_hive_namesake_column_name_a(some_id string)");
        env.executeHiveUpdate("CREATE TABLE test_hive_namesake_column_name_b(SOME_ID string)");
        env.executeHiveUpdate("INSERT INTO TABLE test_hive_namesake_column_name_a VALUES ('hive')");
        env.executeHiveUpdate("INSERT INTO TABLE test_hive_namesake_column_name_b VALUES (' hive ')");

        env.executeHiveUpdate("DROP VIEW IF EXISTS test_namesake_column_names_view");
        env.executeHiveUpdate("" +
                "CREATE VIEW test_namesake_column_names_view AS \n" +
                "    SELECT a.some_id FROM test_hive_namesake_column_name_a a \n" +
                "    LEFT JOIN (SELECT trim(SOME_ID) AS SOME_ID FROM test_hive_namesake_column_name_b) b \n" +
                "       ON a.some_id = b.some_id \n" +
                "    WHERE a.some_id != ''");
        assertViewQuery(env,
                "SELECT * FROM test_namesake_column_names_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));

        env.executeHiveUpdate("DROP TABLE test_hive_namesake_column_name_a");
        env.executeHiveUpdate("DROP TABLE test_hive_namesake_column_name_b");
        env.executeHiveUpdate("DROP VIEW test_namesake_column_names_view");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testRunAsInvoker(HiveBasicEnvironment env)
            throws SQLException
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.run_as_invoker");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.run_as_invoker_view");

        env.executeTrinoUpdate("CREATE TABLE hive.default.run_as_invoker (a INTEGER)");
        env.executeHiveUpdate("CREATE VIEW run_as_invoker_view AS SELECT * FROM default.run_as_invoker");
        String definerQuery = "SELECT * FROM hive.default.run_as_invoker_view";
        String invokerQuery = "SELECT * FROM hive_with_run_view_as_invoker.default.run_as_invoker_view";

        // Querying as the definer should succeed.
        try (Connection conn = env.createTrinoConnectionAsUser("alice");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(definerQuery)) {
            assertThat(rs.next()).isFalse(); // No rows, but query is allowed
        }

        // Alice cannot query the invoker view (runs as alice, who lacks access)
        try (Connection conn = env.createTrinoConnectionAsUser("alice");
                Statement stmt = conn.createStatement()) {
            assertThatThrownBy(() -> stmt.executeQuery(invokerQuery))
                    .hasMessageContaining("Access Denied");
        }

        env.executeHiveUpdate("DROP VIEW run_as_invoker_view");
        env.executeTrinoUpdate("DROP TABLE hive.default.run_as_invoker");
    }

    // ==================== Helper Methods ====================

    private static void assertViewQuery(HiveBasicEnvironment env, String query, Consumer<QueryResultAssert> assertion)
    {
        // Ensure Hive and Trino view compatibility by comparing the results
        assertion.accept(assertThat(env.executeHive(query)));
        assertion.accept(assertThat(env.executeTrino(query)));
    }

    private static Date sqlDate(int year, int month, int day)
    {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    /**
     * Creates the nation table from TPCH if it doesn't exist.
     */
    private static void createNationTable(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT * FROM tpch.tiny.nation");
    }

    /**
     * Creates the orders table from TPCH if it doesn't exist.
     */
    private static void createOrdersTable(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM tpch.tiny.orders");
    }

    /**
     * Creates the region table from TPCH if it doesn't exist.
     */
    private static void createRegionTable(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE IF NOT EXISTS region AS SELECT * FROM tpch.tiny.region");
    }
}
