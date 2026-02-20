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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
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

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive views with legacy translation mode enabled.
 * <p>
 * Ported from the Tempto-based TestHiveViewsLegacy which extends AbstractTestHiveViews.
 * <p>
 * These tests run with the session property {@code hive.hive_views_legacy_translation = true}
 * which enables a different view translation path compared to the default mode.
 * <p>
 * This class includes:
 * - 21 tests inherited from AbstractTestHiveViews (adapted for legacy mode)
 * - Some tests are overridden with different behavior expectations in legacy mode
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveViewsLegacy
{
    /**
     * Helper method to execute Hive queries in the legacy view mode session.
     */
    private void executeInSession(HiveBasicEnvironment env, SessionConsumer consumer)
    {
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.hive_views_legacy_translation = true");
            consumer.accept(session);
        });
    }

    /**
     * Helper method to execute a Trino query in legacy view mode and return the result.
     */
    private QueryResult executeTrinoWithLegacyMode(HiveBasicEnvironment env, String sql)
    {
        final QueryResult[] result = new QueryResult[1];
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.hive_views_legacy_translation = true");
            result[0] = session.executeQuery(sql);
        });
        return result[0];
    }

    private static Date sqlDate(int year, int month, int day)
    {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    // ==================== Tests from AbstractTestHiveViews ====================

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectOnView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_test_view");
        env.executeHiveUpdate("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM hive_test_view");
            assertThat(result).hasRowsCount(25);

            QueryResult filteredResult = session.executeQuery(
                    "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive_test_view WHERE n_nationkey < 3");
            assertThat(filteredResult).containsOnly(
                    row(0L, "ALGERIA", 0L, " haggle. carefully final deposits detect slyly agai"),
                    row(1L, "ARGENTINA", 1L, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                    row(2L, "BRAZIL", 1L, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testRichSqlSyntax(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_rich_syntax");
        env.executeHiveUpdate("CREATE VIEW view_with_rich_syntax AS " +
                "SELECT \n" +
                "   `n_nationkey`, \n" +
                "   n_name, \n" +
                "   `n_regionkey` AS `n_regionkey`, \n" +
                "   n_regionkey BETWEEN 1 AND 2 AS region_between_1_2, \n" +
                "   IF(`n`.`n_name` IN ('ALGERIA', 'ARGENTINA'), 1, 0) AS `starts_with_a`, \n" +
                "   IF(`n`.`n_name` != 'PERU', 1, 0) `not_peru`, \n" +
                "   IF(`n`.`n_name` LIKE '%N%', 1, 0) `CONTAINS_N`, \n" +
                "   CASE WHEN n_name = \"BRAZIL\" THEN 'is BRAZIL' WHEN n_name = \"ALGERIA\" THEN 'is ALGERIA' ELSE \"\" END is_something,\n" +
                "   COALESCE(IF(n_name LIKE 'A%', NULL, n_name), 'A%') AS coalesced_name, \n" +
                "   round(tan(n_nationkey), 3) AS rounded_tan, \n" +
                "   o_orderdate AS the_orderdate, \n" +
                "   `n`.`n_nationkey` + `n_nationkey` + n.n_nationkey + n_nationkey + 10000 - -1 AS arithmetic--some comment without leading space \n" +
                "FROM `default`.`nation` AS `n` \n" +
                "LEFT JOIN (SELECT * FROM orders WHERE o_custkey > 1000) `o` ON `o`.`o_orderkey` = `n`.`n_nationkey` ");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("" +
                    "SELECT" +
                    "   n_nationkey, n_name, region_between_1_2, starts_with_a, not_peru, contains_n, is_something, coalesced_name," +
                    "   rounded_tan, the_orderdate, arithmetic " +
                    "FROM view_with_rich_syntax " +
                    "WHERE n_regionkey < 3 AND (n_nationkey < 5 OR n_nationkey IN (12, 17))");
            assertThat(result).containsOnly(
                    row(0L, "ALGERIA", false, 1, 1, 0, "is ALGERIA", "A%", 0.0, null, 10001L),
                    row(1L, "ARGENTINA", true, 1, 1, 1, "", "A%", 1.557, sqlDate(1996, 1, 2), 10005L),
                    row(2L, "BRAZIL", true, 0, 1, 0, "is BRAZIL", "BRAZIL", -2.185, sqlDate(1996, 12, 1), 10009L),
                    row(3L, "CANADA", true, 0, 1, 1, "", "CANADA", -0.143, sqlDate(1993, 10, 14), 10013L),
                    row(12L, "JAPAN", true, 0, 1, 1, "", "JAPAN", -0.636, null, 10049L),
                    row(17L, "PERU", true, 0, 0, 0, "", "PERU", 3.494, null, 10069L));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCommonTableExpression(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8) SELECT * FROM t");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM test_common_table_expression");
            assertThat(result).containsOnly(row(8L, 2L));
        });

        env.executeHiveUpdate("DROP VIEW test_common_table_expression");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedCommonTableExpression(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_nested_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8), " +
                        "t2 AS (SELECT n_nationkey * 2 AS nationkey, n_regionkey * 2 AS regionkey FROM t) SELECT * FROM t2");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM test_nested_common_table_expression");
            assertThat(result).containsOnly(row(16L, 4L));
        });

        env.executeHiveUpdate("DROP VIEW test_nested_common_table_expression");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectOnViewFromDifferentSchema(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_schema CASCADE");
        env.executeHiveUpdate("CREATE SCHEMA test_schema");
        env.executeHiveUpdate(
                "CREATE VIEW test_schema.hive_test_view_1 AS SELECT * FROM " +
                        // no schema is specified on purpose
                        "nation");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM test_schema.hive_test_view_1");
            assertThat(result).hasRowsCount(25);
        });
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

        env.executeTrinoUpdate(format("CREATE TABLE hive.%s.%s AS SELECT * FROM tpch.tiny.nation", schemaY, tableName));
        env.executeHiveUpdate(format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s", schemaX, viewName, schemaY, tableName));

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery(format("SELECT COUNT(*) FROM %s.%s", schemaX, viewName));
            assertThat(result).containsOnly(row(25L));
        });

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

        env.executeTrinoUpdate(format("CREATE TABLE hive.%s.%s AS SELECT * FROM tpch.tiny.nation", schemaX, tableName));
        env.executeHiveUpdate(format("USE %s", schemaX));
        env.executeHiveUpdate(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery(format("SELECT COUNT(*) FROM %s.%s", schemaX, viewName));
            assertThat(result).containsOnly(row(25L));
        });

        env.executeHiveUpdate(format("DROP SCHEMA %s CASCADE", schemaX));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testViewWithUnsupportedCoercion(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_unsupported_coercion");
        env.executeHiveUpdate("CREATE VIEW view_with_unsupported_coercion AS SELECT length(n_comment) FROM nation");

        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT COUNT(*) FROM view_with_unsupported_coercion"))
                .hasMessageContaining("View 'hive.default.view_with_unsupported_coercion' is stale or in invalid state: a column of type bigint projected from query view at position 0 has no name");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testOuterParentheses(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("CREATE OR REPLACE VIEW view_outer_parentheses AS (SELECT 'parentheses' AS col FROM nation LIMIT 1)");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM view_outer_parentheses");
            assertThat(result).containsOnly(row("parentheses"));
        });

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

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM hive_view_date_function");
            assertThat(result).containsOnly(row(sqlDate(2021, 8, 21)));
        });

        env.executeHiveUpdate("DROP VIEW hive_view_date_function");
        env.executeHiveUpdate("DROP TABLE hive_table_date_function");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testWithUnsupportedFunction(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS view_with_repeat_function");
        env.executeHiveUpdate("CREATE VIEW view_with_repeat_function AS SELECT REPEAT(n_comment,2) FROM nation");

        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT COUNT(*) FROM view_with_repeat_function"))
                .hasMessageContaining("View 'hive.default.view_with_repeat_function' is stale or in invalid state: a column of type array(varchar(152)) projected from query view at position 0 has no name");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testExistingView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_duplicate_view");
        env.executeHiveUpdate("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertThatThrownBy(() -> env.executeTrinoUpdate("CREATE VIEW hive.default.hive_duplicate_view AS SELECT * FROM hive.default.nation"))
                .hasMessageContaining("View already exists");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testIdentifierThatStartWithDigit(HiveBasicEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.\"7_table_with_number\"");
        env.executeTrinoUpdate("CREATE TABLE hive.default.\"7_table_with_number\" WITH (format='TEXTFILE') AS SELECT VARCHAR 'abc' x");

        env.executeHiveUpdate("DROP VIEW IF EXISTS view_on_identifiers_starting_with_numbers");
        env.executeHiveUpdate("CREATE VIEW view_on_identifiers_starting_with_numbers AS SELECT * FROM 7_table_with_number");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM view_on_identifiers_starting_with_numbers");
            assertThat(result).contains(row("abc"));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectFromHiveViewWithoutDefaultCatalogAndSchema(HiveBasicEnvironment env)
            throws SQLException
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS no_catalog_schema_view");
        env.executeHiveUpdate("CREATE VIEW no_catalog_schema_view AS SELECT * FROM nation WHERE n_nationkey = 1");

        try (Connection conn = env.createTrinoConnectionWithoutDefaultCatalog();
                Statement stmt = conn.createStatement()) {
            assertThatThrownBy(() -> stmt.executeQuery("SELECT count(*) FROM no_catalog_schema_view"))
                    .hasMessageMatching(".*Schema must be specified when session schema is not set.*");

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

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT CAST(ts AS varchar) FROM timestamp_hive_view");
            assertThat(result).containsOnly(row("1990-01-02 12:13:14.123"));
        });

        assertThat(env.executeTrino("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view"))
                .containsOnly(row("1990-01-02 12:13:14.123456789"));

        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = 'MILLISECONDS'");
            session.executeUpdate("SET SESSION hive_timestamp_nanos.timestamp_precision = 'MILLISECONDS'");

            assertThat(session.executeQuery("SELECT CAST(ts AS varchar) FROM timestamp_hive_view"))
                    .containsOnly(row("1990-01-02 12:13:14.123"));
            assertThat(session.executeQuery("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view"))
                    .containsOnly(row("1990-01-02 12:13:14.123456789"));
        });

        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = 'NANOSECONDS'");
            session.executeUpdate("SET SESSION hive_timestamp_nanos.timestamp_precision = 'NANOSECONDS'");

            assertThat(session.executeQuery("SELECT CAST(ts AS varchar) FROM timestamp_hive_view"))
                    .containsOnly(row("1990-01-02 12:13:14.123"));
            assertThat(session.executeQuery("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view"))
                    .containsOnly(row("1990-01-02 12:13:14.123456789"));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedGroupBy(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_nested_group_by_view");
        env.executeHiveUpdate("CREATE VIEW test_nested_group_by_view AS SELECT n_regionkey, count(1) count FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey ) t GROUP BY n_regionkey");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM test_nested_group_by_view");
            assertThat(result).containsOnly(
                    row(0L, 1L),
                    row(1L, 1L),
                    row(2L, 1L),
                    row(3L, 1L),
                    row(4L, 1L));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUnionAllViews(HiveBasicEnvironment env)
    {
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

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT r_regionkey FROM union_all_view");
            assertThat(result).containsOnly(
                    // base rows
                    row(0L),
                    row(1L),
                    row(2L),
                    row(3L),
                    row(4L),
                    // mod 3
                    row(0L),
                    row(1L),
                    row(2L),
                    row(0L),
                    row(1L));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUnionDistinctViews(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS union_helper");
        env.executeHiveUpdate("CREATE TABLE union_helper (\n"
                + "r_regionkey BIGINT,\n"
                + "r_name VARCHAR(25),\n"
                + "r_comment VARCHAR(152)\n"
                + ")");
        env.executeHiveUpdate("INSERT INTO union_helper\n"
                + "SELECT r_regionkey % 3, r_name, r_comment FROM region");

        // Test UNION view
        env.executeHiveUpdate("DROP VIEW IF EXISTS UNION_view");
        env.executeHiveUpdate(
                "CREATE VIEW UNION_view AS\n"
                        + "SELECT r_regionkey FROM region\n"
                        + "UNION\n"
                        + "SELECT r_regionkey FROM union_helper\n");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT r_regionkey FROM UNION_view");
            assertThat(result).containsOnly(row(0L), row(1L), row(2L), row(3L), row(4L));
        });

        // Test UNION DISTINCT view
        env.executeHiveUpdate("DROP VIEW IF EXISTS UNION_DISTINCT_view");
        env.executeHiveUpdate(
                "CREATE VIEW UNION_DISTINCT_view AS\n"
                        + "SELECT r_regionkey FROM region\n"
                        + "UNION DISTINCT\n"
                        + "SELECT r_regionkey FROM union_helper\n");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT r_regionkey FROM UNION_DISTINCT_view");
            assertThat(result).containsOnly(row(0L), row(1L), row(2L), row(3L), row(4L));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testHivePartitionViews(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_view_partitioned_column");
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_table_partitioned_column");
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table_partitioned_column(some_id VARCHAR(25), ds VARCHAR(25)) WITH (partitioned_by=array['ds'])");
        env.executeTrinoUpdate("INSERT INTO hive.default.test_table_partitioned_column VALUES ('1', '2022-09-17')");
        env.executeHiveUpdate("CREATE VIEW test_view_partitioned_column PARTITIONED ON (ds) AS SELECT some_id, ds FROM test_table_partitioned_column");

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT some_id, ds FROM test_view_partitioned_column");
            assertThat(result).containsOnly(row("1", "2022-09-17"));
        });

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

        executeInSession(env, session -> {
            QueryResult result = session.executeQuery("SELECT * FROM test_namesake_column_names_view");
            assertThat(result).containsOnly(row("hive"));
        });

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

        try (Connection conn = env.createTrinoConnectionAsUser("alice");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(definerQuery)) {
            assertThat(rs.next()).isFalse();
        }

        try (Connection conn = env.createTrinoConnectionAsUser("alice");
                Statement stmt = conn.createStatement()) {
            assertThatThrownBy(() -> stmt.executeQuery(invokerQuery))
                    .hasMessageContaining("Access Denied");
        }

        env.executeHiveUpdate("DROP VIEW run_as_invoker_view");
        env.executeTrinoUpdate("DROP TABLE hive.default.run_as_invoker");
    }

    // ==================== Overridden tests with legacy-specific behavior ====================
    // These tests have different expected behavior in legacy mode compared to the default mode

    @Test
    void testShowCreateView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_show_view");
        env.executeHiveUpdate("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        // view SQL depends on Hive distribution
        QueryResult result = executeTrinoWithLegacyMode(env, "SHOW CREATE VIEW hive_show_view");
        assertThat(result).hasRowsCount(1);
    }

    @Test
    void testHiveViewInInformationSchema(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP SCHEMA IF EXISTS test_schema CASCADE");

        env.executeHiveUpdate("CREATE SCHEMA test_schema");
        env.executeHiveUpdate("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        env.executeHiveUpdate("CREATE TABLE test_schema.hive_table(a string)");
        env.executeTrinoUpdate("CREATE TABLE hive.test_schema.trino_table(a int)");
        env.executeTrinoUpdate("CREATE VIEW hive.test_schema.trino_test_view AS SELECT * FROM hive.default.nation");

        executeInSession(env, session -> {
            QueryResult tablesResult = session.executeQuery(
                    "SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'");
            assertThat(tablesResult).containsOnly(
                    row("hive", "test_schema", "trino_table", "BASE TABLE"),
                    row("hive", "test_schema", "hive_table", "BASE TABLE"),
                    row("hive", "test_schema", "hive_test_view", "VIEW"),
                    row("hive", "test_schema", "trino_test_view", "VIEW"));

            // In legacy mode, the view definition format is different
            QueryResult viewDefResult = session.executeQuery(
                    "SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'");
            assertThat(viewDefResult).containsOnly(
                    row("SELECT \"nation\".\"n_nationkey\", \"nation\".\"n_name\", \"nation\".\"n_regionkey\", \"nation\".\"n_comment\" FROM \"default\".\"nation\""));

            QueryResult describeResult = session.executeQuery("DESCRIBE test_schema.hive_test_view");
            assertThat(describeResult).contains(row("n_nationkey", "bigint", "", ""));
        });
    }

    @Test
    void testHiveViewWithParametrizedTypes(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS hive_view_parametrized");
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_parametrized");

        env.executeHiveUpdate("CREATE TABLE hive_table_parametrized(a decimal(20,4), b bigint, c varchar(20))");
        env.executeHiveUpdate("CREATE VIEW hive_view_parametrized AS SELECT * FROM hive_table_parametrized");
        env.executeHiveUpdate("INSERT INTO TABLE hive_table_parametrized VALUES (1.2345, 42, 'bar')");

        executeInSession(env, session -> {
            QueryResult dataResult = session.executeQuery("SELECT * FROM hive.default.hive_view_parametrized");
            assertThat(dataResult).containsOnly(
                    row(new BigDecimal("1.2345"), 42L, "bar"));

            // In legacy mode, varchar type preserves the length parameter
            QueryResult columnsResult = session.executeQuery(
                    "SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'");
            assertThat(columnsResult).containsOnly(
                    row("decimal(20,4)"),
                    row("bigint"),
                    row("varchar(20)"));
        });
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testArrayIndexingInView(HiveBasicEnvironment env)
    {
        // Setup data for array indexing test
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_hive_view_array_index_table");
        env.executeHiveUpdate("CREATE TABLE test_hive_view_array_index_table(an_index int, an_array array<string>)");
        env.executeHiveUpdate("INSERT INTO TABLE test_hive_view_array_index_table SELECT 1, array('trino','hive') FROM nation WHERE n_nationkey = 1");

        // Create view with literal array index
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_hive_view_array_index_view");
        env.executeHiveUpdate("CREATE VIEW test_hive_view_array_index_view AS SELECT an_array[1] AS sql_dialect FROM test_hive_view_array_index_table");

        // In legacy mode, Hive uses 0-based array indices, so Trino returns incorrect results
        // The test expects 'hive' but gets 'trino' because index translation doesn't occur
        assertThatThrownBy(() -> {
            executeInSession(env, session -> {
                QueryResult result = session.executeQuery("SELECT * FROM test_hive_view_array_index_view");
                assertThat(result).containsOnly(row("hive"));
            });
        }).hasMessageContaining("trino");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testArrayConstructionInView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS test_array_construction_view");
        env.executeHiveUpdate("CREATE VIEW test_array_construction_view AS SELECT n_nationkey, array(n_nationkey, n_regionkey) AS a FROM nation");

        // In legacy mode, the 'array' function is not registered
        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT a[1], a[2] FROM test_array_construction_view WHERE n_nationkey = 8"))
                .hasMessageContaining("Function 'array' not registered");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testMapConstructionInView(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate(
                "CREATE OR REPLACE VIEW test_map_construction_view AS " +
                        "SELECT" +
                        "  o_orderkey" +
                        ", MAP(o_clerk, o_orderpriority) AS simple_map" +
                        ", MAP(o_clerk, MAP(o_orderpriority, o_shippriority)) AS nested_map" +
                        " FROM orders");

        // In legacy mode, MAP function has unexpected parameters
        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT simple_map['Clerk#000000951'] FROM test_map_construction_view WHERE o_orderkey = 1"))
                .hasMessageContaining("Unexpected parameters (varchar(15), varchar(15)) for function map");

        env.executeHiveUpdate("DROP VIEW test_map_construction_view");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testPmodFunction(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS hive_table_pmod_function");
        env.executeHiveUpdate("CREATE TABLE hive_table_pmod_function(n int, m int)");
        env.executeHiveUpdate("INSERT INTO hive_table_pmod_function (n, m) values (-5, 2)");
        env.executeHiveUpdate("CREATE OR REPLACE VIEW hive_view_pmod_function AS SELECT pmod(n, m) AS col FROM hive_table_pmod_function");

        // In legacy mode, the 'pmod' function is not registered
        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT * FROM hive_view_pmod_function"))
                .hasMessageContaining("Function 'pmod' not registered");

        env.executeHiveUpdate("DROP VIEW hive_view_pmod_function");
        env.executeHiveUpdate("DROP TABLE hive_table_pmod_function");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNestedHiveViews(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_base_view");
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_middle_view");
        env.executeHiveUpdate("DROP VIEW IF EXISTS nested_top_view");

        env.executeHiveUpdate("CREATE VIEW nested_base_view AS SELECT n_nationkey as k, n_name as n, n_regionkey as r, n_comment as c FROM nation");
        env.executeHiveUpdate("CREATE VIEW nested_middle_view AS SELECT n, c FROM nested_base_view WHERE k = 14");
        env.executeHiveUpdate("CREATE VIEW nested_top_view AS SELECT n AS n_renamed FROM nested_middle_view");

        // In legacy mode, nested views have type coercion issues
        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT n_renamed FROM nested_top_view"))
                .hasMessageContaining("View 'hive.default.nested_top_view' is stale or in invalid state: column [n_renamed] of type varchar projected from query view at position 0 cannot be coerced to column [n_renamed] of type varchar(25) stored in view definition");
    }

    @Test
    void testCurrentUser(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP VIEW IF EXISTS current_user_hive_view");
        env.executeHiveUpdate("CREATE VIEW current_user_hive_view as SELECT current_user() AS cu FROM nation LIMIT 1");

        // In legacy mode, parsing fails due to parentheses after current_user
        assertThatThrownBy(() -> executeTrinoWithLegacyMode(env, "SELECT cu FROM current_user_hive_view"))
                .hasMessageContaining("Failed parsing stored view 'hive.default.current_user_hive_view': line 1:20: mismatched input '('");
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

        executeInSession(env, session -> {
            // Query the data
            QueryResult dataResult = session.executeQuery("SELECT * FROM hive_view_textual");
            // In legacy mode, char columns are padded
            String paddedRainy = String.format("%-255s", "rainy");
            assertThat(dataResult).containsOnly(row("a", paddedRainy, "i", "calendar", "Boston Red Sox"));

            // In legacy mode, columns types match Hive's types exactly (with length parameters preserved)
            QueryResult columnsResult = session.executeQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'hive_view_textual'");
            assertThat(columnsResult).containsOnly(
                    row("a_char_1", "char(1)"),
                    row("a_char_255", "char(255)"),
                    row("a_varchar_1", "varchar(1)"),
                    row("a_varchar_65535", "varchar(65535)"),
                    row("a_string", "varchar"));
        });

        env.executeHiveUpdate("DROP VIEW hive_view_textual");
        env.executeHiveUpdate("DROP TABLE hive_table_textual");
    }

    @FunctionalInterface
    private interface SessionConsumer
    {
        void accept(ProductTestEnvironment.TrinoSession session)
                throws SQLException;
    }
}
