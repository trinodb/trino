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
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableRegionTable;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.utils.QueryExecutors;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.HIVE_VIEWS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Requires({
        ImmutableNationTable.class,
        ImmutableOrdersTable.class,
        ImmutableRegionTable.class,
})
public abstract class AbstractTestHiveViews
        extends HiveProductTest
{
    @Test(groups = HIVE_VIEWS)
    public void testSelectOnView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_test_view");
        onHive().executeQuery("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        assertViewQuery("SELECT * FROM hive_test_view", queryAssert -> queryAssert.hasRowsCount(25));
        assertViewQuery(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM hive_test_view WHERE n_nationkey < 3",
                queryAssert -> queryAssert.containsOnly(
                        row(0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai"),
                        row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
                        row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ")));
    }

    @Test(groups = HIVE_VIEWS)
    public void testArrayIndexingInView()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_hive_view_array_index_table");
        onHive().executeQuery("CREATE TABLE test_hive_view_array_index_table(an_index int, an_array array<string>)");
        onHive().executeQuery("INSERT INTO TABLE test_hive_view_array_index_table SELECT 1, array('presto','hive') FROM nation WHERE n_nationkey = 1");

        // literal array index
        onHive().executeQuery("DROP VIEW IF EXISTS test_hive_view_array_index_view");
        onHive().executeQuery("CREATE VIEW test_hive_view_array_index_view AS SELECT an_array[1] AS sql_dialect FROM test_hive_view_array_index_table");
        assertViewQuery(
                "SELECT * FROM test_hive_view_array_index_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));

        // expression array index
        onHive().executeQuery("DROP VIEW IF EXISTS test_hive_view_expression_array_index_view");
        onHive().executeQuery("CREATE VIEW test_hive_view_expression_array_index_view AS SELECT an_array[an_index] AS sql_dialect FROM test_hive_view_array_index_table");
        assertViewQuery(
                "SELECT * FROM test_hive_view_expression_array_index_view",
                queryAssert -> queryAssert.containsOnly(row("hive")));
    }

    @Test(groups = HIVE_VIEWS)
    public void testCommonTableExpression()
    {
        onHive().executeQuery(
                "CREATE OR REPLACE VIEW test_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8) SELECT * FROM t");

        assertViewQuery("SELECT * FROM test_common_table_expression",
                queryAssert -> queryAssert.containsOnly(row(8, 2)));

        onHive().executeQuery("DROP VIEW test_common_table_expression");
    }

    @Test(groups = HIVE_VIEWS)
    public void testNestedCommonTableExpression()
    {
        onHive().executeQuery(
                "CREATE OR REPLACE VIEW test_nested_common_table_expression AS " +
                        "WITH t AS (SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 8), " +
                        "t2 AS (SELECT n_nationkey * 2 AS nationkey, n_regionkey * 2 AS regionkey FROM t) SELECT * FROM t2");

        assertViewQuery("SELECT * FROM test_nested_common_table_expression",
                queryAssert -> queryAssert.containsOnly(row(16, 4)));

        onHive().executeQuery("DROP VIEW test_nested_common_table_expression");
    }

    @Test(groups = HIVE_VIEWS)
    public void testArrayConstructionInView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS test_array_construction_view");
        onHive().executeQuery("CREATE VIEW test_array_construction_view AS SELECT n_nationkey, array(n_nationkey, n_regionkey) AS a FROM nation");

        assertThat(onHive().executeQuery("SELECT a[0], a[1] FROM test_array_construction_view WHERE n_nationkey = 8"))
                .containsOnly(row(8, 2));
        assertThat(onTrino().executeQuery("SELECT a[1], a[2] FROM test_array_construction_view WHERE n_nationkey = 8"))
                .containsOnly(row(8, 2));
    }

    @Test(groups = HIVE_VIEWS)
    public void testMapConstructionInView()
    {
        onHive().executeQuery(
                "CREATE OR REPLACE VIEW test_map_construction_view AS " +
                        "SELECT" +
                        "  o_orderkey" +
                        ", MAP(o_clerk, o_orderpriority) AS simple_map" +
                        ", MAP(o_clerk, MAP(o_orderpriority, o_shippriority)) AS nested_map" +
                        " FROM orders");

        assertViewQuery("SELECT simple_map['Clerk#000000951'] FROM test_map_construction_view WHERE o_orderkey = 1",
                queryAssert -> queryAssert.containsOnly(row("5-LOW")));
        assertViewQuery("SELECT nested_map['Clerk#000000951']['5-LOW'] FROM test_map_construction_view WHERE o_orderkey = 1",
                queryAssert -> queryAssert.containsOnly(row(0)));

        onHive().executeQuery("DROP VIEW test_map_construction_view");
    }

    @Test(groups = HIVE_VIEWS)
    public void testSelectOnViewFromDifferentSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE");
        onHive().executeQuery("CREATE SCHEMA test_schema");
        onHive().executeQuery(
                "CREATE VIEW test_schema.hive_test_view_1 AS SELECT * FROM " +
                        // no schema is specified in purpose
                        "nation");

        assertViewQuery("SELECT * FROM test_schema.hive_test_view_1", queryAssert -> queryAssert.hasRowsCount(25));
    }

    @Test(groups = HIVE_VIEWS)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    public void testViewWithUnsupportedCoercion()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_unsupported_coercion");
        onHive().executeQuery("CREATE VIEW view_with_unsupported_coercion AS SELECT length(n_comment) FROM nation");

        assertQueryFailure(() -> query("SELECT COUNT(*) FROM view_with_unsupported_coercion"))
                .hasMessageContaining("View 'hive.default.view_with_unsupported_coercion' is stale or in invalid state: a column of type bigint projected from query view at position 0 has no name");
    }

    @Test(groups = HIVE_VIEWS)
    public void testOuterParentheses()
    {
        if (getHiveVersionMajor() <= 1) {
            throw new SkipException("The old Hive doesn't allow outer parentheses in a view definition");
        }

        onHive().executeQuery("CREATE OR REPLACE VIEW view_outer_parentheses AS (SELECT 'parentheses' AS col FROM nation LIMIT 1)");

        assertViewQuery("SELECT * FROM view_outer_parentheses",
                queryAssert -> queryAssert.containsOnly(row("parentheses")));

        onHive().executeQuery("DROP VIEW view_outer_parentheses");
    }

    @Test(groups = HIVE_VIEWS)
    public void testDateFunction()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_date_function");
        onHive().executeQuery("CREATE TABLE hive_table_date_function(s string)");
        onHive().executeQuery("INSERT INTO hive_table_date_function (s) values ('2021-08-21')");
        onHive().executeQuery("CREATE OR REPLACE VIEW hive_view_date_function AS SELECT date(s) AS col FROM hive_table_date_function");

        assertViewQuery("SELECT * FROM hive_view_date_function",
                queryAssert -> queryAssert.containsOnly(row(sqlDate(2021, 8, 21))));

        onHive().executeQuery("DROP VIEW hive_view_date_function");
        onHive().executeQuery("DROP TABLE hive_table_date_function");
    }

    @Test(groups = HIVE_VIEWS)
    public void testPmodFunction()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_pmod_function");
        onHive().executeQuery("CREATE TABLE hive_table_pmod_function(n int, m int)");
        onHive().executeQuery("INSERT INTO hive_table_pmod_function (n, m) values (-5, 2)");
        onHive().executeQuery("CREATE OR REPLACE VIEW hive_view_pmod_function AS SELECT pmod(n, m) AS col FROM hive_table_pmod_function");

        assertViewQuery("SELECT * FROM hive_view_pmod_function",
                queryAssert -> queryAssert.containsOnly(row(1)));

        onHive().executeQuery("DROP VIEW hive_view_pmod_function");
        onHive().executeQuery("DROP TABLE hive_table_pmod_function");
    }

    @Test(groups = HIVE_VIEWS)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    public void testWithUnsupportedFunction()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_repeat_function");
        onHive().executeQuery("CREATE VIEW view_with_repeat_function AS SELECT REPEAT(n_comment,2) FROM nation");

        assertQueryFailure(() -> query("SELECT COUNT(*) FROM view_with_repeat_function"))
                .hasMessageContaining("View 'hive.default.view_with_repeat_function' is stale or in invalid state: a column of type array(varchar(152)) projected from query view at position 0 has no name");
    }

    @Test(groups = HIVE_VIEWS)
    public void testExistingView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_duplicate_view");
        onHive().executeQuery("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertQueryFailure(() -> query("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation"))
                .hasMessageContaining("View already exists");
    }

    @Test(groups = HIVE_VIEWS)
    public void testShowCreateView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_show_view");
        onHive().executeQuery("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        String showCreateViewSql = "SHOW CREATE VIEW %s.default.hive_show_view";
        String expectedResult = "CREATE VIEW %s.default.hive_show_view SECURITY DEFINER AS\n" +
                "SELECT\n" +
                "  \"n_nationkey\"\n" +
                ", \"n_name\"\n" +
                ", \"n_regionkey\"\n" +
                ", \"n_comment\"\n" +
                "FROM\n" +
                "  \"default\".\"nation\"";

        QueryResult actualResult = query(format(showCreateViewSql, "hive"));
        assertThat(actualResult).hasRowsCount(1);
        assertEquals((String) actualResult.row(0).get(0), format(expectedResult, "hive"));

        // Verify the translated view sql for a catalog other than "hive", which is configured to the same metastore
        actualResult = query(format(showCreateViewSql, "hive_with_external_writes"));
        assertThat(actualResult).hasRowsCount(1);
        assertEquals((String) actualResult.row(0).get(0), format(expectedResult, "hive_with_external_writes"));
    }

    /**
     * Test view containing IF, IN, LIKE, BETWEEN, CASE, COALESCE, operators, delimited and non-delimited columns, an inline comment
     */
    @Test(groups = HIVE_VIEWS)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/7535", match = "FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask")
    public void testRichSqlSyntax()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_rich_syntax");
        onHive().executeQuery("CREATE VIEW view_with_rich_syntax AS " +
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
        assertViewQuery("" +
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

    @Test(groups = HIVE_VIEWS)
    public void testIdentifierThatStartWithDigit()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS \"7_table_with_number\"");
        onTrino().executeQuery("CREATE TABLE \"7_table_with_number\" WITH (format='TEXTFILE') AS SELECT VARCHAR 'abc' x");

        onHive().executeQuery("DROP VIEW IF EXISTS view_on_identifiers_starting_with_numbers");
        onHive().executeQuery("CREATE VIEW view_on_identifiers_starting_with_numbers AS SELECT * FROM 7_table_with_number");

        assertViewQuery(
                "SELECT * FROM view_on_identifiers_starting_with_numbers",
                queryAssert -> queryAssert.contains(row("abc")));
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewInInformationSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE");

        onHive().executeQuery("CREATE SCHEMA test_schema");
        onHive().executeQuery("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        onHive().executeQuery("CREATE TABLE test_schema.hive_table(a string)");
        onTrino().executeQuery("CREATE TABLE test_schema.trino_table(a int)");
        onTrino().executeQuery("CREATE VIEW test_schema.trino_test_view AS SELECT * FROM nation");

        boolean hiveWithTableNamesByType = getHiveVersionMajor() >= 3 ||
                (getHiveVersionMajor() == 2 && getHiveVersionMinor() >= 3);
        assertThat(query("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'")).containsOnly(
                row("hive", "test_schema", "trino_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_test_view", hiveWithTableNamesByType ? "VIEW" : "BASE TABLE"),
                row("hive", "test_schema", "trino_test_view", "VIEW"));

        assertThat(query("SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'")).containsOnly(
                row("SELECT \"n_nationkey\", \"n_name\", \"n_regionkey\", \"n_comment\"\nFROM \"default\".\"nation\""));

        assertThat(query("DESCRIBE test_schema.hive_test_view"))
                .contains(row("n_nationkey", "bigint", "", ""));
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewWithParametrizedTypes()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_view_parametrized");
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_parametrized");

        onHive().executeQuery("CREATE TABLE hive_table_parametrized(a decimal(20,4), b bigint, c varchar(20))");
        onHive().executeQuery("CREATE VIEW hive_view_parametrized AS SELECT * FROM hive_table_parametrized");
        onHive().executeQuery("INSERT INTO TABLE hive_table_parametrized VALUES (1.2345, 42, 'bar')");

        assertViewQuery(
                "SELECT * FROM hive_view_parametrized",
                queryAssert -> queryAssert.containsOnly(row(new BigDecimal("1.2345"), 42, "bar")));

        assertThat(query("SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'")).containsOnly(
                row("decimal(20,4)"),
                row("bigint"),
                row("varchar"));
    }

    @Test(groups = HIVE_VIEWS)
    public void testNestedHiveViews()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS nested_base_view");
        onHive().executeQuery("DROP VIEW IF EXISTS nested_middle_view");
        onHive().executeQuery("DROP VIEW IF EXISTS nested_top_view");

        onHive().executeQuery("CREATE VIEW nested_base_view AS SELECT n_nationkey as k, n_name as n, n_regionkey as r, n_comment as c FROM nation");
        onHive().executeQuery("CREATE VIEW nested_middle_view AS SELECT n, c FROM nested_base_view WHERE k = 14");
        onHive().executeQuery("CREATE VIEW nested_top_view AS SELECT n AS n_renamed FROM nested_middle_view");

        assertViewQuery(
                "SELECT n_renamed FROM nested_top_view",
                queryAssert -> queryAssert.containsOnly(row("KENYA")));
    }

    @Test(groups = HIVE_VIEWS)
    public void testSelectFromHiveViewWithoutDefaultCatalogAndSchema()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS no_catalog_schema_view");
        onHive().executeQuery("CREATE VIEW no_catalog_schema_view AS SELECT * FROM nation WHERE n_nationkey = 1");

        QueryExecutor executor = connectToPresto("presto_no_default_catalog");
        assertQueryFailure(() -> executor.executeQuery("SELECT count(*) FROM no_catalog_schema_view"))
                .hasMessageMatching(".*Schema must be specified when session schema is not set.*");
        assertThat(executor.executeQuery("SELECT count(*) FROM hive.default.no_catalog_schema_view"))
                .containsOnly(row(1L));
    }

    @Test(groups = HIVE_VIEWS)
    public void testTimestampHiveView()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS timestamp_hive_table");
        onHive().executeQuery("CREATE TABLE timestamp_hive_table (ts timestamp)");
        onHive().executeQuery("INSERT INTO timestamp_hive_table (ts) values ('1990-01-02 12:13:14.123456789')");
        onHive().executeQuery("DROP VIEW IF EXISTS timestamp_hive_view");
        onHive().executeQuery("CREATE VIEW timestamp_hive_view AS SELECT * FROM timestamp_hive_table");

        // timestamp_precision not set
        unsetSessionProperty("hive.timestamp_precision");
        unsetSessionProperty("hive_timestamp_nanos.timestamp_precision");

        assertThat(query("SELECT CAST(ts AS varchar) FROM timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123"));
        assertThatThrownBy(
                // TODO(https://github.com/prestosql/presto/issues/6295) it is not possible to query Hive view with timestamps if hive.timestamp-precision=NANOSECONDS
                () -> assertThat(query("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123456789"))
        ).hasMessageContaining("timestamp(9) projected from query view at position 0 cannot be coerced to column [ts] of type timestamp(3) stored in view definition");

        setSessionProperty("hive.timestamp_precision", "'MILLISECONDS'");
        setSessionProperty("hive_timestamp_nanos.timestamp_precision", "'MILLISECONDS'");

        assertThat(query("SELECT CAST(ts AS varchar) FROM timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123"));
        assertThatThrownBy(
                // TODO(https://github.com/prestosql/presto/issues/6295) it is not possible to query Hive view with timestamps if hive.timestamp-precision=NANOSECONDS
                () -> assertThat(query("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123"))
        ).hasMessageContaining("timestamp(9) projected from query view at position 0 cannot be coerced to column [ts] of type timestamp(3) stored in view definition");

        setSessionProperty("hive.timestamp_precision", "'NANOSECONDS'");
        setSessionProperty("hive_timestamp_nanos.timestamp_precision", "'NANOSECONDS'");

        // TODO(https://github.com/prestosql/presto/issues/6295) timestamp_precision has no effect on Hive views
        // should be: assertThat(query("SELECT CAST(ts AS varchar) FROM timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123456789"))
        assertThat(query("SELECT CAST(ts AS varchar) FROM timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123"));
        assertThatThrownBy(
                // TODO(https://github.com/prestosql/presto/issues/6295) it is not possible to query Hive view with timestamps if hive.timestamp-precision=NANOSECONDS
                () -> assertThat(query("SELECT CAST(ts AS varchar) FROM hive_timestamp_nanos.default.timestamp_hive_view")).containsOnly(row("1990-01-02 12:13:14.123456789"))
        ).hasMessageContaining("timestamp(9) projected from query view at position 0 cannot be coerced to column [ts] of type timestamp(3) stored in view definition");
    }

    @Test(groups = HIVE_VIEWS)
    public void testCurrentUser()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS current_user_hive_view");
        onHive().executeQuery("CREATE VIEW current_user_hive_view as SELECT current_user() AS cu FROM nation LIMIT 1");

        String testQuery = "SELECT cu FROM current_user_hive_view";
        assertThat(query(testQuery)).containsOnly(row("hive"));
        assertThat(connectToPresto("alice@presto").executeQuery(testQuery)).containsOnly(row("alice"));
    }

    @Test(groups = HIVE_VIEWS)
    // Test is currently flaky on CDH5 environment
    @Flaky(issue = "https://github.com/trinodb/trino/issues/9074", match = "Error while processing statement: FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask")
    public void testNestedGroupBy()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS test_nested_group_by_view");
        onHive().executeQuery("CREATE VIEW test_nested_group_by_view AS SELECT n_regionkey, count(1) count FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey ) t GROUP BY n_regionkey");

        assertViewQuery(
                "SELECT * FROM test_nested_group_by_view",
                queryAssert -> queryAssert.containsOnly(
                        row(0, 1),
                        row(1, 1),
                        row(2, 1),
                        row(3, 1),
                        row(4, 1)));
    }

    @Test(groups = HIVE_VIEWS)
    public void testUnionAllViews()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS union_helper");
        onHive().executeQuery("CREATE TABLE union_helper (\n"
                + "r_regionkey BIGINT,\n"
                + "r_name VARCHAR(25),\n"
                + "r_comment VARCHAR(152)\n"
                + ")");
        onHive().executeQuery("INSERT INTO union_helper\n"
                + "SELECT r_regionkey % 3, r_name, r_comment FROM region");

        onHive().executeQuery("DROP VIEW IF EXISTS union_all_view");
        onHive().executeQuery("CREATE VIEW union_all_view AS\n"
                + "SELECT r_regionkey FROM region\n"
                + "UNION ALL\n"
                + "SELECT r_regionkey FROM union_helper\n");

        assertThat(query("SELECT r_regionkey FROM union_all_view"))
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

    @Test(groups = HIVE_VIEWS)
    public void testUnionDistinctViews()
    {
        if (getHiveVersionMajor() < 1 || (getHiveVersionMajor() == 1 && getHiveVersionMinor() < 2)) {
            throw new SkipException("UNION DISTINCT and plain UNION are not supported before Hive 1.2.0");
        }

        onHive().executeQuery("DROP TABLE IF EXISTS union_helper");
        onHive().executeQuery("CREATE TABLE union_helper (\n"
                + "r_regionkey BIGINT,\n"
                + "r_name VARCHAR(25),\n"
                + "r_comment VARCHAR(152)\n"
                + ")");
        onHive().executeQuery("INSERT INTO union_helper\n"
                + "SELECT r_regionkey % 3, r_name, r_comment FROM region");

        for (String operator : List.of("UNION", "UNION DISTINCT")) {
            String name = format("%s_view", operator.replace(" ", "_"));
            onHive().executeQuery(format("DROP VIEW IF EXISTS %s", name));
            // Add mod to one side to add duplicate and non-overlapping values
            onHive().executeQuery(format(
                    "CREATE VIEW %s AS\n"
                            + "SELECT r_regionkey FROM region\n"
                            + "%s\n"
                            + "SELECT r_regionkey FROM union_helper\n",
                    name,
                    operator));

            assertViewQuery(
                    format("SELECT r_regionkey FROM %s", name),
                    assertion -> assertion.as("View with %s", operator)
                            .containsOnly(row(0), row(1), row(2), row(3), row(4)));
        }
    }

    protected static void assertViewQuery(String query, Consumer<QueryAssert> assertion)
    {
        // Ensure Hive and Presto view compatibility by comparing the results
        assertion.accept(assertThat(onHive().executeQuery(query)));
        assertion.accept(assertThat(onTrino().executeQuery(query)));
    }

    protected static Date sqlDate(int year, int month, int day)
    {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    protected QueryExecutor connectToPresto(String catalog)
    {
        return QueryExecutors.connectToPresto(catalog);
    }

    protected void setSessionProperty(String key, String value)
    {
        // We need to setup sessions for both "presto" and "default" executors in tempto
        onTrino().executeQuery(format("SET SESSION %s = %s", key, value));
        query(format("SET SESSION %s = %s", key, value));
    }

    protected void unsetSessionProperty(String key)
    {
        // We need to setup sessions for both "presto" and "default" executors in tempto
        onTrino().executeQuery("RESET SESSION " + key);
        query("RESET SESSION " + key);
    }
}
