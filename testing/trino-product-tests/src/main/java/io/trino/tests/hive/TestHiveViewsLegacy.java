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

import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.function.Consumer;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.TestGroups.HIVE_VIEWS;
import static io.trino.tests.utils.QueryExecutors.connectToPresto;
import static io.trino.tests.utils.QueryExecutors.onHive;
import static io.trino.tests.utils.QueryExecutors.onPresto;

@Requires({
        ImmutableNationTable.class,
        ImmutableOrdersTable.class,
})
public class TestHiveViewsLegacy
        extends HiveProductTest
{
    @BeforeTestWithContext
    public void setup()
    {
        // We need to setup sessions for both "presto" and "default" executors in tempto
        onPresto().executeQuery("SET SESSION hive.legacy_hive_view_translation = true");
        query("SET SESSION hive.legacy_hive_view_translation = true");
    }

    @Test(groups = HIVE_VIEWS)
    public void testSelectOnView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_test_view");

        onHive().executeQuery("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        assertThat(query("SELECT * FROM hive_test_view")).hasRowsCount(25);
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

        assertThat(query("SELECT * FROM test_schema.hive_test_view_1")).hasRowsCount(25);
    }

    @Test(groups = HIVE_VIEWS)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    public void testViewWithUnsupportedCoercion()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_unsupported_coercion");
        onHive().executeQuery("CREATE VIEW view_with_unsupported_coercion AS SELECT length(n_comment) FROM nation");

        assertThat(() -> query("SELECT COUNT(*) FROM view_with_unsupported_coercion"))
                .failsWithMessage("View 'hive.default.view_with_unsupported_coercion' is stale or in invalid state: a column of type bigint projected from query view at position 0 has no name");
    }

    @Test(groups = HIVE_VIEWS)
    // TODO (https://github.com/trinodb/trino/issues/5911) the test does not test view coercion
    public void testWithUnsupportedFunction()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_repeat_function");
        onHive().executeQuery("CREATE VIEW view_with_repeat_function AS SELECT REPEAT(n_comment,2) FROM nation");

        assertThat(() -> query("SELECT COUNT(*) FROM view_with_repeat_function"))
                .failsWithMessage("View 'hive.default.view_with_repeat_function' is stale or in invalid state: a column of type array(varchar(152)) projected from query view at position 0 has no name");
    }

    @Test(groups = HIVE_VIEWS)
    public void testExistingView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_duplicate_view");

        onHive().executeQuery("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertThat(() -> query("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation"))
                .failsWithMessage("View already exists");
    }

    @Test(groups = HIVE_VIEWS)
    public void testShowCreateView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_show_view");

        onHive().executeQuery("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        // view SQL depends on Hive distribution
        assertThat(query("SHOW CREATE VIEW hive_show_view")).hasRowsCount(1);
    }

    @Test(groups = HIVE_VIEWS)
    public void testUnsupportedLateralViews()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view");
        onHive().executeQuery("DROP TABLE IF EXISTS pageAds");

        onHive().executeQuery("CREATE TABLE pageAds(pageid string, adid_list array<int>)");
        onHive().executeQuery("CREATE VIEW hive_lateral_view as SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid");

        assertThat(() -> query("SELECT COUNT(*) FROM hive_lateral_view"))
                .failsWithMessage("Failed parsing stored view 'hive.default.hive_lateral_view': line 1:78: mismatched input 'VIEW'");
    }

    @Test(groups = HIVE_VIEWS)
    public void testIdentifierThatStartWithDigit()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_on_identifiers_starting_with_numbers");
        onHive().executeQuery("DROP TABLE IF EXISTS 7_table_with_number");

        onHive().executeQuery("CREATE TABLE 7_table_with_number(num string)");
        onHive().executeQuery("CREATE VIEW view_on_identifiers_starting_with_numbers AS SELECT * FROM 7_table_with_number");

        assertThat(query("SELECT COUNT(*) FROM view_on_identifiers_starting_with_numbers")).contains(row(0));
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewInInformationSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE;");

        onHive().executeQuery("CREATE SCHEMA test_schema;");
        onHive().executeQuery("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        onHive().executeQuery("CREATE TABLE test_schema.hive_table(a string)");
        onPresto().executeQuery("CREATE TABLE test_schema.trino_table(a int)");
        onPresto().executeQuery("CREATE VIEW test_schema.trino_test_view AS SELECT * FROM nation");

        boolean hiveWithTableNamesByType = getHiveVersionMajor() >= 3 ||
                (getHiveVersionMajor() == 2 && getHiveVersionMinor() >= 3);
        assertThat(query("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'")).containsOnly(
                row("hive", "test_schema", "trino_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_test_view", hiveWithTableNamesByType ? "VIEW" : "BASE TABLE"),
                row("hive", "test_schema", "trino_test_view", "VIEW"));

        assertThat(query("SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'")).containsOnly(
                row("SELECT \"nation\".\"n_nationkey\", \"nation\".\"n_name\", \"nation\".\"n_regionkey\", \"nation\".\"n_comment\" FROM \"default\".\"nation\""));

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

        assertThat(query("SELECT * FROM hive.default.hive_view_parametrized")).containsOnly(
                row(new BigDecimal("1.2345"), 42, "bar"));

        assertThat(query("SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'")).containsOnly(
                row("decimal(20,4)"),
                row("bigint"),
                row("varchar(20)"));
    }

    /**
     * Test view containing IF, IN, LIKE, BETWEEN, CASE, COALESCE, operators, delimited and non-delimited columns, an inline comment
     */
    @Test(groups = HIVE_VIEWS)
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

    @Test
    public void testSelectFromHiveViewWithoutDefaultCatalogAndSchema()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS no_catalog_schema_view");
        onHive().executeQuery("CREATE VIEW no_catalog_schema_view AS SELECT * FROM nation WHERE n_nationkey = 1");

        QueryExecutor executor = connectToPresto("presto_no_default_catalog");
        executor.executeQuery("SET SESSION hive.legacy_hive_view_translation = true");

        assertThat(() -> executor.executeQuery("SELECT count(*) FROM no_catalog_schema_view"))
                .failsWithMessageMatching(".*Schema must be specified when session schema is not set.*");
        assertThat(executor.executeQuery("SELECT count(*) FROM hive.default.no_catalog_schema_view"))
                .containsExactly(row(1L));
    }

    private static void assertViewQuery(String query, Consumer<QueryAssert> assertion)
    {
        // Ensure Hive and Presto view compatibility by comparing the results
        assertion.accept(assertThat(onHive().executeQuery(query)));
        assertion.accept(assertThat(onPresto().executeQuery(query)));
    }

    private static Date sqlDate(int year, int month, int day)
    {
        return Date.valueOf(LocalDate.of(year, month, day));
    }
}
