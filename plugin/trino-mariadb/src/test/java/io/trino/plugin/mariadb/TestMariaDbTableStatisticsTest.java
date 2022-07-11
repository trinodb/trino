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
package io.trino.plugin.mariadb;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.mariadb.MariaDbQueryRunner.createMariaDbQueryRunner;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMariaDbTableStatisticsTest
        extends BaseJdbcTableStatisticsTest
{
    protected TestingMariaDbServer mariaDbServer;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        // From MariaDB 10.8, JSON_HB, JSON-format histograms, are accepted, so used 10.8.3 version
        mariaDbServer = closeAfterClass(new TestingMariaDbServer("10.8.3", "JSON_HB"));

        return createMariaDbQueryRunner(
                mariaDbServer,
                ImmutableMap.of(),
                ImmutableMap.of("case-insensitive-name-matching", "true"),
                List.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    @Override
    public void setUpTables()
    {
        // Do nothing here
    }

    @Override
    public void testNotAnalyzed()
    {
        String tableName = "test_not_analyzed_" + randomTableSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderstatus', null, null, null, null, null, null)," +
                            "('totalprice', null, null, null, null, null, null)," +
                            "('orderdate', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('clerk', null, null, null, null, null, null)," +
                            "('shippriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    public void testBasic()
    {
        String tableName = "test_stats_orders";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1231, 0, null, 1, 1499)," +
                            "('orderstatus', null, null, null, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('orderdate', null, 2599, 0, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('clerk', null, null, null, null, null, null)," +
                            "('shippriority', null, 1, 0, null, 0, 0)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 0, null, 1, null, null, null)," +
                            "('custkey', 0, null, 1, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    orderkey, " +
                        "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                        "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                        "FROM tpch.tiny.orders",
                15000);
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1228, 0.33329999446868896, null, 1, 1499)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    public void testAverageColumnLength()
    {
        String tableName = "test_stats_table_avg_col_len";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual("" +
                "CREATE TABLE " + tableName + " AS SELECT " +
                "  orderkey, " +
                "  'abc' v3_in_3, " +
                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                "  if(orderkey = 1, 9876543210, NULL) single_10v_value, " +
                "  if(orderkey % 2 = 0, 9876543210, NULL) half_10v_value, " +
                "  if(orderkey % 2 = 0, CAST((1000000 - orderkey) * (1000000 - orderkey) / 1000000 AS double), NULL) half_distinct_20v_value, " +
                "  CAST(NULL AS varchar(10)) all_nulls " +
                "FROM tpch.tiny.orders " +
                "ORDER BY orderkey LIMIT 100");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 100, 0, null, 1, 388)," +
                            "('v3_in_3', null, null, null, null, null, null)," +
                            "('v3_in_42', null, null, null, null, null, null)," +
                            "('single_10v_value', null, 0.9999995231628418, 0.9900000047683716, null, 9876543210, 9876543210)," +
                            "('half_10v_value', null, 1, 0.5, null, 9876543210, 9876543210)," +
                            "('half_distinct_20v_value', null, 50, 0.5, null, 999224.0, 999996.0)," +
                            "('all_nulls', null, null, null, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    public void testPartitionedTable()
    {
        String tableName = "test_stats_orders_part";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        executeInMariaDb(format("CREATE TABLE %s (" +
                "orderkey bigint(20) DEFAULT NULL," +
                "custkey bigint(20) DEFAULT NULL," +
                "orderstatus tinytext DEFAULT NULL," +
                "totalprice double DEFAULT NULL," +
                "orderdate date DEFAULT NULL," +
                "orderpriority tinytext DEFAULT NULL," +
                "clerk tinytext DEFAULT NULL," +
                "shippriority int(11) DEFAULT NULL," +
                "comment tinytext DEFAULT NULL" +
                ") " +
                "PARTITION BY RANGE (YEAR(orderdate)) " +
                "(" +
                "PARTITION p0 VALUES LESS THAN (1990)," +
                "PARTITION p1 VALUES LESS THAN (1994)," +
                "PARTITION p2 VALUES LESS THAN (1995)," +
                "PARTITION p3 VALUES LESS THAN (1999)" +
                ");", tableName));
        executeInMariaDb(format("insert into %s select * from orders;", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1231, 0, null, 1, 1499)," +
                            "('orderstatus', null, null, null, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('orderdate', null, 2599, 0, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('clerk', null, null, null, null, null, null)," +
                            "('shippriority', null, 1, 0, null, 0, 0)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName); // This removes child tables too
        }
    }

    @Override
    public void testView()
    {
        String tableName = "test_stats_view";
        executeInMariaDb("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in MariaDB
        }
        finally {
            executeInMariaDb("DROP VIEW " + tableName);
        }
    }

    @Override
    public void testMaterializedView()
    {
        throw new SkipException(""); // TODO Not support materialized view in MariaDB
    }

    @Override
    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        executeInMariaDb(("" +
                "CREATE TABLE " + tableName + " " +
                "AS SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM orders")
                .replace("\"", "`"));
        try {
            gatherStats(tableName.replace("\"", "`"));
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, 1, 60000)," +
                            "('case_unquoted_lower', null, 1231, 0, null, 1, 1499)," +
                            "('case_unquoted_mixed', null, null, null, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('case_quoted_lower', null, 2599, 0, null, null, null)," +
                            "('case_quoted_mixed', null, null, null, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInMariaDb("DROP TABLE " + tableName.replace("\"", "`"));
        }
    }

    @Override
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        // TODO Infinity and NaNs not supported by MariaDB
//                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
//                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
//                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
//                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
//                        .put("nans_only double", List.of("nan()", "nan()"))
//                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        // DECIMALS up to precision 30 are supported
                        .put("long_decimals_big_fraction decimal(30,29)", List.of("-1.23456789012345678901234567890", "1.23456789012345678901234567890"))
                        .put("long_decimals_middle decimal(30,16)", List.of("-12345678901234.5678901234567890", "12345678901234.5678901234567890"))
                        .put("long_decimals_big_integral decimal(30,1)", List.of("-12345678901234567890123456789.0", "12345678901234567890123456789.0"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            // TODO Infinity and NaNs not supported by MariaDB
//                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
//                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
//                            "('mixed_infinities', null, 2, 0, null, null, null)," +
//                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
//                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
//                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.0, null, cast(-5.037190915060955E22 as double), cast(5.037190915060955E22 as double))," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, -1.234567890123456, 1.234567890123456)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, -123456789012345.6, 123456789012345.6)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, -1.2345678901234567, 1.2345678901234567)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, -12345678901234.5678901234567890, 12345678901234.5678901234567890)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, -12345678901234567890123456789.0, 12345678901234567890123456789.0)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    @Override
    protected void gatherStats(String tableName)
    {
        executeInMariaDb(format(
                "ANALYZE TABLE %s PERSISTENT FOR ALL;", tableName));
    }

    protected void executeInMariaDb(String sql)
    {
        try (Handle handle = Jdbi.open(() -> mariaDbServer.createConnection())) {
            handle.execute("USE tpch");
            handle.execute(sql);
        }
    }

    @Override
    public void testStatsWithJoinPushdown()
    {
        String regionTable = "region_join";
        String nationTable = "nation_join";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM region", regionTable));
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(regionTable);
        gatherStats(nationTable);
        // Simple join with heavily filtered side, should be eligible for pushdown.
        String query = format(
                "SELECT r.regionkey regionkey, r.name r_name, n.name n_name FROM %s r JOIN %s n ON r.regionkey = n.regionkey WHERE n.nationkey = 5",
                regionTable, nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('r_name', null, null, null)," +
                        "('n_name', null, null, null)," +
                        "(null, null, null, 1e0)");
    }

    @Override
    public void testStatsWithPredicatePushdown()
    {
        String nationTable = "nation_predicate";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Predicate on a numeric column. Should be eligible for pushdown.
        String query = format("SELECT * FROM %s WHERE regionkey = 1", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 5e0, 0e0, null)," +
                        "('name', null, null, null)," +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('comment', null, null, null)," +
                        "(null, null, null, 5e0)");
    }

    @Override
    public void testStatsWithPredicatePushdownWithStatsPrecalculationDisabled()
    {
        String nationTable = "nation_predicate_with";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Predicate on a numeric column. Should be eligible for pushdown.
        String query = format("SELECT * FROM %s WHERE regionkey = 1", nationTable);
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED, "false")
                .build();

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(session, query)).isFullyPushedDown();

        assertThat(query(session, "SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 25e0, 0e0, null)," +
                        "('name', null, null, null)," +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('comment', null, null, null)," +
                        "(null, null, null, 25e0)");
    }

    @Override
    public void testStatsWithSimpleJoinPushdown()
    {
        String regionTable = "region_join1";
        String nationTable = "nation_join1";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM region", regionTable));
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(regionTable);
        gatherStats(nationTable);
        // Simple filtering join with no predicates, should be eligible for pushdown.
        String query = format("SELECT n.name n_name FROM %s n JOIN %s r ON n.nationkey = r.regionkey", nationTable, regionTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('n_name', null, null, null)," +
                        "(null, null, null, 5e0)");
    }

    @Override
    public void testStatsWithVarcharPredicatePushdown()
    {
        String nationTable = "nation_varchar";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        String query = format("SELECT * FROM %s WHERE name = 'PERU'", nationTable);
        // mariaDB will not collect the varchar statistics, so not fully pusheddown
        assertThat(query(query)).isNotFullyPushedDown(FilterNode.class);

        String tableName = "varchar_duplicates";
        computeActual(format("CREATE TABLE %s AS SELECT nationkey, chr(codepoint('A') + nationkey / 5) fl FROM tpch.tiny.nation", tableName));
        gatherStats(tableName);

        String query1 = format("SELECT * FROM %s WHERE fl = 'B'", tableName);
        // mariaDB will not collect the varchar statistics, so not fully pusheddown
        assertThat(query(query1)).isNotFullyPushedDown(FilterNode.class);
    }

    @Override
    public void testStatsWithAggregationPushdown()
    {
        String nationTable = "nation_aggregation";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Simple aggregation, should be eligible for pushdown.
        String query = format("SELECT regionkey, max(nationkey) max_nationkey, count(*) c FROM %s GROUP BY regionkey", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('max_nationkey', null, null, null)," +
                        "('c', null, null, null)," +
                        "(null, null, null, 5e0)");
    }

    @Override
    public void testStatsWithDistinctLimitPushdown()
    {
        String nationTable = "nation_dis_limit";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Distinct with limit (DistinctLimitNode), should be eligible for pushdown.
        String query = format("SELECT DISTINCT regionkey FROM %s LIMIT 3", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than count(DISTINCT regionkey), hence need to skip results check
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 3e0, 0e0, null)," +
                        "(null, null, null, 3e0)");
    }

    @Override
    public void testStatsWithLimitPushdown()
    {
        String nationTable = "nation_limit";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Just limit, should be eligible for pushdown.
        String query = format("SELECT regionkey, nationkey FROM %s LIMIT 2", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than table row count, hence need to skip results check
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 2e0, 0e0, null)," +
                        "('nationkey', 2e0, 0e0, null)," +
                        "(null, null, null, 2e0)");
    }

    @Override
    public void testStatsWithTopNPushdown()
    {
        String nationTable = "nation_topn";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // TopN on a numeric column, should be eligible for pushdown.
        String query = format("SELECT regionkey, nationkey FROM %s ORDER BY regionkey LIMIT 2", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than table row count and we intentionally sort on a non-unique regionkey, hence need to skip results check.
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 2e0, 0e0, null)," +
                        "('nationkey', 2e0, 0e0, null)," +
                        "(null, null, null, 2e0)");
    }

    @Override
    public void testStatsWithDistinctPushdown()
    {
        String nationTable = "nation_dis";
        executeInMariaDb(format("CREATE TABLE %s AS SELECT * FROM nation", nationTable));
        gatherStats(nationTable);
        // Just distinct, should be eligible for pushdown.
        String query = format("SELECT DISTINCT regionkey FROM %s", nationTable);

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 5e0, 0e0, null)," +
                        "(null, null, null, 5e0)");
    }
}
