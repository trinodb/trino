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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.function.Function;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestOracleTableStatistics
        extends AbstractTestQueryFramework
{
    public TestOracleTableStatistics()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .put("case-insensitive-name-matching", "true")
                        .build(),
                Function.identity(),
                ImmutableList.of(TpchTable.ORDERS)));
    }

    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
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

    @Test
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
                            "('orderkey', 75000, 15000, 0, null, null, null)," +
                            "('custkey', 60000, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', 120000, 14996, 0, null, null, null)," +
                            "('orderdate', 120000, 2401, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', 30000, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testEmptyTable()
    {
        String tableName = "test_stats_table_empty";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 0, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
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
                            "('orderkey', 0, 0, 1, null, null, null)," +
                            "('custkey', 0, 0, 1, null, null, null)," +
                            "('orderpriority', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
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
                            "('orderkey', 75000, 15000, 0, null, null, null)," +
                            "('custkey', 45000, 1000, 0.3333333333333333, null, null, null)," +
                            "('orderpriority', 120000, 5, 0.2, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testAverageColumnLength()
    {
        String tableName = "test_stats_table_avg_col_len";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual("" +
                "CREATE TABLE " + tableName + " AS SELECT " +
                "  orderkey, " +
                "  'abc' v3_in_3, " +
                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                "  if(orderkey = 1, '0123456789', NULL) single_10v_value, " +
                "  if(orderkey % 2 = 0, '0123456789', NULL) half_10v_value, " +
                "  if(orderkey % 2 = 0, CAST((1000000 - orderkey) * (1000000 - orderkey) AS varchar(20)), NULL) half_distinct_20v_value, " + // 12 chars each
                "  CAST(NULL AS varchar(10)) all_nulls " +
                "FROM tpch.tiny.orders " +
                "ORDER BY orderkey LIMIT 100");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 400, 100, 0, null, null, null)," +
                            "('v3_in_3', 400, 1, 0, null, null, null)," +
                            "('v3_in_42', 400, 1, 0, null, null, null)," +
                            "('single_10v_value', 200, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 600, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 700, 50, 0.5, null, null, null)," +
                            "('all_nulls', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testPartitionedTable()
    {
        String tableName = "test_stats_orders_part";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        executeInOracle("CREATE TABLE " + tableName + " PARTITION BY HASH(ORDERKEY) PARTITIONS 4 AS SELECT * FROM ORDERS");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 75000, 15000, 0, null, null, null)," +
                            "('custkey', 60000, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', 120000, 14996, 0, null, null, null)," +
                            "('orderdate', 120000, 2401, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', 30000, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        executeInOracle("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, \"COMMENT\" FROM orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to DBMS_STATS.GATHER_TABLE_STATS on a VIEW in Oracle
        }
        finally {
            executeInOracle("DROP VIEW " + tableName);
        }
    }

    @Test
    public void testMaterializedView()
    {
        String tableName = "test_stats_materialized_view";
        executeInOracle("" +
                "CREATE MATERIALIZED VIEW " + tableName + " " +
                "BUILD IMMEDIATE REFRESH ON DEMAND " +
                "AS SELECT orderkey, custkey, orderpriority, \"COMMENT\" FROM orders");
        try {
            // MATERIALIZED VIEW has stats gathered implicitly
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 75000, 15000, 0, null, null, null)," +
                            "('custkey', 60000, 1000, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInOracle("DROP MATERIALIZED VIEW " + tableName);
        }
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        executeInOracle("" +
                "CREATE TABLE " + tableName + " " +
                "AS SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM orders");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', 75000, 15000, 0, null, null, null)," +
                            "('case_unquoted_lower', 60000, 1000, 0, null, null, null)," +
                            "('case_unquoted_mixed', 30000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', 120000, 14996, 0, null, null, null)," +
                            "('case_quoted_lower', 120000, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', 150000, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInOracle("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] testCaseColumnNamesDataProvider()
    {
        return new Object[][] {
                {"TEST_STATS_MIXED_UNQUOTED_UPPER"},
                {"test_stats_mixed_unquoted_lower"},
                {"test_stats_mixed_uNQuoTeD_miXED"},
                {"\"TEST_STATS_MIXED_QUOTED_UPPER\""},
                {"\"test_stats_mixed_quoted_lower\""},
                {"\"test_stats_mixed_QuoTeD_miXED\""},
        };
    }

    @Test
    public void testAnalyzedWithProcedure()
    {
        String tableName = "test_stats_analyzed_via_presto";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            assertUpdate(format("CALL system.analyze('%s', '%s')", TestingOracleServer.USER, tableName));
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 75000, 15000, 0, null, null, null)," +
                            "('custkey', 60000, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', 120000, 14996, 0, null, null, null)," +
                            "('orderdate', 120000, 2401, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', 30000, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private static void gatherStats(String tableName)
    {
        executeInOracle(connection -> {
            try (CallableStatement statement = connection.prepareCall("{CALL DBMS_STATS.GATHER_TABLE_STATS(?, ?)}")) {
                statement.setString(1, TestingOracleServer.USER);
                statement.setString(2, tableName);
                statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
