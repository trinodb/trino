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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestSqlServerTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        return SqlServerQueryRunner.createSqlServerQueryRunner(
                sqlServer,
                Map.of(),
                Map.of(
                        "case-insensitive-name-matching", "true",
                        "join-pushdown.enabled", "true"),
                List.of(ORDERS));
    }

    @Override
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
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
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
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, null, null)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', 252376, 5, 0, null, null, null)," +
                            "('clerk', 450000, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, null, null)," +
                            "('comment', 1454727, 14994, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    protected void checkEmptyTableStats(String tableName)
    {
        // TODO: Empty tables should have NDV as 0 and nulls fraction as 1
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('orderkey', null, null, null, null, null, null)," +
                        "('custkey', null, null, null, null, null, null)," +
                        "('orderpriority', null, null, null, null, null, null)," +
                        "('comment', null, null, null, null, null, null)," +
                        "(null, null, null, null, null, null, null)");
    }

    @Override
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

    @Override
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
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0.3333333333333333, null, null, null)," +
                            "('orderpriority', 201914, 5, 0.2, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
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
                            "('orderkey', null, 100, 0, null, null, null)," +
                            "('v3_in_3', 600, 1, 0, null, null, null)," +
                            "('v3_in_42', 600, 1, 0, null, null, null)," +
                            "('single_10v_value', 20, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 1000, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 1200, 50, 0.5, null, null, null)," +
                            "('all_nulls', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        throw new SkipException("Not implemented"); // TODO
    }

    @Override
    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        sqlServer.execute("CREATE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in SQL Server
        }
        finally {
            sqlServer.execute("DROP VIEW " + tableName);
        }
    }

    @Override
    public void testMaterializedView()
    {
        throw new SkipException("see testIndexedView");
    }

    @Test
    public void testIndexedView() // materialized view
    {
        String tableName = "test_stats_indexed_view";
        // indexed views require fixed values for several SET options
        try (Handle handle = Jdbi.open(sqlServer::createConnection)) {
            // indexed views require fixed values for several SET options
            handle.execute("SET NUMERIC_ROUNDABORT OFF");
            handle.execute("SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON");

            handle.execute("" +
                    "CREATE VIEW " + tableName + " " +
                    "WITH SCHEMABINDING " +
                    "AS SELECT orderkey, custkey, orderpriority, comment FROM dbo.orders");
            try {
                handle.execute("CREATE UNIQUE CLUSTERED INDEX idx1 ON " + tableName + " (orderkey, custkey, orderpriority, comment)");
                gatherStats(tableName);
                assertQuery(
                        "SHOW STATS FOR " + tableName,
                        "VALUES " +
                                "('orderkey', null, 15000, 0, null, null, null)," +
                                "('custkey', null, 1000, 0, null, null, null)," +
                                "('orderpriority', 252376, 5, 0, null, null, null)," +
                                "('comment', 1454727, 14994, 0, null, null, null)," +
                                "(null, null, null, null, 15000, null, null)");
            }
            finally {
                handle.execute("DROP VIEW " + tableName);
            }
        }
    }

    @Override
    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        sqlServer.execute("" +
                "SELECT " +
                "  orderkey CASE_UNQUOTED_UPPER, " +
                "  custkey case_unquoted_lower, " +
                "  orderstatus cASe_uNQuoTeD_miXED, " +
                "  totalprice \"CASE_QUOTED_UPPER\", " +
                "  orderdate \"case_quoted_lower\", " +
                "  orderpriority \"CasE_QuoTeD_miXED\" " +
                "INTO " + tableName + " " +
                "FROM orders");
        try {
            gatherStats(
                    tableName,
                    ImmutableList.of(
                            "CASE_UNQUOTED_UPPER",
                            "case_unquoted_lower",
                            "cASe_uNQuoTeD_miXED",
                            "CASE_QUOTED_UPPER",
                            "case_quoted_lower",
                            "CasE_QuoTeD_miXED"));
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, null, null)," +
                            "('case_unquoted_lower', null, 1000, 0, null, null, null)," +
                            "('case_unquoted_mixed', 30000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, null, null)," +
                            "('case_quoted_lower', null, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', 252376, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            sqlServer.execute("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
// TODO infinity and NaNs are not supported by SQLServer
//                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
//                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
//                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
//                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
//                        .put("nans_only double", List.of("nan()", "nan()"))
//                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
// TODO infinity and NaNs are not supported by SQLServer
//                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
//                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
//                            "('mixed_infinities', null, 2, 0, null, null, null)," +
//                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
//                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
//                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    @Test
    public void testShowStatsAfterCreateIndex()
    {
        String tableName = "test_stats_create_index";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));

        String expected = "VALUES " +
                "('orderkey', null, 15000, 0, null, null, null)," +
                "('custkey', null, 1000, 0, null, null, null)," +
                "('orderstatus', 30000, 3, 0, null, null, null)," +
                "('totalprice', null, 14996, 0, null, null, null)," +
                "('orderdate', null, 2401, 0, null, null, null)," +
                "('orderpriority', 252376, 5, 0, null, null, null)," +
                "('clerk', 450000, 1000, 0, null, null, null)," +
                "('shippriority', null, 1, 0, null, null, null)," +
                "('comment', 1454727, 14994, 0, null, null, null)," +
                "(null, null, null, null, 15000, null, null)";

        try {
            gatherStats(tableName);
            assertQuery("SHOW STATS FOR " + tableName, expected);

            // CREATE INDEX statement updates sys.partitions table
            sqlServer.execute(format("CREATE INDEX idx ON %s (orderkey)", tableName));
            sqlServer.execute(format("CREATE UNIQUE INDEX unique_index ON %s (orderkey)", tableName));
            sqlServer.execute(format("CREATE CLUSTERED INDEX clustered_index ON %s (orderkey)", tableName));
            sqlServer.execute(format("CREATE NONCLUSTERED INDEX non_clustered_index ON %s (orderkey)", tableName));

            assertQuery("SHOW STATS FOR " + tableName, expected);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        gatherStats(tableName, columnNames);
    }

    private void gatherStats(String tableName, List<String> columnNames)
    {
        for (Object columnName : columnNames) {
            sqlServer.execute(format("CREATE STATISTICS %1$s ON %2$s (%1$s)", columnName, tableName));
        }
        sqlServer.execute("UPDATE STATISTICS " + tableName);
    }
}
