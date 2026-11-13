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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestOracleTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(new TestingOracleServer());
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(Map.of(
                        "case-insensitive-name-matching", "true",
                        "oracle.synonyms.enabled", "true"))
                .setInitialTables(List.of(ORDERS))
                .build();
    }

    @Test
    @Override
    public void testStatsWithTopNPushdown()
    {
        abort("Oracle connector does not support TopN pushdown");
    }

    @Override
    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed_" + randomNameSuffix();
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
    @Test
    public void testBasic()
    {
        String tableName = "test_stats_orders_" + randomNameSuffix();
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
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls_" + randomNameSuffix();
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
        String tableName = "test_stats_table_with_nulls_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(
                format(
                        """
                        CREATE TABLE %s AS
                        SELECT
                            orderkey,
                            if(orderkey %% 3 = 0, NULL, custkey) custkey,
                            if(orderkey %% 5 = 0, NULL, orderpriority) orderpriority
                        FROM tpch.tiny.orders
                        """,
                        tableName),
                15000);
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0.3333333333333333, null, null, null)," +
                            "('orderpriority', 120000, 5, 0.2, null, null, null)," +
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
        String tableName = "test_stats_table_avg_col_len_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format(
                """
                CREATE TABLE %s AS
                SELECT
                    orderkey,
                    'abc' v3_in_3,
                    CAST('abc' AS varchar(42)) v3_in_42,
                    if(orderkey = 1, '0123456789', NULL) single_10v_value,
                    if(orderkey %% 2 = 0, '0123456789', NULL) half_10v_value,
                    if(orderkey %% 2 = 0, CAST((1000000 - orderkey) * (1000000 - orderkey) AS varchar(20)), NULL) half_distinct_20v_value,
                    CAST(NULL AS varchar(10)) all_nulls,
                    CAST('0123456789' AS varchar) nclob_value,
                    if(orderkey %% 2 = 0, CAST('0123456789' AS varchar), NULL) half_nclob_value,
                    CAST(NULL AS varchar) all_null_nclob
                FROM tpch.tiny.orders
                ORDER BY orderkey LIMIT 100
                """,
                tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 100, 0, null, null, null)," +
                            "('v3_in_3', 400, 1, 0, null, null, null)," +
                            "('v3_in_42', 400, 1, 0, null, null, null)," +
                            "('single_10v_value', 200, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 600, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 700, 50, 0.5, null, null, null)," +
                            "('all_nulls', 0, 0, 1, null, null, null)," +
                            "('nclob_value', 5100, null, 0, null, null, null)," +
                            "('half_nclob_value', 2600, null, 0.5, null, null, null)," +
                            "('all_null_nclob', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testClobStatistics()
    {
        String tableName = "test_stats_clob_" + randomNameSuffix();
        oracleServer.execute(format(
                """
                CREATE TABLE %s (
                    ID NUMBER(10),
                    NON_NULL_CLOB CLOB,
                    HALF_NULL_CLOB CLOB,
                    ALL_NULL_CLOB CLOB
                )
                """,
                tableName));
        try {
            oracleServer.execute(format(
                    """
                    INSERT INTO %s
                    SELECT
                        LEVEL,
                        '0123456789',
                        CASE WHEN MOD(LEVEL, 2) = 0 THEN '0123456789' ELSE NULL END,
                        NULL
                    FROM DUAL
                    CONNECT BY LEVEL <= 100
                    """,
                    tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('id', null, 100, 0, null, null, null)," +
                            "('non_null_clob', 5100, null, 0, null, null, null)," +
                            "('half_null_clob', 2600, null, 0.5, null, null, null)," +
                            "('all_null_clob', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            oracleServer.execute("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        String tableName = "test_stats_orders_part_" + randomNameSuffix();
        oracleServer.execute(format(
                """
                CREATE TABLE %s (
                    ORDERKEY NUMBER(19),
                    CUSTKEY NUMBER(19),
                    ORDERSTATUS VARCHAR2(1 CHAR),
                    TOTALPRICE BINARY_DOUBLE,
                    ORDERDATE DATE,
                    ORDERPRIORITY VARCHAR2(15 CHAR),
                    CLERK VARCHAR2(15 CHAR),
                    SHIPPRIORITY NUMBER(10),
                    "COMMENT" VARCHAR2(79 CHAR)
                )
                PARTITION BY RANGE (ORDERDATE) (
                    PARTITION p_1990_1994 VALUES LESS THAN (DATE '1995-01-01'),
                    PARTITION p_1995_1999 VALUES LESS THAN (MAXVALUE)
                )
                """,
                tableName));
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

            oracleServer.execute(format("INSERT INTO %s SELECT * FROM ORDERS", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, null, null)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            oracleServer.execute("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testView()
    {
        String viewName = "test_stats_view_" + randomNameSuffix();
        try (TestView view = new TestView(oracleServer::execute, viewName, "SELECT orderkey, custkey, orderpriority, \"COMMENT\" FROM orders")) {
            assertQuery(
                    "SHOW STATS FOR " + view.getName(),
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
        }
    }

    @Override
    @Test
    public void testMaterializedView()
    {
        String viewName = "test_stats_mv_" + randomNameSuffix();
        oracleServer.execute(format(
                """
                CREATE MATERIALIZED VIEW %s
                BUILD IMMEDIATE
                AS SELECT ORDERKEY, CUSTKEY, ORDERPRIORITY, "COMMENT" FROM ORDERS
                """,
                viewName));
        try {
            gatherStats(viewName);
            assertQuery(
                    "SHOW STATS FOR " + viewName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            oracleServer.execute("DROP MATERIALIZED VIEW " + viewName);
        }
    }

    @Override
    protected void testCaseColumnNames(String tableName)
    {
        oracleServer.execute(format(
                """
                CREATE TABLE %s AS
                SELECT
                    ORDERKEY AS CASE_UNQUOTED_UPPER,
                    CUSTKEY AS case_unquoted_lower,
                    ORDERSTATUS AS cASe_uNQuoTeD_miXED,
                    TOTALPRICE AS "CASE_QUOTED_UPPER",
                    ORDERDATE AS "case_quoted_lower",
                    ORDERPRIORITY AS "CasE_QuoTeD_miXED"
                FROM ORDERS
                """,
                tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, null, null)," +
                            "('case_unquoted_lower', null, 1000, 0, null, null, null)," +
                            "('case_unquoted_mixed', 30000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, null, null)," +
                            "('case_quoted_lower', null, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', 150000, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            oracleServer.execute("DROP TABLE " + tableName);
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
                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
                        .put("nans_only double", List.of("nan()", "nan()"))
                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)"))
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
                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
                            "('mixed_infinities', null, 2, 0, null, null, null)," +
                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "(null, null, null, null, 4, null, null)");
        }
    }

    @Test
    public void testSynonymStatistics()
    {
        String suffix = randomNameSuffix();
        String privateSyn = "test_priv_syn_" + suffix;
        String publicSyn = "test_pub_syn_" + suffix;
        String chainSyn2 = "test_chain_syn2_" + suffix;
        String chainSyn1 = "test_chain_syn1_" + suffix;
        String unanalyzedColliding = "test_unanalyzed_colliding_" + suffix;
        String collidingView = "test_view_colliding_" + suffix;

        String expectedNationStats = "VALUES " +
                "('nationkey', null, 25, 0, null, null, null)," +
                "('name', 225, 25, 0, null, null, null)," +
                "('regionkey', null, 5, 0, null, null, null)," +
                "('comment', 1900, 25, 0, null, null, null)," +
                "(null, null, null, null, 25, null, null)";
        String expectedEmptyNationStats = "VALUES " +
                "('nationkey', null, null, null, null, null, null)," +
                "('name', null, null, null, null, null, null)," +
                "('regionkey', null, null, null, null, null, null)," +
                "('comment', null, null, null, null, null, null)," +
                "(null, null, null, null, null, null, null)";

        oracleServer.execute(format("CREATE SYNONYM %s FOR %s.NATION", privateSyn, TEST_USER));
        oracleServer.execute(format("CREATE PUBLIC SYNONYM %s FOR %s.NATION", publicSyn, TEST_USER));
        oracleServer.execute(format("CREATE SYNONYM %s FOR %s.NATION", chainSyn2, TEST_USER));
        oracleServer.execute(format("CREATE SYNONYM %s FOR %s.%s", chainSyn1, TEST_USER, chainSyn2));
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", unanalyzedColliding));
        oracleServer.execute(format("CREATE PUBLIC SYNONYM %s FOR %s.NATION", unanalyzedColliding, TEST_USER));
        oracleServer.execute(format("CREATE VIEW %s AS SELECT nationkey, name, regionkey, \"COMMENT\" FROM %s.NATION", collidingView, TEST_USER));
        oracleServer.execute(format("CREATE PUBLIC SYNONYM %s FOR %s.NATION", collidingView, TEST_USER));
        try {
            // Private synonym
            assertQuery("SHOW STATS FOR " + privateSyn, expectedNationStats);

            // Public synonym
            assertQuery("SHOW STATS FOR public." + publicSyn, expectedNationStats);

            // Chained synonyms (chainSyn1 -> chainSyn2 -> NATION)
            assertQuery("SHOW STATS FOR " + chainSyn1, expectedNationStats);

            // Unanalyzed local table must NOT inherit colliding PUBLIC synonym's stats
            assertQuery("SHOW STATS FOR " + unanalyzedColliding, expectedEmptyNationStats);
            // Explicitly querying public schema resolves the PUBLIC synonym's target stats
            assertQuery("SHOW STATS FOR public." + unanalyzedColliding, expectedNationStats);

            // View must NOT inherit colliding PUBLIC synonym's stats
            assertQuery("SHOW STATS FOR " + collidingView, expectedEmptyNationStats);
        }
        finally {
            oracleServer.execute("DROP SYNONYM " + privateSyn);
            oracleServer.execute("DROP PUBLIC SYNONYM " + publicSyn);
            oracleServer.execute("DROP SYNONYM " + chainSyn1);
            oracleServer.execute("DROP SYNONYM " + chainSyn2);
            oracleServer.execute("DROP PUBLIC SYNONYM " + unanalyzedColliding);
            assertUpdate("DROP TABLE IF EXISTS " + unanalyzedColliding);
            oracleServer.execute("DROP PUBLIC SYNONYM " + collidingView);
            oracleServer.execute("DROP VIEW " + collidingView);
        }
    }

    @Override
    protected void gatherStats(String tableName)
    {
        String oracleTableName = tableName.startsWith("\"") && tableName.endsWith("\"")
                ? tableName
                : tableName.toUpperCase(ENGLISH);
        oracleServer.execute(format(
                "BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => '%s', tabname => '%s', estimate_percent => 100, method_opt => 'FOR ALL COLUMNS SIZE 1'); END;",
                TEST_USER.toUpperCase(ENGLISH),
                oracleTableName));
    }
}
