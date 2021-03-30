/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithHive;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

public class TestStarburstRemoteStatisticsWithHive
        extends AbstractTestQueryFramework
{
    private DistributedQueryRunner remoteStarburst;
    private Session remoteSession;
    private H2QueryRunner h2QueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        h2QueryRunner = closeAfterClass(new H2QueryRunner());
        Path tempDir = createTempDirectory("HiveCatalogForRemoteStatistics");
        closeAfterClass(() -> deleteRecursively(tempDir, ALLOW_INSECURE));
        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithHive(
                tempDir,
                Map.of(),
                ImmutableList.of(ORDERS, NATION),
                Optional.empty()));
        remoteSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("tiny")
                .build();
        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "hive"),
                        "allow-drop-table", "true"));
    }

    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            // Hive connectors collects stats during write by default
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, '1', '60000')," +
                            "('custkey', null, 990, 0, null, '1', '1499')," +
                            "('orderstatus', 15000, 3, 0, null, null, null)," +
                            "('totalprice', null, 15000, 0, null, '874.89', '466001.28')," +
                            "('orderdate', null, 2406, 0, null, '1992-01-01', '1998-08-02')," +
                            "('orderpriority', 126188, 5, 0, null, null, null)," +
                            "('clerk', 225000, 995, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, '0', '0')," +
                            "('comment', 727364, 15000, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
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
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, '1', '60000')," +
                            "('custkey', null, 990, 0, null, '1', '1499')," +
                            "('orderstatus', 15000, 3, 0, null, null, null)," +
                            "('totalprice', null, 15000, 0, null, '874.89', '466001.28')," +
                            "('orderdate', null, 2406, 0, null, '1992-01-01', '1998-08-02')," +
                            "('orderpriority', 126188, 5, 0, null, null, null)," +
                            "('clerk', 225000, 995, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, '0', '0')," +
                            "('comment', 727364, 15000, 0, null, null, null)," +
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
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 0, 0, 1, null, null, null)," +
                            "('custkey', 0, 0, 1, null, null, null)," +
                            "('orderpriority', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
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
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            executeInRemoteStarburst(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
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
        executeInRemoteStarburst("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    orderkey, " +
                        "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                        "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                        "FROM tpch.tiny.orders");
        assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES 15000");
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 990, 0.3333333333333333, null, 1, 1499)," +
                            "('orderpriority', 100957, 5, 0.2, null, null, null)," +
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
        executeInRemoteStarburst("" +
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
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 100, 0, null, 1, 388)," +
                            "('v3_in_3', 300, 1, 0, null, null, null)," +
                            "('v3_in_42', 300, 1, 0, null, null, null)," +
                            "('single_10v_value', 10, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 500, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 600, 50, 0.5, null, null, null)," +
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
        String tableName = "test_stats_partitioned_table";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        executeInRemoteStarburst("CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['pk']) AS SELECT *, orderpriority AS pk FROM tpch.tiny.orders");
        try {
            gatherStats(tableName);
            // NOTE the null-fraction and NDV stats for partitioned tables do not match the base table because they cannot be aggregated across partitions
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 3065, 0, null, '1', '60000')," +
                            "('custkey', null, 931, 0, null, '1', '1499')," +
                            "('orderstatus', 15000, 3, 0, null, null, null)," +
                            "('totalprice', null, 3020, 0, null, '874.89', '466001.28')," +
                            "('orderdate', null, 1784, 0, null, '1992-01-01', '1998-08-02')," +
                            "('orderpriority', 126188, 1, 0, null, null, null)," +
                            "('clerk', 225000, 953, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, '0', '0')," +
                            "('comment', 727364, 3065, 0, null, null, null)," +
                            "('pk', 126188, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testPartitionedTableWithPredicate()
    {
        throw new SkipException("https://starburstdata.atlassian.net/browse/SEP-5011"); // TODO https://starburstdata.atlassian.net/browse/SEP-5011
    }

    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        executeInRemoteStarburst("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 990, 0, null, 1, 1499)," +
                            "('orderpriority', 126188, 5, 0, null, null, null)," +
                            "('comment', 727364, 15000, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInRemoteStarburst("DROP VIEW " + tableName);
        }
    }

    @Test
    public void testMaterializedView()
    {
        throw new SkipException("Hive < 3.0 doesn't have materialized views");
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        executeInRemoteStarburst("" +
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
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, '1', '60000')," +
                            "('case_unquoted_lower', null, 990, 0, null, '1', '1499')," +
                            "('case_unquoted_mixed', 15000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 15000, 0, null, '874.89', '466001.28')," +
                            "('case_quoted_lower', null, 2406, 0, null, '1992-01-01', '1998-08-02')," +
                            "('case_quoted_mixed', 126188.00000000001, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInRemoteStarburst("DROP TABLE " + tableName);
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
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .build(),
                "null")) {
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
                            "('mixed_infinities', null, 1, 0, null, null, null)," +
                            "('mixed_infinities_and_numbers', null, 1.0, 0.0, null, '-5.0', '7.0')," +
                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
                            "('nans_and_numbers', null, 1.0, 0.0, null, '-5.0', '7.0')," +
                            "('large_doubles', null, 1.0, 0.5, null, '-5.037190915060955E22', '5.037190915060955E22')," +
                            "('short_decimals_big_fraction', null, 1.0, 0.5, null, '-1.234567890123456', '1.234567890123456')," +
                            "('short_decimals_big_integral', null, 1.0, 0.5, null, '-1.234567890123456E14', '1.234567890123456E14')," +
                            "('long_decimals_big_fraction', null, 1.0, 0.5, null, '-1.2345678901234567', '1.2345678901234567')," +
                            "('long_decimals_middle', null, 1.0, 0.5, null, '-1.2345678901234568E15', '1.2345678901234568E15')," +
                            "('long_decimals_big_integral', null, 1.0, 0.5, null, '-1.2345678901234568E36', '1.2345678901234568E36')," +
                            "(null, null, null, null, 4, null, null)");
        }
    }

    @Test
    public void testShowStatsWithWhere()
    {
        assertLocalAndRemoteStatistics(
                "SHOW STATS FOR nation",
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24), " +
                        "('name', 177.0, 25.0, 0.0, null, null, null), " +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 25.0, null, null)");

        assertLocalAndRemoteStatistics(
                "SHOW STATS FOR (SELECT * FROM nation WHERE regionkey = 1)",
                "VALUES " +
                        "('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "('name', 35.4, 5.0, 0.0, null, null, null), " +
                        "('regionkey', null, 1.0, 0.0, null, 1, 1), " +
                        "('comment', 371.4, 5.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 5.0, null, null)");
    }

    private void assertLocalAndRemoteStatistics(String showStatsQuery, String expectedValues)
    {
        assertQuery(showStatsQuery, expectedValues);
        QueryAssertions.assertQuery(remoteStarburst, remoteSession, showStatsQuery, h2QueryRunner, expectedValues, false, false);
    }

    private void executeInRemoteStarburst(String sql)
    {
        remoteStarburst.execute(remoteSession, sql);
    }

    private void gatherStats(String tableName)
    {
        executeInRemoteStarburst("ANALYZE " + tableName);
    }
}
