/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithHive;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestPrestoTableStatisticsWithHive
        extends AbstractTestQueryFramework
{
    private DistributedQueryRunner remotePresto;
    private Session remoteSession;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDir();
        closeAfterClass(() -> deleteRecursively(Path.of(tempDir.getPath()), ALLOW_INSECURE));
        remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithHive(
                tempDir,
                Map.of(),
                ImmutableList.of(ORDERS)));
        remoteSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("tiny")
                .build();
        return createPrestoConnectorQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "hive"),
                        "allow-drop-table", "true"));
    }

    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        executeInRemotePresto(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            // Hive connectors collects stats during write by default
            assertQuery(
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
        executeInRemotePresto(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
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
            assertQuery(
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
        executeInRemotePresto(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            executeInRemotePresto(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
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
        executeInRemotePresto("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    orderkey, " +
                        "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                        "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                        "FROM tpch.tiny.orders");
        assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES 15000");
        try {
            gatherStats(tableName);
            assertQuery(
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
        executeInRemotePresto("" +
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
        executeInRemotePresto("CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['pk']) AS SELECT *, orderpriority AS pk FROM tpch.tiny.orders");
        try {
            gatherStats(tableName);
            // NOTE the null-fraction and NDV stats for partitioned tables do not match the base table because they cannot be aggregated across partitions
            assertQuery(
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
        throw new SkipException("https://starburstdata.atlassian.net/browse/PRESTO-5011"); // TODO https://starburstdata.atlassian.net/browse/PRESTO-5011
    }

    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        executeInRemotePresto("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 990, 0, null, 1, 1499)," +
                            "('orderpriority', 126188, 5, 0, null, null, null)," +
                            "('comment', 727364, 15000, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInRemotePresto("DROP VIEW " + tableName);
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
        executeInRemotePresto("" +
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
                            "('case_unquoted_upper', null, 15000, 0, null, '1', '60000')," +
                            "('case_unquoted_lower', null, 990, 0, null, '1', '1499')," +
                            "('case_unquoted_mixed', 15000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 15000, 0, null, '874.89', '466001.28')," +
                            "('case_quoted_lower', null, 2406, 0, null, '1992-01-01', '1998-08-02')," +
                            "('case_quoted_mixed', 126188.00000000001, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInRemotePresto("DROP TABLE " + tableName);
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

    private void executeInRemotePresto(String sql)
    {
        remotePresto.execute(remoteSession, sql);
    }

    private void gatherStats(String tableName)
    {
        executeInRemotePresto("ANALYZE " + tableName);
    }
}
