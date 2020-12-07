/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public abstract class AbstractTestSapHanaTableStatistics
        extends AbstractTestQueryFramework
{
    protected TestingSapHanaServer sapHanaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sapHanaServer = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                sapHanaServer,
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(ORDERS));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sapHanaServer.close();
    }

    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            // SAP HANA doesn't have automatic statistics
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
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1000, 0, null, 1, 1499)," +
                            "('orderstatus', null, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', null, 5, 0, null, null, null)," +
                            "('clerk', null, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, 0, 0)," +
                            "('comment', null, 14995, 0, null, null, null)," +
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
                            "('orderkey', null, 0, 1, null, null, null)," +
                            "('custkey', null, 0, 1, null, null, null)," +
                            "('orderpriority', null, 0, 1, null, null, null)," +
                            "('comment', null, 0, 1, null, null, null)," +
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
            assertQuery("SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1000, 0.3333333432674408, null, 1, 1499)," +
                            "('orderpriority', null, 5, 0.20000000298023224, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testAverageColumnLength()
    {
        throw new SkipException("SAP HANA connector does not report average column length");
    }

    @Test
    public void testPartitionedTable()
    {
        // TODO test with partitioned table
        throw new SkipException("Not implemented");
    }

    @Test
    public void testView()
    {
        throw new SkipException("SAP HANA doesn't support statistics for views");
    }

    @Test
    public void testMaterializedView()
    {
        throw new SkipException("SAP HANA does not have Materialized Views");
    }

    @Test
    public void testCachedView()
    {
        // TODO test with cached view
        throw new SkipException("Not implemented");
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        String schemaName = getSession().getSchema().orElseThrow();
        sapHanaServer.execute(("" +
                "CREATE TABLE " + format("%s.%s", schemaName, tableName) + " " +
                "AS (SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM " + format("%s.orders", schemaName) + ")"));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, 1, 60000)," +
                            "('case_unquoted_lower', null, 1000, 0, null, 1, 1499)," +
                            "('case_unquoted_mixed', null, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('case_quoted_lower', null, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', null, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            sapHanaServer.execute(format("DROP TABLE %s.%s", schemaName, tableName));
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

    protected abstract void gatherStats(String tableName);
}
