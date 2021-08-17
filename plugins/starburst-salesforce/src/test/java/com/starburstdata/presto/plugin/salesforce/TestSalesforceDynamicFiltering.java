/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestSalesforceDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SalesforceQueryRunner.builder()
                .enableWrites()
                .setTables(ImmutableList.of(ORDERS))
                .build();
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return false;
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFiltering()
    {
        assertDynamicFilters("SELECT * FROM orders__c a JOIN orders__c b ON a.orderkey__c = b.orderkey__c AND b.totalprice__c < 0");
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringWithAggregationGroupingColumn()
    {
        assertDynamicFilters(
                "SELECT * FROM (SELECT orderkey__c, count(*) FROM orders__c GROUP BY 1) a JOIN orders__c b " +
                        "ON a.orderkey__c = b.orderkey__c AND b.totalprice__c < 1000");
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringWithAggregationAggregateColumn()
    {
        // Salesforce connector doesn't support aggregate pushdown
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey__c, count(*) count FROM orders__c GROUP BY 1) a JOIN orders__c b " +
                        "ON a.count = b.orderkey__c AND b.totalprice__c < 1000");
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringWithAggregationGroupingSet()
    {
        // DF pushdown is not supported for grouping column that is not part of every grouping set
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey__c, count(*) FROM orders__c GROUP BY GROUPING SETS ((orderkey__c), ())) a JOIN orders__c b " +
                        "ON a.orderkey__c = b.orderkey__c AND b.totalprice__c < 1000");
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringWithLimit()
    {
        // DF pushdown is not supported for limit queries
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey__c FROM orders__c LIMIT 10000000) a JOIN orders__c b " +
                        "ON a.orderkey__c = b.orderkey__c AND b.totalprice__c < 1000");
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringBroadcastJoin()
    {
        String query = "SELECT * FROM orders__c a JOIN orders__c b " +
                "ON a.orderkey__c = b.orderkey__c AND b.totalprice__c <= 1000";
        long filteredInputPositions = getQueryInputPositions(broadcastJoinWithDynamicFiltering(true), query);
        long unfilteredInputPositions = getQueryInputPositions(broadcastJoinWithDynamicFiltering(false), query);

        assertGreaterThan(unfilteredInputPositions, filteredInputPositions);
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringDomainCompactionThreshold()
    {
        // Rather than creating and dropping the table, we only create it if it does not exist
        // This is to avoid hitting any custom object limits in Salesforce
        // We can't use IF NOT EXISTS because the table name has the __c suffix and the driver does not use these suffixes,
        // so it will try to create the table anyway
        String tableName = "orderkeys_test_df_domain_compaction";
        if (getQueryRunner().execute(format("SHOW TABLES LIKE '%s__c'", tableName)).getRowCount() == 0) {
            assertUpdate("CREATE TABLE " + tableName + " (orderkey) AS VALUES 30000, 60000", 2);
        }
        else {
            // Assert values in table if it already exists
            // The values returned from Salesforce are doubles
            MaterializedResult results = getQueryRunner().execute(format("SELECT orderkey__c FROM %s__c", tableName));
            assertEquals(results.getOnlyColumnAsSet(), ImmutableSet.of(30000.0, 60000.0));
        }

        String query = "SELECT * FROM orders__c a " +
                "JOIN " + tableName + "__c b ON a.orderkey__c = b.orderkey__c";

        long filteredInputPositions = getQueryInputPositions(dynamicFiltering(true), query);
        long smallCompactionInputPositions = getQueryInputPositions(dynamicFilteringWithCompactionThreshold(1), query);
        long unfilteredInputPositions = getQueryInputPositions(dynamicFiltering(false), query);

        assertGreaterThan(unfilteredInputPositions, smallCompactionInputPositions);
        assertGreaterThan(smallCompactionInputPositions, filteredInputPositions);
    }

    @Override
    @Test(timeOut = 120_000)
    public void testDynamicFilteringCaseInsensitiveDomainCompaction()
    {
        // Rather than creating and dropping the table, we only create it if it does not exist
        // This is to avoid hitting any custom object limits in Salesforce
        // We can't use IF NOT EXISTS because the table name has the __c suffix and the driver does not use these suffixes,
        // so it will try to create the table anyway
        String tableName = "test_caseinsensitive";
        if (getQueryRunner().execute(format("SHOW TABLES LIKE '%s__c'", tableName)).getRowCount() == 0) {
            assertUpdate("CREATE TABLE " + tableName + " (id) AS VALUES CAST('0' AS VARCHAR(1)), CAST('a' AS VARCHAR(1)), CAST('B' AS VARCHAR(1))", 3);
        }
        else {
            // Assert values in table if it already exists
            // The values returned from Salesforce are doubles
            MaterializedResult results = getQueryRunner().execute(format("SELECT id__c FROM %s__c", tableName));
            assertEquals(results.getOnlyColumnAsSet(), ImmutableSet.of("0", "a", "B"));
        }

        assertThat(computeActual(
                // Force conversion to a range predicate which would exclude the row corresponding to 'B'
                // if the range predicate were pushed into a case insensitive connector
                dynamicFilteringWithCompactionThreshold(1),
                "SELECT COUNT(*) FROM "
                        + tableName + "__c a JOIN " + tableName + "__c b ON a.id__c = b.id__c")
                .getOnlyValue())
                .isEqualTo(3L);
    }
}
