/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestDynamoDbDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());
        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .setTables(ImmutableList.of(ORDERS))
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .enableWrites()
                .build();
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return false;
    }

    @Override
    @Test(timeOut = 240_000)
    public void testDynamicFiltering()
    {
        // Predicate pushdown is disabled
        assertNoDynamicFiltering("SELECT * FROM orders a JOIN orders b ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 120_000)
    @Override
    public void testDynamicFilteringBroadcastJoin()
    {
        // Predicate pushdown is disabled
        assertNoDynamicFiltering("SELECT * FROM orders a JOIN orders b " +
                "ON a.orderkey = b.orderkey AND b.totalprice <= 1000");
    }

    @Test(timeOut = 120_000)
    @Override
    public void testDynamicFilteringDomainCompactionThreshold()
    {
        String tableName = "orderkeys_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (orderkey) AS VALUES 30000, 60000", 2);

        // Predicate pushdown is disabled
        assertNoDynamicFiltering("SELECT * FROM orders a " +
                "JOIN " + tableName + " b ON a.orderkey = b.orderkey");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(timeOut = 120_000)
    @Override
    public void testDynamicFilteringWithAggregationGroupingColumn()
    {
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey, count(*) FROM orders GROUP BY 1) a JOIN orders b " +
                        "ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 120_000)
    @Override
    public void testDynamicFilteringWithAggregationAggregateColumn()
    {
        // DynamoDB connector doesn't support aggregate
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey, count(*) count FROM orders GROUP BY 1) a JOIN orders b " +
                        "ON a.count = b.orderkey AND b.totalprice < 1000");
    }
}
