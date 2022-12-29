/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class AbstractDynamicFilteringTest
        extends BaseDynamicFilteringTest
{
    protected abstract boolean isJoinPushdownEnabledByDefault();

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                // test assertions assume join pushdown is not happening so we disable it here
                // TODO (https://starburstdata.atlassian.net/browse/SEP-5900) Add explicit test verifying dynamic filtering works with join pushdow
                .setCatalogSessionProperty(super.getSession().getCatalog().get(), JOIN_PUSHDOWN_ENABLED, "false")
                .build();
    }

    @Test
    public void verifyIsJoinPushdownEnabledByDefault()
    {
        // sanity check verifying if value provided by `isJoinPushdownEnabledByDefault` matches actual state.
        if (!isJoinPushdownEnabledByDefault()) {
            assertThat(query("SHOW SESSION LIKE '%.join_pushdown_strategy'")).returnsEmptyResult();
        }
        else {
            assertThat(query("SHOW SESSION LIKE '%.join_pushdown_strategy'"))
                    .skippingTypesCheck()
                    .matches(result -> result.getRowCount() == 1)
                    .matches(result -> {
                        String value = (String) result.getMaterializedRows().get(0).getField(1);
                        return value.equals("AUTOMATIC") || value.equals("EAGER");
                    });
        }
    }

    @Test(timeOut = 240_000)
    public void testDynamicFiltering()
    {
        assertDynamicFilters("SELECT * FROM orders a JOIN orders b ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringWithAggregationGroupingColumn()
    {
        assertDynamicFilters(
                "SELECT * FROM (SELECT orderkey, count(*) FROM orders GROUP BY 1) a JOIN orders b " +
                        "ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringWithAggregationAggregateColumn()
    {
        assertDynamicFilters(
                "SELECT * FROM (SELECT orderkey, count(*) count FROM orders GROUP BY 1) a JOIN orders b " +
                        "ON a.count = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringWithAggregationGroupingSet()
    {
        // DF pushdown is not supported for grouping column that is not part of every grouping set
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey, count(*) FROM orders GROUP BY GROUPING SETS ((orderkey), ())) a JOIN orders b " +
                        "ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringWithLimit()
    {
        // DF pushdown is not supported for limit queries
        assertNoDynamicFiltering(
                "SELECT * FROM (SELECT orderkey FROM orders LIMIT 10000000) a JOIN orders b " +
                        "ON a.orderkey = b.orderkey AND b.totalprice < 1000");
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringBroadcastJoin()
    {
        String query = "SELECT * FROM orders a JOIN orders b " +
                "ON a.orderkey = b.orderkey AND b.totalprice <= 1000";
        long filteredInputPositions = getQueryInputPositions(broadcastJoinWithDynamicFiltering(true), query);
        long unfilteredInputPositions = getQueryInputPositions(broadcastJoinWithDynamicFiltering(false), query);

        assertThat(unfilteredInputPositions)
                .as("unfiltered input positions")
                .isGreaterThan(filteredInputPositions);
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringDomainCompactionThreshold()
    {
        String tableName = "orderkeys_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (orderkey) AS VALUES 30000, 60000", 2);
        String query = "SELECT * FROM orders a " +
                "JOIN " + tableName + " b ON a.orderkey = b.orderkey";

        long filteredInputPositions = getQueryInputPositions(dynamicFiltering(true), query);
        long smallCompactionInputPositions = getQueryInputPositions(dynamicFilteringWithCompactionThreshold(1), query);
        long unfilteredInputPositions = getQueryInputPositions(dynamicFiltering(false), query);

        assertThat(unfilteredInputPositions)
                .as("unfiltered input positions")
                .isGreaterThan(smallCompactionInputPositions);

        assertThat(smallCompactionInputPositions)
                .as("small compaction input positions")
                .isGreaterThan(filteredInputPositions);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(timeOut = 240_000)
    public void testDynamicFilteringCaseInsensitiveDomainCompaction()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_caseinsensitive",
                "(id varchar(1))",
                ImmutableList.of("'0'", "'a'", "'B'"))) {
            assertThat(computeActual(
                    // Force conversion to a range predicate which would exclude the row corresponding to 'B'
                    // if the range predicate were pushed into a case insensitive connector
                    dynamicFilteringWithCompactionThreshold(1),
                    "SELECT COUNT(*) FROM " + table.getName() + " a JOIN " + table.getName() + " b ON a.id = b.id")
                    .getOnlyValue())
                    .isEqualTo(3L);
        }
    }

    protected Session dynamicFilteringWithCompactionThreshold(int compactionThreshold)
    {
        return Session.builder(dynamicFiltering(true))
                .setCatalogSessionProperty(getSession().getCatalog().get(), DOMAIN_COMPACTION_THRESHOLD, Integer.toString(compactionThreshold))
                .build();
    }

    protected Session broadcastJoinWithDynamicFiltering(boolean dynamicFiltering)
    {
        return Session.builder(dynamicFiltering(dynamicFiltering))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    protected void assertDynamicFilters(String query)
    {
        assertDynamicFilters(getSession(), query);
    }

    protected void assertDynamicFilters(Session session, String query)
    {
        long filteredInputPositions = getQueryInputPositions(dynamicFiltering(session, true), query);
        long unfilteredInputPositions = getQueryInputPositions(dynamicFiltering(session, false), query);

        assertThat(filteredInputPositions)
                .as("filtered input positions")
                .isLessThan(unfilteredInputPositions);
    }

    protected void assertNoDynamicFiltering(String query)
    {
        long filteredInputPositions = getQueryInputPositions(dynamicFiltering(true), query);
        long unfilteredInputPositions = getQueryInputPositions(dynamicFiltering(false), query);
        assertEquals(filteredInputPositions, unfilteredInputPositions);
    }

    protected long getQueryInputPositions(Session session, String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        QueryId queryId = runner.executeWithQueryId(session, sql).getQueryId();
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getPhysicalInputPositions();
    }
}
