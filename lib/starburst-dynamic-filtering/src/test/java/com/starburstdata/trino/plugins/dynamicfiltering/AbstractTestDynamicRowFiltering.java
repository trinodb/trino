/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestDynamicRowFiltering
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(CUSTOMER, NATION);

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return new Object[][] {{BROADCAST}, {PARTITIONED}};
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithSelectiveRowFiltering(JoinDistributionType joinDistributionType)
    {
        assertRowFiltering(
                "SELECT * FROM customer c, nation n WHERE c.nationkey = n.nationkey and n.name = 'ALGERIA'",
                joinDistributionType);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithNonSelectiveRowFiltering(JoinDistributionType joinDistributionType)
    {
        assertNoRowFiltering(
                "SELECT * FROM  customer c, nation n WHERE c.nationkey = n.nationkey",
                joinDistributionType);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testRowFilteringWithStrings(JoinDistributionType joinDistributionType)
    {
        // name is high cardinality, VariableWidthBlock is used
        assertRowFiltering(
                "SELECT * FROM customer c1, customer c2 WHERE c1.name = c2.name AND c2.acctbal > 9000",
                joinDistributionType);

        // mktsegment is low cardinality, DictionaryBlock is used
        assertRowFiltering(
                "SELECT * FROM customer c1, customer c2 WHERE c1.mktsegment = c2.mktsegment AND c2.custkey = 1",
                joinDistributionType);

        assertNoRowFiltering(
                "SELECT * FROM customer c1, customer c2 WHERE c1.mktsegment = c2.mktsegment AND c2.custkey < 10",
                joinDistributionType);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithMultipleDynamicFilters(JoinDistributionType joinDistributionType)
    {
        assertNoRowFiltering(
                "SELECT a.* FROM customer a INNER JOIN customer b ON a.nationkey = b.nationkey" +
                        " AND a.mktsegment = b.mktsegment",
                joinDistributionType);

        assertRowFiltering(
                "SELECT * FROM (" +
                        "SELECT a.* FROM customer a INNER JOIN customer b ON a.mktsegment = b.mktsegment AND a.custkey = b.custkey) c" +
                        " INNER JOIN nation on c.nationkey = nation.nationkey AND  nation.name IN ('ALGERIA')",
                joinDistributionType);
    }

    protected void assertRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType, String tableName)
    {
        MaterializedResultWithQueryId rowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(
                dynamicRowFiltering(joinDistributionType),
                sql);

        MaterializedResultWithQueryId noRowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(
                noDynamicRowFiltering(joinDistributionType),
                sql);

        // ensure results are correct
        MaterializedResult expected = computeExpected(sql, rowFilteringResultWithQueryId.getResult().getTypes());
        assertEqualsIgnoreOrder(rowFilteringResultWithQueryId.getResult(), expected, "For query: \n " + sql);
        assertEqualsIgnoreOrder(noRowFilteringResultWithQueryId.getResult(), expected, "For query: \n " + sql);

        OperatorStats rowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                rowFilteringResultWithQueryId.getQueryId(),
                tableName);
        // input positions is smaller than physical input positions due to row filtering
        assertThat(rowFilteringProbeStats.getInputPositions())
                .isLessThan(rowFilteringProbeStats.getPhysicalInputPositions());

        OperatorStats noRowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                noRowFilteringResultWithQueryId.getQueryId(),
                tableName);
        assertThat(noRowFilteringProbeStats.getInputPositions())
                .isEqualTo(noRowFilteringProbeStats.getPhysicalInputPositions());
        // input positions is smaller in row filtering case than in no row filtering case
        assertThat(rowFilteringProbeStats.getInputPositions())
                .isLessThan(noRowFilteringProbeStats.getInputPositions());

        Map<String, Metric<?>> metrics = rowFilteringProbeStats.getConnectorMetrics().getMetrics();
        long filterOutputPositions = ((Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_OUTPUT_POSITIONS)).getTotal();
        long filterInputPositions = ((Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_INPUT_POSITIONS)).getTotal();
        assertThat(filterOutputPositions).isLessThan(filterInputPositions);
        assertThat(filterOutputPositions).isLessThan(rowFilteringProbeStats.getPhysicalInputPositions());
        assertThat(((Count<?>) metrics.get(DynamicRowFilteringPageSource.ROW_FILTERING_TIME_MILLIS)).getTotal()).isGreaterThanOrEqualTo(0);
    }

    private void assertRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType)
    {
        assertRowFiltering(sql, joinDistributionType, "customer");
    }

    protected void assertNoRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType, String tableName)
    {
        MaterializedResultWithQueryId rowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(
                dynamicRowFiltering(joinDistributionType),
                sql);

        // ensure results are correct
        MaterializedResult expected = computeExpected(sql, rowFilteringResultWithQueryId.getResult().getTypes());
        assertEqualsIgnoreOrder(rowFilteringResultWithQueryId.getResult(), expected, "For query: \n " + sql);

        OperatorStats rowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                rowFilteringResultWithQueryId.getQueryId(),
                tableName);
        // input positions is equal to physical input positions due to no row filtering
        assertThat(rowFilteringProbeStats.getInputPositions())
                .isEqualTo(rowFilteringProbeStats.getPhysicalInputPositions());

        Map<String, Metric<?>> metrics = rowFilteringProbeStats.getConnectorMetrics().getMetrics();
        Count<?> filterOutputPositions = (Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_OUTPUT_POSITIONS);
        Count<?> filterInputPositions = (Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_INPUT_POSITIONS);
        assertThat(filterOutputPositions).isEqualTo(filterInputPositions);
        assertThat(((Count<?>) metrics.get(DynamicRowFilteringPageSource.ROW_FILTERING_TIME_MILLIS)).getTotal()).isGreaterThanOrEqualTo(0);
    }

    private void assertNoRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType)
    {
        assertNoRowFiltering(sql, joinDistributionType, "customer");
    }

    private OperatorStats getScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
        PlanNodeId nodeId = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof ProjectNode projectNode)) {
                        return false;
                    }
                    if (!(projectNode.getSource() instanceof FilterNode filterNode)) {
                        return false;
                    }
                    if (!(filterNode.getSource() instanceof TableScanNode tableScanNode)) {
                        return false;
                    }
                    if (extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts().isEmpty()) {
                        return false;
                    }
                    return getSchemaTableName(tableScanNode.getTable().getConnectorHandle())
                            .equals(new SchemaTableName("tpch", tableName));
                })
                .findOnlyElement()
                .getId();

        return extractOperatorStatsForNodeId(getDistributedQueryRunner(), queryId, nodeId);
    }

    public static OperatorStats extractOperatorStatsForNodeId(DistributedQueryRunner queryRunner, QueryId queryId, PlanNodeId nodeId)
    {
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()) && summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .collect(MoreCollectors.onlyElement());
    }

    protected abstract SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle);

    private Session dynamicRowFiltering(JoinDistributionType distributionType)
    {
        String catalog = super.getSession().getCatalog().orElseThrow();
        return Session.builder(noJoinReordering(distributionType))
                .setCatalogSessionProperty(catalog, DynamicRowFilteringSessionProperties.DYNAMIC_ROW_FILTERING_ENABLED, "true")
                .setCatalogSessionProperty(catalog, DynamicRowFilteringSessionProperties.DYNAMIC_ROW_FILTERING_WAIT_TIMEOUT, "10m")
                .setCatalogSessionProperty(catalog, DynamicRowFilteringSessionProperties.DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD, "1")
                .build();
    }

    private Session noDynamicRowFiltering(JoinDistributionType distributionType)
    {
        return Session.builder(noJoinReordering(distributionType))
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), DynamicRowFilteringSessionProperties.DYNAMIC_ROW_FILTERING_ENABLED, "false")
                .build();
    }
}
