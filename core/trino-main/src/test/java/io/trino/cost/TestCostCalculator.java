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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.ExchangeNode.replicatedExchange;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCostCalculator
{
    private static final int NUMBER_OF_NODES = 10;
    private static final double AVERAGE_ROW_SIZE = 8.0;
    private static final double IS_NULL_OVERHEAD = 9.0 / AVERAGE_ROW_SIZE;
    private static final double OFFSET_AND_IS_NULL_OVERHEAD = 13.0 / AVERAGE_ROW_SIZE;
    private CostCalculator costCalculatorUsingExchanges;
    private CostCalculator costCalculatorWithEstimatedExchanges;
    private PlanFragmenter planFragmenter;
    private Session session;
    private PlanTester planTester;

    @BeforeAll
    public void setUp()
    {
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(() -> NUMBER_OF_NODES);
        costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(taskCountEstimator);
        costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, taskCountEstimator);

        session = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).build();

        planTester = PlanTester.create(session);
        PlannerContext plannerContext = planTester.getPlannerContext();
        plannerContext.getLanguageFunctionManager().registerQuery(session);
        planTester.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(), ImmutableMap.of());

        planFragmenter = new PlanFragmenter(
                plannerContext.getMetadata(),
                plannerContext.getFunctionManager(),
                planTester.getTransactionManager(),
                planTester.getCatalogManager(),
                plannerContext.getLanguageFunctionManager(),
                new QueryManagerConfig());
    }

    @AfterAll
    public void tearDown()
    {
        costCalculatorUsingExchanges = null;
        costCalculatorWithEstimatedExchanges = null;
        planFragmenter = null;
        session = null;
        planTester.close();
        planTester = null;
    }

    @Test
    public void testTableScan()
    {
        TableScanNode tableScan = tableScan("ts", new Symbol(BIGINT, "orderkey"));

        assertCost(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)))
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)))
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostFragmentedPlan(tableScan, ImmutableMap.of(), ImmutableMap.of("ts", statsEstimate(tableScan, 1000)))
                .cpu(1000 * IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(tableScan);
    }

    @Test
    public void testProject()
    {
        TableScanNode tableScan = tableScan("ts", new Symbol(BIGINT, "orderkey"));
        PlanNode project = project("project", tableScan, new Symbol(VARCHAR, "string"), new Cast(new Reference(BIGINT, "orderkey"), VARCHAR));
        Map<String, PlanCostEstimate> costs = ImmutableMap.of("ts", cpuCost(1000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "project", statsEstimate(project, 4000),
                "ts", statsEstimate(tableScan, 1000));

        assertCost(project, costs, stats)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(project, costs, stats)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostFragmentedPlan(project, costs, stats)
                .cpu(1000 + 4000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(project);
    }

    @Test
    public void testFilter()
    {
        TableScanNode tableScan = tableScan("ts", new Symbol(VARCHAR, "string"));
        IsNull expression = new IsNull(new Reference(VARCHAR, "string"));
        FilterNode filter = new FilterNode(new PlanNodeId("filter"), tableScan, expression);
        Map<String, PlanCostEstimate> costs = ImmutableMap.of("ts", cpuCost(1000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "filter", statsEstimate(filter, 4000),
                "ts", statsEstimate(tableScan, 1000));

        assertCost(filter, costs, stats)
                .cpu(1000 + 1000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(filter, costs, stats)
                .cpu(1000 + 1000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostFragmentedPlan(filter, costs, stats)
                .cpu(1000 + 1000 * OFFSET_AND_IS_NULL_OVERHEAD)
                .memory(0)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(filter);
    }

    @Test
    public void testRepartitionedJoin()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "orderkey_0"));
        JoinNode join = join("join",
                ts1,
                ts2,
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));

        assertCost(join, costs, stats)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000 + 6000 + 1000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network((6000 + 1000) * IS_NULL_OVERHEAD);

        assertCostFragmentedPlan(join, costs, stats)
                .cpu(6000 + 1000 + (12000 + 6000 + 1000) * IS_NULL_OVERHEAD)
                .memory(1000 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(join);
    }

    @Test
    public void testReplicatedJoin()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "orderkey_0"));
        JoinNode join = join("join",
                ts1,
                ts2,
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(6000),
                "ts2", cpuCost(1000));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 12000),
                "ts1", statsEstimate(ts1, 6000),
                "ts2", statsEstimate(ts2, 1000));

        assertCost(join, costs, stats)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(join, costs, stats)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * NUMBER_OF_NODES) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD);

        assertCostFragmentedPlan(join, costs, stats)
                .cpu(1000 + 6000 + (12000 + 6000 + 10000 + 1000 * (NUMBER_OF_NODES - 1)) * IS_NULL_OVERHEAD)
                .memory(1000 * NUMBER_OF_NODES * IS_NULL_OVERHEAD)
                .network(0);

        assertCostHasUnknownComponentsForUnknownStats(join);
    }

    @Test
    public void testMemoryCostJoinAboveJoin()
    {
        //      join
        //     /   \
        //   ts1    join23
        //          /    \
        //        ts2     ts3

        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "key1"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "key2"));
        TableScanNode ts3 = tableScan("ts3", new Symbol(BIGINT, "key3"));
        JoinNode join23 = join(
                "join23",
                ts2,
                ts3,
                JoinNode.DistributionType.PARTITIONED,
                "key2",
                "key3");
        JoinNode join = join(
                "join",
                ts1,
                join23,
                JoinNode.DistributionType.PARTITIONED,
                "key1",
                "key2");

        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", new PlanCostEstimate(0, 128, 128, 0),
                "ts2", new PlanCostEstimate(0, 64, 64, 0),
                "ts3", new PlanCostEstimate(0, 32, 32, 0));

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "join", statsEstimate(join, 10_000),
                "join23", statsEstimate(join23, 2_000),
                "ts1", statsEstimate(ts1, 10_000),
                "ts2", statsEstimate(ts2, 1_000),
                "ts3", statsEstimate(ts3, 100));

        assertCost(join23, costs, stats)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCost(join, costs, stats)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint

        assertCostEstimatedExchanges(join23, costs, stats)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCostEstimatedExchanges(join, costs, stats)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint

        assertCostFragmentedPlan(join23, costs, stats)
                .memory(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64 + 32) // ts2, ts3 memory footprint
                .memoryWhenOutputting(
                        100 * IS_NULL_OVERHEAD // join23 memory footprint
                                + 64); // ts2 memory footprint

        assertCostFragmentedPlan(join, costs, stats)
                .memory(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 100 * IS_NULL_OVERHEAD + 64 // join23 total memory when outputting
                                + 128) // ts1 memory footprint
                .memoryWhenOutputting(
                        2000 * IS_NULL_OVERHEAD // join memory footprint
                                + 128); // ts1 memory footprint
    }

    @Test
    public void testAggregation()
    {
        TableScanNode tableScan = tableScan("ts", new Symbol(BIGINT, "orderkey"));
        AggregationNode aggregation = aggregation("agg", tableScan);

        Map<String, PlanCostEstimate> costs = ImmutableMap.of("ts", cpuCost(6000));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts", statsEstimate(tableScan, 6000),
                "agg", statsEstimate(aggregation, 13));

        assertCost(aggregation, costs, stats)
                .cpu(6000 * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0);

        assertCostEstimatedExchanges(aggregation, costs, stats)
                .cpu((6000 + 6000 + 6000) * IS_NULL_OVERHEAD + 6000)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(6000 * IS_NULL_OVERHEAD);

        assertCostFragmentedPlan(aggregation, costs, stats)
                .cpu(6000 + 6000 * IS_NULL_OVERHEAD)
                .memory(13 * IS_NULL_OVERHEAD)
                .network(0 * IS_NULL_OVERHEAD);

        assertCostHasUnknownComponentsForUnknownStats(aggregation);
    }

    @Test
    public void testRepartitionedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "orderkey_0"));
        ExchangeNode remoteExchange1 = partitionedExchange(new PlanNodeId("re1"), REMOTE, ts1, ImmutableList.of(new Symbol(BIGINT, "orderkey")), Optional.empty());
        ExchangeNode remoteExchange2 = partitionedExchange(new PlanNodeId("re2"), REMOTE, ts2, ImmutableList.of(new Symbol(BIGINT, "orderkey_0")), Optional.empty());
        ExchangeNode localExchange = partitionedExchange(new PlanNodeId("le"), LOCAL, remoteExchange2, ImmutableList.of(new Symbol(BIGINT, "orderkey_0")), Optional.empty());

        JoinNode join = join("join",
                remoteExchange1,
                localExchange,
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.<String, PlanNodeStatsEstimate>builder()
                .put("join", statsEstimate(join, 12000))
                .put("re1", statsEstimate(remoteExchange1, 10000))
                .put("re2", statsEstimate(remoteExchange2, 10000))
                .put("le", statsEstimate(localExchange, 6000))
                .put("ts1", statsEstimate(ts1, 6000))
                .put("ts2", statsEstimate(ts2, 1000))
                .buildOrThrow();

        assertFragmentedEqualsUnfragmented(join, stats);
    }

    @Test
    public void testReplicatedJoinWithExchange()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "orderkey_0"));
        ExchangeNode remoteExchange2 = replicatedExchange(new PlanNodeId("re2"), REMOTE, ts2);
        ExchangeNode localExchange = partitionedExchange(new PlanNodeId("le"), LOCAL, remoteExchange2, ImmutableList.of(new Symbol(BIGINT, "orderkey_0")), Optional.empty());

        JoinNode join = join("join",
                ts1,
                localExchange,
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.<String, PlanNodeStatsEstimate>builder()
                .put("join", statsEstimate(join, 12000))
                .put("re2", statsEstimate(remoteExchange2, 10000))
                .put("le", statsEstimate(localExchange, 6000))
                .put("ts1", statsEstimate(ts1, 6000))
                .put("ts2", statsEstimate(ts2, 1000))
                .buildOrThrow();

        assertFragmentedEqualsUnfragmented(join, stats);
    }

    @Test
    public void testUnion()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        TableScanNode ts2 = tableScan("ts2", new Symbol(BIGINT, "orderkey_0"));
        ImmutableListMultimap.Builder<Symbol, Symbol> outputMappings = ImmutableListMultimap.builder();
        outputMappings.put(new Symbol(BIGINT, "orderkey_1"), new Symbol(BIGINT, "orderkey"));
        outputMappings.put(new Symbol(BIGINT, "orderkey_1"), new Symbol(BIGINT, "orderkey_0"));
        UnionNode union = new UnionNode(new PlanNodeId("union"), ImmutableList.of(ts1, ts2), outputMappings.build(), ImmutableList.of(new Symbol(BIGINT, "orderkey_1")));
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts1", statsEstimate(ts1, 4000),
                "ts2", statsEstimate(ts2, 1000),
                "union", statsEstimate(ts1, 5000));
        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(1000),
                "ts2", cpuCost(1000));
        assertCost(union, costs, stats)
                .cpu(2000)
                .memory(0)
                .network(0);
        assertCostEstimatedExchanges(union, costs, stats)
                .cpu(2000)
                .memory(0)
                .network(5000 * IS_NULL_OVERHEAD);
    }

    @Test
    public void testLimit()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        LimitNode limit = new LimitNode(new PlanNodeId("limit"), ts1, 5, false);
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts1", statsEstimate(ts1, 4000),
                "limit", statsEstimate(ts1, 40)); // 5 * average row size
        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(1000));
        // Do not estimate cost other than CPU for limit node.
        assertCost(limit, costs, stats)
                .cpu(1045) // 1000 + (is null boolean array) + 40
                .memory(0)
                .network(0);
        assertCostEstimatedExchanges(limit, costs, stats)
                .cpu(1045)
                .memory(0)
                .network(0);
    }

    @Test
    public void testEnforceSingleRow()
    {
        TableScanNode ts1 = tableScan("ts1", new Symbol(BIGINT, "orderkey"));
        EnforceSingleRowNode singleRow = new EnforceSingleRowNode(new PlanNodeId("singleRow"), ts1);
        Map<String, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                "ts1", statsEstimate(ts1, 4000),
                "singleRow", statsEstimate(ts1, 8)); // 1 * Average Row Size
        Map<String, PlanCostEstimate> costs = ImmutableMap.of(
                "ts1", cpuCost(1000));
        assertCost(singleRow, costs, stats)
                .cpu(1000) // Only count the accumulated cost of source nodes
                .memory(0)
                .network(0);
        assertCostEstimatedExchanges(singleRow, costs, stats)
                .cpu(1000)
                .memory(0)
                .network(0);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return assertCost(costCalculatorUsingExchanges, node, costs, stats);
    }

    private CostAssertionBuilder assertCostEstimatedExchanges(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        return assertCost(costCalculatorWithEstimatedExchanges, node, costs, stats);
    }

    private CostAssertionBuilder assertCostFragmentedPlan(
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator(stats), session, new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session));
        CostProvider costProvider = new TestingCostProvider(costs, costCalculatorUsingExchanges, statsProvider, session);
        SubPlan subPlan = fragment(new Plan(node, StatsAndCosts.create(node, statsProvider, costProvider)));
        return new CostAssertionBuilder(subPlan.getFragment().getStatsAndCosts().getCosts().getOrDefault(node.getId(), PlanCostEstimate.unknown()));
    }

    private static class TestingCostProvider
            implements CostProvider
    {
        private final Map<String, PlanCostEstimate> costs;
        private final CostCalculator costCalculator;
        private final StatsProvider statsProvider;
        private final Session session;

        private TestingCostProvider(Map<String, PlanCostEstimate> costs, CostCalculator costCalculator, StatsProvider statsProvider, Session session)
        {
            this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanCostEstimate getCost(PlanNode node)
        {
            if (costs.containsKey(node.getId().toString())) {
                return costs.get(node.getId().toString());
            }
            return costCalculator.calculateCost(node, statsProvider, this, session);
        }
    }

    private CostAssertionBuilder assertCost(
            CostCalculator costCalculator,
            PlanNode node,
            Map<String, PlanCostEstimate> costs,
            Map<String, PlanNodeStatsEstimate> stats)
    {
        Function<PlanNode, PlanNodeStatsEstimate> statsProvider = planNode -> stats.get(planNode.getId().toString());
        PlanCostEstimate cost = calculateCost(
                costCalculator,
                node,
                sourceCostProvider(costCalculator, costs, statsProvider),
                statsProvider);
        return new CostAssertionBuilder(cost);
    }

    private Function<PlanNode, PlanCostEstimate> sourceCostProvider(
            CostCalculator costCalculator,
            Map<String, PlanCostEstimate> costs,
            Function<PlanNode, PlanNodeStatsEstimate> statsProvider)
    {
        return node -> {
            PlanCostEstimate providedCost = costs.get(node.getId().toString());
            if (providedCost != null) {
                return providedCost;
            }
            return calculateCost(
                    costCalculator,
                    node,
                    sourceCostProvider(costCalculator, costs, statsProvider),
                    statsProvider);
        };
    }

    private void assertCostHasUnknownComponentsForUnknownStats(PlanNode node)
    {
        new CostAssertionBuilder(calculateCost(
                costCalculatorUsingExchanges,
                node,
                planNode -> PlanCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown()))
                .hasUnknownComponents();
        new CostAssertionBuilder(calculateCost(
                costCalculatorWithEstimatedExchanges,
                node,
                planNode -> PlanCostEstimate.unknown(),
                planNode -> PlanNodeStatsEstimate.unknown()))
                .hasUnknownComponents();
    }

    private void assertFragmentedEqualsUnfragmented(PlanNode node, Map<String, PlanNodeStatsEstimate> stats)
    {
        StatsCalculator statsCalculator = statsCalculator(stats);
        PlanCostEstimate costWithExchanges = calculateCost(node, costCalculatorUsingExchanges, statsCalculator);
        PlanCostEstimate costWithFragments = calculateCostFragmentedPlan(node, statsCalculator);
        assertThat(costWithExchanges).isEqualTo(costWithFragments);
    }

    private StatsCalculator statsCalculator(Map<String, PlanNodeStatsEstimate> stats)
    {
        return (node, context) -> requireNonNull(stats.get(node.getId().toString()), "no stats for node");
    }

    private PlanCostEstimate calculateCost(
            CostCalculator costCalculator,
            PlanNode node,
            Function<PlanNode, PlanCostEstimate> costs,
            Function<PlanNode, PlanNodeStatsEstimate> stats)
    {
        return costCalculator.calculateCost(
                node,
                planNode -> requireNonNull(stats.apply(planNode), "no stats for node"),
                source -> requireNonNull(costs.apply(source), format("no cost for source: %s", source.getId())),
                session);
    }

    private PlanCostEstimate calculateCost(PlanNode node, CostCalculator costCalculator, StatsCalculator statsCalculator)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session));
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
        return costProvider.getCost(node);
    }

    private PlanCostEstimate calculateCostFragmentedPlan(PlanNode node, StatsCalculator statsCalculator)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session));
        CostProvider costProvider = new CachingCostProvider(costCalculatorUsingExchanges, statsProvider, Optional.empty(), session);
        SubPlan subPlan = fragment(new Plan(node, StatsAndCosts.create(node, statsProvider, costProvider)));
        return subPlan.getFragment().getStatsAndCosts().getCosts().getOrDefault(node.getId(), PlanCostEstimate.unknown());
    }

    private static class CostAssertionBuilder
    {
        private final PlanCostEstimate actual;

        CostAssertionBuilder(PlanCostEstimate actual)
        {
            this.actual = requireNonNull(actual, "actual is null");
        }

        CostAssertionBuilder cpu(double value)
        {
            assertThat(actual.getCpuCost()).isCloseTo(value, offset(0.000001));
            return this;
        }

        CostAssertionBuilder memory(double value)
        {
            assertThat(actual.getMaxMemory()).isCloseTo(value, offset(0.000001));
            return this;
        }

        CostAssertionBuilder memoryWhenOutputting(double value)
        {
            assertThat(actual.getMaxMemoryWhenOutputting()).isCloseTo(value, offset(0.000001));
            return this;
        }

        CostAssertionBuilder network(double value)
        {
            assertThat(actual.getNetworkCost()).isCloseTo(value, offset(0.000001));
            return this;
        }

        CostAssertionBuilder hasUnknownComponents()
        {
            assertThat(actual.hasUnknownComponents()).isTrue();
            return this;
        }
    }

    private static PlanNodeStatsEstimate statsEstimate(PlanNode node, double outputSizeInBytes)
    {
        return statsEstimate(node.getOutputSymbols(), outputSizeInBytes);
    }

    private static PlanNodeStatsEstimate statsEstimate(Collection<Symbol> symbols, double outputSizeInBytes)
    {
        checkArgument(symbols.size() > 0, "No symbols");
        checkArgument(ImmutableSet.copyOf(symbols).size() == symbols.size(), "Duplicate symbols");

        double rowCount = outputSizeInBytes / symbols.size() / AVERAGE_ROW_SIZE;

        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount);
        for (Symbol symbol : symbols) {
            builder.addSymbolStatistics(
                    symbol,
                    SymbolStatsEstimate.builder()
                            .setNullsFraction(0)
                            .setAverageRowSize(AVERAGE_ROW_SIZE)
                            .build());
        }
        return builder.build();
    }

    private TableScanNode tableScan(String id, Symbol... symbols)
    {
        List<Symbol> symbolsList = ImmutableList.copyOf(symbols);
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();

        for (Symbol symbol : symbolsList) {
            assignments.put(symbol, new TpchColumnHandle("orderkey", BIGINT));
        }

        return new TableScanNode(
                new PlanNodeId(id),
                planTester.getTableHandle(TEST_CATALOG_NAME, "sf1", "orders"),
                symbolsList,
                assignments.buildOrThrow(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
    }

    private PlanNode project(String id, PlanNode source, Symbol symbol, Expression expression)
    {
        return new ProjectNode(
                new PlanNodeId(id),
                source,
                Assignments.of(symbol, expression));
    }

    private AggregationNode aggregation(String id, PlanNode source)
    {
        AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                new TestingFunctionResolution(planTester.getTransactionManager(), planTester.getPlannerContext()).resolveFunction("count", ImmutableList.of()),
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return singleAggregation(
                new PlanNodeId(id),
                source,
                ImmutableMap.of(new Symbol(BIGINT, "count"), aggregation),
                singleGroupingSet(source.getOutputSymbols()));
    }

    /**
     * EquiJoinClause is created from symbols in form of:
     * symbol[0] = symbol[1] AND symbol[2] = symbol[3] AND ...
     */
    private JoinNode join(String planNodeId, PlanNode left, PlanNode right, JoinNode.DistributionType distributionType, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, symbols[i]), new Symbol(BIGINT, symbols[i + 1])));
        }

        return new JoinNode(
                new PlanNodeId(planNodeId),
                JoinType.INNER,
                left,
                right,
                criteria.build(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private SubPlan fragment(Plan plan)
    {
        return inTransaction(session -> planFragmenter.createSubPlans(session, plan, false, WarningCollector.NOOP));
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        Metadata metadata = planTester.getPlannerContext().getMetadata();
        return transaction(planTester.getTransactionManager(), metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }

    private static PlanCostEstimate cpuCost(double cpuCost)
    {
        return new PlanCostEstimate(cpuCost, 0, 0, 0);
    }
}
