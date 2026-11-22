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
package io.trino.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.CostCalculator;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsCalculator;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.planner.plan.Patterns.values;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIterativeOptimizer
{
    @Test
    public void testUnchangedSubtreeIsNotExploredAgain()
    {
        try (PlanTester planTester = PlanTester.create(testSessionBuilder().build())) {
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            ValuesNode values = new ValuesNode(idAllocator.getNextId(), 1);
            LimitNode inner = new LimitNode(idAllocator.getNextId(), values, 3, false);
            LimitNode outer = new LimitNode(idAllocator.getNextId(), inner, 2, false);
            AtomicInteger valuesVisits = new AtomicInteger();
            PlanOptimizer optimizer = new IterativeOptimizer(
                    "TestUnchangedSubtree",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    planTester.getStatsCalculator(),
                    planTester.getCostCalculator(),
                    ImmutableSet.of(new RewriteLimits(), new CountValuesVisits(valuesVisits)));

            LimitNode result = (LimitNode) optimizer.optimize(outer, new PlanOptimizer.Context(
                    planTester.getDefaultSession(),
                    new SymbolAllocator(ImmutableList.of()),
                    idAllocator,
                    NOOP,
                    createPlanOptimizersStatsCollector(),
                    _ -> TableStatistics.empty(),
                    RuntimeInfoProvider.noImplementation()));

            assertThat(result.getCount()).isEqualTo(1);
            assertThat(((LimitNode) result.getSource()).getCount()).isEqualTo(1);
            assertThat(valuesVisits.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRuleStatsAndCostProviders(boolean costFirst)
    {
        try (PlanTester planTester = PlanTester.create(testSessionBuilder().build())) {
            AtomicInteger statsCalculations = new AtomicInteger();
            StatsCalculator statsCalculator = (node, _) -> {
                statsCalculations.incrementAndGet();
                return PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(((ValuesNode) node).getRowCount())
                        .build();
            };
            CostCalculator costCalculator = (node, stats, _, _) -> new PlanCostEstimate(stats.getStats(node).getOutputRowCount(), 0, 0, 0);
            List<Integer> observedRowCounts = new ArrayList<>();
            Rule<ValuesNode> rule = new ReadStatsAndCost(costFirst, statsCalculations, observedRowCounts);
            PlanOptimizer optimizer = new IterativeOptimizer(
                    "TestRuleStatsAndCostProviders",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    statsCalculator,
                    costCalculator,
                    ImmutableSet.of(rule));

            planTester.inTransaction(session -> planTester.createPlan(session, "SELECT 1", ImmutableList.of(optimizer), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector()));
            assertThat(observedRowCounts).containsExactly(1, 2);
        }
    }

    @Test
    @Timeout(10)
    public void optimizerQueryRulesStatsCollect()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty("iterative_optimizer_timeout", "5s");
        try (PlanTester planTester = PlanTester.create(sessionBuilder.build())) {
            PlanOptimizersStatsCollector planOptimizersStatsCollector = new PlanOptimizersStatsCollector(10);
            PlanOptimizer optimizer = new IterativeOptimizer(
                    "TestRuleStatsCollection",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    planTester.getStatsCalculator(),
                    planTester.getCostCalculator(),
                    ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

            Session session = sessionBuilder.build();
            planTester.inTransaction(session, transactionSession ->
                    planTester.createPlan(transactionSession, "SELECT 1", ImmutableList.of(optimizer), OPTIMIZED_AND_VALIDATED, NOOP, planOptimizersStatsCollector));
            Optional<QueryPlanOptimizerStatistics> queryRuleStats = planOptimizersStatsCollector.getTopRuleStats().stream().findFirst();

            assertThat(queryRuleStats).isPresent();
            QueryPlanOptimizerStatistics queryRuleStat = queryRuleStats.get();
            assertThat(queryRuleStat.rule()).isEqualTo(RemoveRedundantIdentityProjections.class.getCanonicalName());
            assertThat(queryRuleStat.invocations()).isEqualTo(4);
            assertThat(queryRuleStat.applied()).isEqualTo(3);
            assertThat(queryRuleStat.failures()).isEqualTo(0);
        }
    }

    @Test
    @Timeout(10)
    public void optimizerTimeoutsOnNonConvergingPlan()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty("iterative_optimizer_timeout", "1ms");

        try (PlanTester planTester = PlanTester.create(sessionBuilder.build())) {
            planTester.createCatalog(
                    planTester.getDefaultSession().getCatalog().get(),
                    new TpchConnectorFactory(1),
                    ImmutableMap.of());

            PlanOptimizer optimizer = new IterativeOptimizer(
                    "TestTimeoutOnNonConvergingPlan",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    planTester.getStatsCalculator(),
                    planTester.getCostCalculator(),
                    ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

            assertTrinoExceptionThrownBy(() -> planTester.inTransaction(planTester.getDefaultSession(), transactionSession ->
                    planTester.createPlan(
                            transactionSession,
                            "SELECT nationkey FROM nation",
                            ImmutableList.of(optimizer),
                            OPTIMIZED_AND_VALIDATED,
                            NOOP,
                            createPlanOptimizersStatsCollector())))
                    .hasErrorCode(OPTIMIZER_TIMEOUT)
                    .hasMessageMatching("The optimizer exhausted the time limit of 1 ms: (no rules invoked|(?s)Top rules:.*(RemoveRedundantIdentityProjections|AddIdentityOverTableScan).*)");
        }
    }

    private static class RewriteLimits
            implements Rule<LimitNode>
    {
        @Override
        public Pattern<LimitNode> getPattern()
        {
            return limit();
        }

        @Override
        public Result apply(LimitNode node, Captures captures, Context context)
        {
            if (node.getCount() == 3 ||
                    (node.getCount() == 2 && context.getLookup().resolve(node.getSource()) instanceof LimitNode source && source.getCount() == 1)) {
                return Result.ofPlanNode(new LimitNode(node.getId(), node.getSource(), 1, false));
            }
            return Result.empty();
        }
    }

    private static class CountValuesVisits
            implements Rule<ValuesNode>
    {
        private final AtomicInteger visits;

        public CountValuesVisits(AtomicInteger visits)
        {
            this.visits = visits;
        }

        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode node, Captures captures, Context context)
        {
            visits.incrementAndGet();
            return Result.empty();
        }
    }

    private static class ReadStatsAndCost
            implements Rule<ValuesNode>
    {
        private final boolean costFirst;
        private final AtomicInteger statsCalculations;
        private final List<Integer> observedRowCounts;

        public ReadStatsAndCost(boolean costFirst, AtomicInteger statsCalculations, List<Integer> observedRowCounts)
        {
            this.costFirst = costFirst;
            this.statsCalculations = statsCalculations;
            this.observedRowCounts = observedRowCounts;
        }

        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode node, Captures captures, Context context)
        {
            int calculationsBefore = statsCalculations.get();
            if (costFirst) {
                assertThat(context.getCostProvider().getCost(node).getCpuCost()).isEqualTo(node.getRowCount());
            }
            assertThat(context.getStatsProvider().getStats(node).getOutputRowCount()).isEqualTo(node.getRowCount());
            assertThat(context.getCostProvider().getCost(node).getCpuCost()).isEqualTo(node.getRowCount());
            // Stats requested directly and through the cost provider share the same cache.
            assertThat(statsCalculations.get()).isEqualTo(calculationsBefore + 1);
            observedRowCounts.add(node.getRowCount());
            if (node.getRowCount() == 1) {
                return Result.ofPlanNode(new ValuesNode(node.getId(), 2));
            }
            return Result.empty();
        }
    }

    private static class AddIdentityOverTableScan
            implements Rule<TableScanNode>
    {
        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return tableScan();
        }

        @Override
        public Result apply(TableScanNode tableScan, Captures captures, Context context)
        {
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), tableScan, Assignments.identity(tableScan.getOutputSymbols())));
        }
    }
}
