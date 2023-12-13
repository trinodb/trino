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
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.plan.Patterns.tableScan;
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
    @Timeout(10)
    public void optimizerQueryRulesStatsCollect()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty("iterative_optimizer_timeout", "5s");
        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build())) {
            PlanOptimizersStatsCollector planOptimizersStatsCollector = new PlanOptimizersStatsCollector(10);
            PlanOptimizer optimizer = new IterativeOptimizer(
                    queryRunner.getPlannerContext(),
                    new RuleStatsRecorder(),
                    queryRunner.getStatsCalculator(),
                    queryRunner.getCostCalculator(),
                    ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

            Session session = sessionBuilder.build();
            queryRunner.inTransaction(session, transactionSession ->
                    queryRunner.createPlan(transactionSession, "SELECT 1", ImmutableList.of(optimizer), OPTIMIZED_AND_VALIDATED, NOOP, planOptimizersStatsCollector));
            Optional<QueryPlanOptimizerStatistics> queryRuleStats = planOptimizersStatsCollector.getTopRuleStats().stream().findFirst();

            assertThat(queryRuleStats.isPresent()).isTrue();
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

        try (LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build())) {
            queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                    new TpchConnectorFactory(1),
                    ImmutableMap.of());

            PlanOptimizer optimizer = new IterativeOptimizer(
                    queryRunner.getPlannerContext(),
                    new RuleStatsRecorder(),
                    queryRunner.getStatsCalculator(),
                    queryRunner.getCostCalculator(),
                    ImmutableSet.of(new AddIdentityOverTableScan(), new RemoveRedundantIdentityProjections()));

            assertTrinoExceptionThrownBy(() -> queryRunner.inTransaction(queryRunner.getDefaultSession(), transactionSession ->
                    queryRunner.createPlan(
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
