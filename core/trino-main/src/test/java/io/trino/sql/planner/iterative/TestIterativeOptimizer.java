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
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.EffectivePredicateExtractor;
import io.trino.sql.planner.EffectivePredicateProvider;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
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

import static io.trino.SystemSessionProperties.PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES;
import static io.trino.SystemSessionProperties.isPredicatePushdownUseTableProperties;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testEffectivePredicateProviderLifetimeAndTableProperties(boolean phaseUsesTableProperties)
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .build();
        try (PlanTester planTester = PlanTester.create(defaultSession)) {
            planTester.createCatalog("mock", MockConnectorFactory.builder()
                    .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("c", BIGINT)))
                    .withGetTableProperties((_, _) -> new ConnectorTableProperties(
                            TupleDomain.withColumnDomains(ImmutableMap.of(new MockConnectorColumnHandle("c", BIGINT), Domain.singleValue(BIGINT, 42L))),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableList.of()))
                    .build(), ImmutableMap.of());

            List<EffectivePredicateProvider> providers = new ArrayList<>();
            List<Session> factorySessions = new ArrayList<>();
            Rule<TableScanNode> inspectPredicates = new InspectEffectivePredicates(planTester.getPlannerContext(), phaseUsesTableProperties, providers);

            IterativeOptimizer optimizer = new IterativeOptimizer(
                    "InspectEffectivePredicates",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    planTester.getStatsCalculator(),
                    planTester.getCostCalculator(),
                    _ -> false,
                    ImmutableList.of(),
                    ImmutableSet.of(inspectPredicates),
                    session -> {
                        factorySessions.add(session);
                        return new EffectivePredicateExtractor(planTester.getPlannerContext(), phaseUsesTableProperties && isPredicatePushdownUseTableProperties(session));
                    })
                    .withName("RenamedInspectEffectivePredicates");

            List<EffectivePredicateProvider> previousProviders = new ArrayList<>();
            for (boolean sessionUsesTableProperties : ImmutableList.of(false, true, false)) {
                providers.clear();
                factorySessions.clear();
                Session session = Session.builder(defaultSession)
                        .setSystemProperty(PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES, Boolean.toString(sessionUsesTableProperties))
                        .build();
                planTester.inTransaction(session, transactionSession -> planTester.createPlan(
                        transactionSession,
                        "SELECT c FROM t UNION ALL SELECT c FROM t",
                        ImmutableList.of(optimizer),
                        OPTIMIZED,
                        NOOP,
                        createPlanOptimizersStatsCollector()));
                assertThat(factorySessions).hasSize(1);
                assertThat(isPredicatePushdownUseTableProperties(factorySessions.getFirst())).isEqualTo(sessionUsesTableProperties);
                assertThat(providers).hasSize(2);
                assertThat(providers.getLast()).isSameAs(providers.getFirst());
                for (EffectivePredicateProvider previous : previousProviders) {
                    assertThat(providers.getFirst()).isNotSameAs(previous);
                }
                previousProviders.add(providers.getFirst());
            }
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

    private static class InspectEffectivePredicates
            implements Rule<TableScanNode>
    {
        private final PlannerContext plannerContext;
        private final boolean phaseUsesTableProperties;
        private final List<EffectivePredicateProvider> providers;

        public InspectEffectivePredicates(PlannerContext plannerContext, boolean phaseUsesTableProperties, List<EffectivePredicateProvider> providers)
        {
            this.plannerContext = plannerContext;
            this.phaseUsesTableProperties = phaseUsesTableProperties;
            this.providers = providers;
        }

        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return tableScan();
        }

        @Override
        public Result apply(TableScanNode node, Captures captures, Context context)
        {
            EffectivePredicateProvider provider = context.getEffectivePredicateProvider();
            assertThat(context.getEffectivePredicateProvider()).isSameAs(provider);
            providers.add(provider);
            TupleDomain<?> expected = phaseUsesTableProperties && isPredicatePushdownUseTableProperties(context.getSession())
                    ? TupleDomain.withColumnDomains(ImmutableMap.of(node.getOutputSymbols().getFirst(), Domain.singleValue(BIGINT, 42L)))
                    : TupleDomain.all();
            assertThat(DomainTranslator.getExtractionResult(plannerContext, context.getSession(), provider.getEffectivePredicate(node)).tupleDomain())
                    .isEqualTo(expected);
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
