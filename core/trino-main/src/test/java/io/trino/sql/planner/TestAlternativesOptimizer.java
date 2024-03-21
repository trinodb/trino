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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.StaticRuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.cache.CacheCommonSubqueries.isCacheChooseAlternativeNode;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAlternativesOptimizer
        extends BasePlanTest
{
    private static final String TEST_CATALOG_NAME = "test_catalog";
    private static final String TEST_SCHEMA_NAME = "test_schema";

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TEST_SCHEMA_NAME);

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(1), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testSingleRule()
    {
        String tableName = "nation";
        TableHandle tableHandle = new TableHandle(
                getPlanTester().getCatalogHandle(TEST_CATALOG_NAME),
                new TpchTableHandle(TEST_SCHEMA_NAME, tableName, 1.0),
                TestingTransactionHandle.create());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Session session = getPlanTester().getDefaultSession();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, getPlanTester().getPlannerContext(), session);
        ProjectNode plan = planBuilder.project(
                Assignments.of(),
                planBuilder.filter(
                        idAllocator.getNextId(),
                        TRUE,
                        planBuilder.tableScan(tableHandle, emptyList(), emptyMap())));

        PlanNode optimized = runOptimizer(
                plan,
                ImmutableSet.of(new CreateAlternativesForFilter(FALSE)));

        assertPlan(
                optimized,
                chooseAlternativeNode(
                        strictProject(
                                ImmutableMap.of(),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName))),
                        strictProject(
                                ImmutableMap.of(),
                                PlanMatchPattern.filter(
                                        FALSE,
                                        PlanMatchPattern.tableScan(tableName)))));
    }

    @Test
    public void testWithSplitLevelCache()
    {
        String tableName = "nation";
        TableHandle tableHandle = new TableHandle(
                getPlanTester().getCatalogHandle(TEST_CATALOG_NAME),
                new TpchTableHandle(TEST_SCHEMA_NAME, tableName, 1.0),
                TestingTransactionHandle.create());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Session session = getPlanTester().getDefaultSession();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, getPlanTester().getPlannerContext(), session);
        TableScanNode scan = planBuilder.tableScan(tableHandle, emptyList(), emptyMap());
        PlanNode plan = new ChooseAlternativeNode(
                idAllocator.getNextId(),
                ImmutableList.of(
                        planBuilder.filter(
                                idAllocator.getNextId(),
                                TRUE,
                                scan),
                        scan,
                        new LoadCachedDataPlanNode(
                                idAllocator.getNextId(),
                                new PlanSignatureWithPredicate(
                                        new PlanSignature(new SignatureKey("sig"), Optional.empty(), ImmutableList.of(), ImmutableList.of()),
                                        TupleDomain.all()),
                                FALSE,
                                ImmutableMap.of(),
                                ImmutableList.of())),
                new FilteredTableScan(scan, Optional.empty()));

        assertThat(isCacheChooseAlternativeNode(plan)).isTrue();

        PlanNode optimized = runOptimizer(
                plan,
                ImmutableSet.of(new CreateAlternativesForFilter(FALSE)));

        assertThat(isCacheChooseAlternativeNode(optimized)).isTrue();
        assertThat(PlanNodeSearcher.searchFrom(optimized).whereIsInstanceOfAny(ChooseAlternativeNode.class).count()).isEqualTo(1);
    }

    @Test
    public void testTwoRules()
    {
        String tableName = "nation";
        TableHandle tableHandle = new TableHandle(
                getPlanTester().getCatalogHandle(TEST_CATALOG_NAME),
                new TpchTableHandle(TEST_SCHEMA_NAME, tableName, 1.0),
                TestingTransactionHandle.create());

        String symbol = "symbol";
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Session session = getPlanTester().getDefaultSession();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, getPlanTester().getPlannerContext(), session);
        ProjectNode plan = planBuilder.project(
                Assignments.of(new Symbol(BOOLEAN, symbol), TRUE),
                planBuilder.filter(
                        idAllocator.getNextId(),
                        TRUE,
                        planBuilder.tableScan(tableHandle, emptyList(), emptyMap())));

        PlanNode optimized = runOptimizer(
                plan,
                ImmutableSet.of(new CreateAlternativesForFilter(FALSE), new CreateAlternativesForProject(FALSE)));

        assertPlan(
                optimized,
                chooseAlternativeNode(
                        strictProject(
                                ImmutableMap.of(symbol, expression(TRUE)),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName))),
                        strictProject(
                                ImmutableMap.of(symbol, expression(FALSE)),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName))),
                        strictProject(
                                ImmutableMap.of(symbol, expression(TRUE)),
                                PlanMatchPattern.filter(
                                        FALSE,
                                        PlanMatchPattern.tableScan(tableName))),
                        strictProject(
                                ImmutableMap.of(symbol, expression(FALSE)),
                                PlanMatchPattern.filter(
                                        FALSE,
                                        PlanMatchPattern.tableScan(tableName)))));
    }

    @Test
    public void testTwoRulesOnTheSameNode()
    {
        String tableName = "nation";
        TableHandle tableHandle = new TableHandle(
                getPlanTester().getCatalogHandle(TEST_CATALOG_NAME),
                new TpchTableHandle(TEST_SCHEMA_NAME, tableName, 1.0),
                TestingTransactionHandle.create());

        String columnName = "nationkey";
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Session session = getPlanTester().getDefaultSession();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, getPlanTester().getPlannerContext(), session);
        Symbol symbol = planBuilder.symbol(columnName, BIGINT);
        ProjectNode plan = planBuilder.project(
                Assignments.of(symbol, new Comparison(EQUAL, new Reference(BIGINT, columnName), new Constant(BIGINT, 1L))),
                planBuilder.filter(
                        idAllocator.getNextId(),
                        TRUE,
                        planBuilder.tableScan(
                                tableHandle,
                                List.of(symbol),
                                ImmutableMap.of(symbol, new TpchColumnHandle(columnName, BIGINT)))));

        PlanNode optimized = runOptimizer(
                plan,
                ImmutableSet.of(
                        new CreateAlternativesForProject(new Comparison(EQUAL, new Reference(BIGINT, columnName), new Constant(BIGINT, 2L))),
                        new CreateAlternativesForProject(new Comparison(EQUAL, new Reference(BIGINT, columnName), new Constant(BIGINT, 3L)))));

        assertPlan(
                optimized,
                chooseAlternativeNode(
                        strictProject(
                                ImmutableMap.of(columnName, expression(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 1L)))),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName, ImmutableMap.of("nationkey", "nationkey")))),
                        strictProject(
                                ImmutableMap.of(columnName, expression(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 2L)))),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName, ImmutableMap.of("nationkey", "nationkey")))),
                        // Each rule returns the original plan. Since the results are accumulated, it's expected to have the same alternative twice.
                        // This might be improved in the future (see AlternativesOptimizer.exploreNode)
                        strictProject(
                                ImmutableMap.of(columnName, expression(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 1L)))),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName, ImmutableMap.of("nationkey", "nationkey")))),
                        strictProject(
                                ImmutableMap.of(columnName, expression(new Comparison(EQUAL, new Reference(BIGINT, "nationkey"), new Constant(BIGINT, 3L)))),
                                PlanMatchPattern.filter(
                                        TRUE,
                                        PlanMatchPattern.tableScan(tableName, ImmutableMap.of("nationkey", "nationkey"))))));
    }

    private PlanNode runOptimizer(PlanNode plan, Set<Rule<?>> rules)
    {
        Session session = getPlanTester().getDefaultSession();

        AlternativesOptimizer optimizer = new AlternativesOptimizer(
                getPlanTester().getPlannerContext(),
                new RuleStatsRecorder(),
                getPlanTester().getStatsCalculator(),
                getPlanTester().getCostCalculator(),
                rules);

        return optimizer.optimize(
                plan,
                new PlanOptimizer.Context(
                        session,
                        new SymbolAllocator(),
                        new PlanNodeIdAllocator(),
                        WarningCollector.NOOP,
                        createPlanOptimizersStatsCollector(),
                        new CachingTableStatsProvider(getPlanTester().getPlannerContext().getMetadata(), session),
                        new StaticRuntimeInfoProvider(ImmutableMap.of(), ImmutableMap.of())));
    }

    private void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        getPlanTester().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(
                    session,
                    getPlanTester().getPlannerContext().getMetadata(),
                    getPlanTester().getPlannerContext().getFunctionManager(),
                    getPlanTester().getStatsCalculator(),
                    new Plan(actual, StatsAndCosts.empty()),
                    pattern);
            return null;
        });
    }

    private static class CreateAlternativesForFilter
            implements Rule<FilterNode>
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
        private static final Pattern<FilterNode> PATTERN = filter()
                .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

        private final Expression alternativeExpression;

        public CreateAlternativesForFilter(Expression alternativeExpression)
        {
            this.alternativeExpression = alternativeExpression;
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            TableScanNode tableScan = captures.get(TABLE_SCAN);
            TableHandle table = tableScan.getTable();

            TableScanNode alternativeScan = new TableScanNode(
                    context.getIdAllocator().getNextId(),
                    new TableHandle(table.getCatalogHandle(), table.getConnectorHandle(), table.getTransaction()),
                    tableScan.getOutputSymbols(),
                    tableScan.getAssignments(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getStatistics(),
                    tableScan.isUpdateTarget(),
                    tableScan.getUseConnectorNodePartitioning());
            FilterNode alternativeFilter = new FilterNode(
                    context.getIdAllocator().getNextId(),
                    alternativeScan,
                    alternativeExpression);

            return Result.ofNodeAlternatives(Optional.empty(), List.of(alternativeFilter));
        }
    }

    private static class CreateAlternativesForProject
            implements Rule<ProjectNode>
    {
        private static final Capture<FilterNode> FILTER = newCapture();
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(filter().capturedAs(FILTER)
                        .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

        private final Expression alternativeAssignmentExpression;

        public CreateAlternativesForProject(Expression alternativeAssignmentExpression)
        {
            this.alternativeAssignmentExpression = alternativeAssignmentExpression;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            TableScanNode tableScan = captures.get(TABLE_SCAN);
            TableHandle table = tableScan.getTable();
            FilterNode filterNode = captures.get(FILTER);

            TableScanNode alternativeScan = new TableScanNode(
                    context.getIdAllocator().getNextId(),
                    new TableHandle(table.getCatalogHandle(), table.getConnectorHandle(), table.getTransaction()),
                    tableScan.getOutputSymbols(),
                    tableScan.getAssignments(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getStatistics(),
                    tableScan.isUpdateTarget(),
                    tableScan.getUseConnectorNodePartitioning());
            FilterNode alternativeFilter = new FilterNode(
                    context.getIdAllocator().getNextId(),
                    alternativeScan,
                    filterNode.getPredicate());

            Assignments alternativeAssignments = Assignments.copyOf(
                    projectNode.getAssignments().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> alternativeAssignmentExpression)));
            ProjectNode alternativeProject = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    alternativeFilter,
                    alternativeAssignments);

            return Result.ofNodeAlternatives(Optional.empty(), List.of(alternativeProject));
        }
    }
}
