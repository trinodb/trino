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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.MergeUnion;
import io.trino.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

/**
 * Demonstrates that `MergeUnion` and `PushLimitThroughUnion` interact badly when placed in the
 * same `IterativeOptimizer`: `PushLimitThroughUnion` fires first (top-down traversal visits the
 * `LimitNode` before its `UnionNode` children), inserts intermediate `LimitNode`s between the
 * outer and inner unions, and `MergeUnion` can no longer flatten them.
 *
 * <p>This corroborates the comment in `PlanOptimizers.java` ("MergeUnion ... must run before
 * limit pushdown rules"), and shows that the actual safeguard today is `InitialPlanCleanup`
 * running `MergeUnion` to fixpoint before any limit-pushdown rules are introduced.
 */
public class TestMergeUnionAndPushLimitInteraction
        extends BasePlanTest
{
    /**
     * Baseline: `MergeUnion` alone successfully flattens nested unions wrapped in a limit.
     */
    @Test
    public void mergeUnionAloneFlattensNestedUnionsUnderLimit()
    {
        assertOptimizedPlan(
                ImmutableSet.of(new MergeUnion()),
                builder -> nestedUnionUnderLimit(builder, 10),
                limit(10,
                        union(values("a1"), values("a2"), values("a3"), values("a4"))));
    }

    /**
     * Baseline: `PushLimitThroughUnion` alone pushes the limit through the outer union.
     */
    @Test
    public void pushLimitThroughUnionAloneInsertsInnerLimits()
    {
        assertOptimizedPlan(
                ImmutableSet.of(new PushLimitThroughUnion()),
                builder -> nestedUnionUnderLimit(builder, 10),
                limit(10,
                        union(
                                limit(10, ImmutableList.of(), true, union(values("a1"), values("a2"))),
                                limit(10, ImmutableList.of(), true, union(values("a3"), values("a4"))))));
    }

    /**
     * Conflict: when both rules co-exist in one IterativeOptimizer, `PushLimitThroughUnion` fires
     * first (top-down) and blocks `MergeUnion`. The result keeps the nested-union shape with
     * inserted intermediate limits — `MergeUnion` never gets to flatten.
     */
    @Test
    public void bothRulesTogetherFailToFlatten()
    {
        assertOptimizedPlan(
                ImmutableSet.of(new MergeUnion(), new PushLimitThroughUnion()),
                builder -> nestedUnionUnderLimit(builder, 10),
                limit(10,
                        union(
                                limit(10, ImmutableList.of(), true, union(values("a1"), values("a2"))),
                                limit(10, ImmutableList.of(), true, union(values("a3"), values("a4"))))));
    }

    /**
     * Builds the plan:
     * <pre>
     *   Limit(N)
     *     Union(outer)
     *       Union(inner1)
     *         Values("a1")
     *         Values("a2")
     *       Union(inner2)
     *         Values("a3")
     *         Values("a4")
     * </pre>
     */
    private static PlanNode nestedUnionUnderLimit(PlanBuilder p, long limit)
    {
        Symbol a1 = p.symbol("a1", BIGINT);
        Symbol a2 = p.symbol("a2", BIGINT);
        Symbol a3 = p.symbol("a3", BIGINT);
        Symbol a4 = p.symbol("a4", BIGINT);
        Symbol inner1Out = p.symbol("inner1", BIGINT);
        Symbol inner2Out = p.symbol("inner2", BIGINT);
        Symbol outerOut = p.symbol("outer", BIGINT);

        PlanNode inner1 = p.union(
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(inner1Out, a1)
                        .put(inner1Out, a2)
                        .build(),
                ImmutableList.of(p.values(1, a1), p.values(1, a2)));
        PlanNode inner2 = p.union(
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(inner2Out, a3)
                        .put(inner2Out, a4)
                        .build(),
                ImmutableList.of(p.values(1, a3), p.values(1, a4)));
        PlanNode outer = p.union(
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(outerOut, inner1Out)
                        .put(outerOut, inner2Out)
                        .build(),
                ImmutableList.of(inner1, inner2));
        return p.limit(limit, outer);
    }

    private void assertOptimizedPlan(ImmutableSet<Rule<?>> rules, PlanCreator planCreator, PlanMatchPattern pattern)
    {
        PlanTester planTester = getPlanTester();
        planTester.inTransaction(session -> {
            session.getCatalog().ifPresent(catalog -> planTester.getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            PlanBuilder planBuilder = new PlanBuilder(idAllocator, planTester.getPlannerContext(), session);
            PlanNode plan = planCreator.create(planBuilder);

            PlanOptimizer optimizer = new IterativeOptimizer(
                    "Test",
                    planTester.getPlannerContext(),
                    new RuleStatsRecorder(),
                    planTester.getStatsCalculator(),
                    planTester.getCostCalculator(),
                    rules);

            SymbolAllocator symbolAllocator = new SymbolAllocator();

            PlanNode optimized = optimizer.optimize(
                    plan,
                    new PlanOptimizer.Context(
                            session,
                            symbolAllocator,
                            idAllocator,
                            WarningCollector.NOOP,
                            createPlanOptimizersStatsCollector(),
                            new CachingTableStatsProvider(planTester.getPlannerContext().getMetadata(), session, () -> false),
                            RuntimeInfoProvider.noImplementation()));

            Plan actual = new Plan(optimized, StatsAndCosts.empty());
            PlanAssert.assertPlan(
                    session,
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    planTester.getStatsCalculator(),
                    actual,
                    pattern);
            return null;
        });
    }

    @FunctionalInterface
    private interface PlanCreator
    {
        PlanNode create(PlanBuilder planBuilder);
    }
}
