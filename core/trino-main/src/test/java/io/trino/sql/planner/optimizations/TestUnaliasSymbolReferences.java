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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;

public class TestUnaliasSymbolReferences
        extends BasePlanTest
{
    @Test
    public void testGroupIdGroupingSetsDeduplicated()
    {
        assertOptimizedPlan(
                new UnaliasSymbolReferences(),
                (p, _, _) -> {
                    Symbol symbol = p.symbol("symbol");
                    Symbol alias1 = p.symbol("alias1");
                    Symbol alias2 = p.symbol("alias2");

                    return p.groupId(ImmutableList.of(ImmutableList.of(alias1, alias2)),
                            ImmutableList.of(),
                            p.symbol("groupId"),
                            p.project(
                                    Assignments.of(alias1, symbol.toSymbolReference(), alias2, symbol.toSymbolReference()),
                                    p.values(symbol)));
                },
                groupId(
                        ImmutableList.of(ImmutableList.of("symbol")),
                        "groupId",
                        project(values("symbol"))));
    }

    private void assertOptimizedPlan(PlanOptimizer optimizer, PlanCreator planCreator, PlanMatchPattern pattern)
    {
        PlanTester planTester = getPlanTester();
        planTester.inTransaction(session -> {
            Metadata metadata = planTester.getPlannerContext().getMetadata();
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            PlanBuilder planBuilder = new PlanBuilder(idAllocator, planTester.getPlannerContext(), session);

            SymbolAllocator symbolAllocator = emptySymbolAllocator();
            PlanNode plan = planCreator.create(planBuilder, session, metadata);
            PlanNode optimized = optimizer.optimize(
                    plan,
                    new PlanOptimizer.Context(
                            session,
                            symbolAllocator,
                            idAllocator,
                            WarningCollector.NOOP,
                            createPlanOptimizersStatsCollector(),
                            new CachingTableStatsProvider(metadata, session, () -> false),
                            RuntimeInfoProvider.noImplementation()));

            Plan actual = new Plan(optimized, StatsAndCosts.empty());
            PlanAssert.assertPlan(session, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), planTester.getStatsCalculator(), actual, pattern);
            return null;
        });
    }

    private TableHandle tableHandle(String tableName)
    {
        return getPlanTester().getTableHandle(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, tableName);
    }

    interface PlanCreator
    {
        PlanNode create(PlanBuilder planBuilder, Session session, Metadata metadata);
    }
}
