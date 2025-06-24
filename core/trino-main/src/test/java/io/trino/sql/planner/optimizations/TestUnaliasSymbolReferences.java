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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.BigintType;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;

public class TestUnaliasSymbolReferences
        extends BasePlanTest
{
    @Test
    public void testDynamicFilterIdUnAliased()
    {
        String probeTable = "supplier";
        String buildTable = "nation";
        assertOptimizedPlan(
                new UnaliasSymbolReferences(),
                (p, session, metadata) -> {
                    ColumnHandle column = new TpchColumnHandle("nationkey", BIGINT);
                    Symbol buildColumnSymbol = p.symbol("nationkey");
                    Symbol buildAlias1 = p.symbol("buildAlias1");
                    Symbol buildAlias2 = p.symbol("buildAlias2");
                    Symbol probeColumn1 = p.symbol("s_nationkey");
                    Symbol probeColumn2 = p.symbol("s_suppkey");
                    DynamicFilterId dynamicFilterId1 = new DynamicFilterId("df1");
                    DynamicFilterId dynamicFilterId2 = new DynamicFilterId("df2");

                    return p.join(
                            INNER,
                            p.filter(
                                    TRUE, // additional filter to test recursive call
                                    p.filter(
                                            and(
                                                    dynamicFilterExpression(metadata, probeColumn1, dynamicFilterId1),
                                                    dynamicFilterExpression(metadata, probeColumn2, dynamicFilterId2)),
                                            p.tableScan(
                                                    tableHandle(probeTable),
                                                    ImmutableList.of(probeColumn1, probeColumn2),
                                                    ImmutableMap.of(
                                                            probeColumn1, new TpchColumnHandle("nationkey", BIGINT),
                                                            probeColumn2, new TpchColumnHandle("suppkey", BIGINT))))),
                            p.project(
                                    Assignments.of(buildAlias1, buildColumnSymbol.toSymbolReference(), buildAlias2, buildColumnSymbol.toSymbolReference()),
                                    p.tableScan(tableHandle(buildTable), ImmutableList.of(buildColumnSymbol), ImmutableMap.of(buildColumnSymbol, column))),
                            ImmutableList.of(),
                            ImmutableList.of(),
                            ImmutableList.of(buildAlias1, buildAlias2),
                            Optional.empty(),
                            ImmutableMap.of(dynamicFilterId1, buildAlias1, dynamicFilterId2, buildAlias2));
                },
                join(INNER, builder -> builder
                        .dynamicFilter(ImmutableMap.of(
                                new Reference(BIGINT, "probeColumn1"), "column",
                                new Reference(BIGINT, "probeColumn2"), "column"))
                        .left(
                                filter(
                                        TRUE,
                                        filter(
                                                TRUE,
                                                tableScan(
                                                        probeTable,
                                                        ImmutableMap.of("probeColumn1", "suppkey", "probeColumn2", "nationkey")))))
                        .right(
                                project(tableScan(buildTable, ImmutableMap.of("column", "nationkey"))))));
    }

    @Test
    public void testGroupIdGroupingSetsDeduplicated()
    {
        assertOptimizedPlan(
                new UnaliasSymbolReferences(),
                (p, session, metadata) -> {
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

            SymbolAllocator symbolAllocator = new SymbolAllocator();
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

    private Expression dynamicFilterExpression(Metadata metadata, Symbol symbol, DynamicFilterId id)
    {
        return createDynamicFilterExpression(metadata, id, BigintType.BIGINT, symbol.toSymbolReference());
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
