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
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.sanity.DynamicFiltersChecker;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.combineDisjuncts;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

@Test(singleThreaded = true) // e.g. PlanBuilder is mutable
public class TestRemoveUnsupportedDynamicFilters
        extends BasePlanTest
{
    private PlannerContext plannerContext;
    private Metadata metadata;
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private TableHandle lineitemTableHandle;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;

    @BeforeClass
    public void setup()
    {
        plannerContext = getQueryRunner().getPlannerContext();
        metadata = plannerContext.getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata, TEST_SESSION);
        CatalogHandle catalogHandle = getCurrentCatalogHandle();
        lineitemTableHandle = new TableHandle(
                catalogHandle,
                new TpchTableHandle("sf1", "lineitem", 1.0),
                TestingTransactionHandle.create());
        lineitemOrderKeySymbol = builder.symbol("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeySymbol), ImmutableMap.of(lineitemOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                catalogHandle,
                new TpchTableHandle("sf1", "orders", 1.0),
                TestingTransactionHandle.create());
        ordersOrderKeySymbol = builder.symbol("ORDERS_OK", BIGINT);
        ordersTableScanNode = builder.tableScan(ordersTableHandle, ImmutableList.of(ordersOrderKeySymbol), ImmutableMap.of(ordersOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));
    }

    @Test
    public void testUnconsumedDynamicFilterInJoin()
    {
        PlanNode root = builder.join(
                INNER,
                builder.filter(expression("ORDERS_OK > 0"), ordersTableScanNode),
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ImmutableList.of(ordersOrderKeySymbol),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(new DynamicFilterId("DF"), lineitemOrderKeySymbol));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                join(INNER, builder -> builder
                        .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                        .left(
                                PlanMatchPattern.filter(
                                        expression("ORDERS_OK > 0"),
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                        .right(
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testDynamicFilterConsumedOnBuildSide()
    {
        Expression dynamicFilter = createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference());
        PlanNode root = builder.join(
                INNER,
                builder.filter(
                        dynamicFilter,
                        ordersTableScanNode),
                builder.filter(
                        dynamicFilter,
                        lineitemTableScanNode),
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ImmutableList.of(ordersOrderKeySymbol),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(new DynamicFilterId("DF"), lineitemOrderKeySymbol));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                join(INNER, builder -> builder
                        .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                        .dynamicFilter("ORDERS_OK", "LINEITEM_OK")
                        .left(
                                PlanMatchPattern.filter(
                                        TRUE_LITERAL,
                                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, new SymbolReference("ORDERS_OK")),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                        .right(
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testUnmatchedDynamicFilter()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                combineConjuncts(
                                        metadata,
                                        expression("LINEITEM_OK > 0"),
                                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(
                                        filter(
                                                expression("LINEITEM_OK > 0"),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRemoveDynamicFilterNotAboveTableScan()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                combineConjuncts(
                                        metadata,
                                        expression("LINEITEM_OK > 0"),
                                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                                builder.values(lineitemOrderKeySymbol)),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                .left(
                                        filter(
                                                expression("LINEITEM_OK > 0"),
                                                values("LINEITEM_OK")))
                                .right(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))));
    }

    @Test
    public void testNestedDynamicFilterDisjunctionRewrite()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                combineConjuncts(
                                        metadata,
                                        combineDisjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NULL"),
                                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineDisjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NOT NULL"),
                                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference()))),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                        ImmutableList.of(ordersOrderKeySymbol),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testNestedDynamicFilterConjunctionRewrite()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        ordersTableScanNode,
                        builder.filter(
                                combineDisjuncts(
                                        metadata,
                                        combineConjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NULL"),
                                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineConjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NOT NULL"),
                                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference()))),
                                lineitemTableScanNode),
                        ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                        ImmutableList.of(ordersOrderKeySymbol),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(
                                        filter(
                                                combineDisjuncts(
                                                        metadata,
                                                        expression("LINEITEM_OK IS NULL"),
                                                        expression("LINEITEM_OK IS NOT NULL")),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRemoveUnsupportedCast()
    {
        // Use lineitem with DOUBLE orderkey to simulate the case of implicit casts
        // Dynamic filter is removed because there isn't an injective mapping from bigint -> double
        Symbol lineitemDoubleOrderKeySymbol = builder.symbol("LINEITEM_DOUBLE_OK", DOUBLE);
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, expression("CAST(LINEITEM_DOUBLE_OK AS BIGINT)")),
                                builder.tableScan(
                                        lineitemTableHandle,
                                        ImmutableList.of(lineitemDoubleOrderKeySymbol),
                                        ImmutableMap.of(lineitemDoubleOrderKeySymbol, new TpchColumnHandle("orderkey", DOUBLE)))),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemDoubleOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(lineitemDoubleOrderKeySymbol),
                        ImmutableList.of(ordersOrderKeySymbol),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_DOUBLE_OK", "ORDERS_OK")
                                .left(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_DOUBLE_OK", "orderkey")))
                                .right(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))));
    }

    @Test
    public void testSpatialJoin()
    {
        Symbol leftSymbol = builder.symbol("LEFT_SYMBOL", BIGINT);
        Symbol rightSymbol = builder.symbol("RIGHT_SYMBOL", BIGINT);
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.spatialJoin(
                        SpatialJoinNode.Type.INNER,
                        builder.values(leftSymbol),
                        builder.values(rightSymbol),
                        ImmutableList.of(leftSymbol, rightSymbol),
                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, expression("LEFT_SYMBOL + RIGHT_SYMBOL"))));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        spatialJoin(
                                "true",
                                values("LEFT_SYMBOL"),
                                values("RIGHT_SYMBOL"))));
    }

    @Test
    public void testUnconsumedDynamicFilterInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(expression("ORDERS_OK > 0"), ordersTableScanNode),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol("SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(expression("ORDERS_OK > 0"),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    @Test
    public void testDynamicFilterConsumedOnFilteringSourceSideInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                ordersTableScanNode,
                builder.filter(
                        combineConjuncts(
                                metadata,
                                expression("LINEITEM_OK > 0"),
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                        lineitemTableScanNode),
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol("SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                        filter(expression("LINEITEM_OK > 0"),
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testUnmatchedDynamicFilterInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                metadata,
                                expression("ORDERS_OK > 0"),
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                        ordersTableScanNode),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol("SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(expression("ORDERS_OK > 0"),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    @Test
    public void testRemoveDynamicFilterNotAboveTableScanWithSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                metadata,
                                expression("ORDERS_OK > 0"),
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                        builder.values(ordersOrderKeySymbol)),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol("SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));

        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(expression("ORDERS_OK > 0"),
                                values("ORDERS_OK")),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    private static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source)
    {
        // assert explicitly that no dynamic filters are present
        return PlanMatchPattern.filter(expectedPredicate, TRUE_LITERAL, source);
    }

    private PlanNode removeUnsupportedDynamicFilters(PlanNode root)
    {
        return getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanNode rewrittenPlan = new RemoveUnsupportedDynamicFilters(plannerContext).optimize(root,
                    session,
                    builder.getTypes(),
                    new SymbolAllocator(),
                    new PlanNodeIdAllocator(),
                    WarningCollector.NOOP,
                    new CachingTableStatsProvider(metadata, session));
            new DynamicFiltersChecker().validate(rewrittenPlan,
                    session,
                    plannerContext, createTestingTypeAnalyzer(plannerContext),
                    builder.getTypes(),
                    WarningCollector.NOOP);
            return rewrittenPlan;
        });
    }

    protected void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(
                    session,
                    metadata,
                    getQueryRunner().getFunctionManager(),
                    getQueryRunner().getStatsCalculator(),
                    new Plan(actual, builder.getTypes(), StatsAndCosts.empty()),
                    pattern);
            return null;
        });
    }
}
