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
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.OperatorType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
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
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.combineDisjuncts;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestRemoveUnsupportedDynamicFilters
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private PlannerContext plannerContext;
    private Metadata metadata;
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private TableHandle lineitemTableHandle;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;

    @BeforeAll
    public void setup()
    {
        plannerContext = getPlanTester().getPlannerContext();
        metadata = plannerContext.getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), plannerContext, TEST_SESSION);
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
                builder.filter(
                        new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                        ordersTableScanNode),
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
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                                        TRUE,
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                        .right(
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testDynamicFilterConsumedOnBuildSide()
    {
        Expression dynamicFilter = createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference());
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
                        .dynamicFilter(BIGINT, "ORDERS_OK", "LINEITEM_OK")
                        .left(
                                PlanMatchPattern.filter(
                                        TRUE,
                                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, new Reference(BIGINT, "ORDERS_OK")),
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
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
                                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
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
                                                new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
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
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
                                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
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
                                                new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
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
                                        combineDisjuncts(
                                                new IsNull(new Reference(BIGINT, "LINEITEM_OK")),
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineDisjuncts(
                                                new Not(new IsNull(new Reference(BIGINT, "LINEITEM_OK"))),
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference()))),
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
                                        combineConjuncts(
                                                new IsNull(new Reference(BIGINT, "LINEITEM_OK")),
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineConjuncts(
                                                new Not(new IsNull(new Reference(BIGINT, "LINEITEM_OK"))),
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference()))),
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
                                                        new IsNull(new Reference(BIGINT, "LINEITEM_OK")),
                                                        new Not(new IsNull(new Reference(BIGINT, "LINEITEM_OK")))),
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
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, new Cast(new Reference(DOUBLE, "LINEITEM_DOUBLE_OK"), BIGINT)),
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
                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "LEFT_SYMBOL"), new Reference(BIGINT, "RIGHT_SYMBOL"))))));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                output(
                        spatialJoin(
                                TRUE,
                                values("LEFT_SYMBOL"),
                                values("RIGHT_SYMBOL"))));
    }

    @Test
    public void testUnconsumedDynamicFilterInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                        ordersTableScanNode),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol(UNKNOWN, "SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
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
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                        lineitemTableScanNode),
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol(UNKNOWN, "SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                        filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "LINEITEM_OK"), new Constant(INTEGER, 0L)),
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testUnmatchedDynamicFilterInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                        ordersTableScanNode),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol(UNKNOWN, "SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    @Test
    public void testRemoveDynamicFilterNotAboveTableScanWithSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                        builder.values(ordersOrderKeySymbol)),
                lineitemTableScanNode,
                ordersOrderKeySymbol,
                lineitemOrderKeySymbol,
                new Symbol(UNKNOWN, "SEMIJOIN_OUTPUT"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DynamicFilterId("DF")));

        assertPlan(
                removeUnsupportedDynamicFilters(root),
                semiJoin("ORDERS_OK", "LINEITEM_OK", "SEMIJOIN_OUTPUT", false,
                        filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "ORDERS_OK"), new Constant(INTEGER, 0L)),
                                values("ORDERS_OK")),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
    }

    private static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source)
    {
        // assert explicitly that no dynamic filters are present
        return PlanMatchPattern.filter(expectedPredicate, TRUE, source);
    }

    private PlanNode removeUnsupportedDynamicFilters(PlanNode root)
    {
        return getPlanTester().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanNode rewrittenPlan = new RemoveUnsupportedDynamicFilters(plannerContext).optimize(
                    root,
                    new PlanOptimizer.Context(
                            session,
                            new SymbolAllocator(),
                            new PlanNodeIdAllocator(),
                            WarningCollector.NOOP,
                            createPlanOptimizersStatsCollector(),
                            new CachingTableStatsProvider(metadata, session),
                            RuntimeInfoProvider.noImplementation()));
            new DynamicFiltersChecker().validate(rewrittenPlan,
                    session,
                    plannerContext,
                    WarningCollector.NOOP);
            return rewrittenPlan;
        });
    }

    protected void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        getPlanTester().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(
                    session,
                    metadata,
                    getPlanTester().getPlannerContext().getFunctionManager(),
                    getPlanTester().getStatsCalculator(),
                    new Plan(actual, StatsAndCosts.empty()),
                    pattern);
            return null;
        });
    }
}
