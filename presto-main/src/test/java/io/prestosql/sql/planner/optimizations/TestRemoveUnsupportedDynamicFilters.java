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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.PlanAssert;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.sanity.DynamicFiltersChecker;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.DynamicFilters.createDynamicFilterExpression;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.combineDisjuncts;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

@Test(singleThreaded = true) // e.g. PlanBuilder is mutable
public class TestRemoveUnsupportedDynamicFilters
        extends BasePlanTest
{
    private Metadata metadata;
    private TypeOperators typeOperators = new TypeOperators();
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private TableHandle lineitemTableHandle;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        CatalogName catalogName = getCurrentConnectorId();
        lineitemTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("lineitem", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        lineitemOrderKeySymbol = builder.symbol("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeySymbol), ImmutableMap.of(lineitemOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
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
                join(
                        INNER,
                        ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                        ImmutableMap.of(),
                        PlanMatchPattern.filter(
                                expression("ORDERS_OK > 0"),
                                TRUE_LITERAL,
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
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
                join(
                        INNER,
                        ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                        ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                        PlanMatchPattern.filter(
                                TRUE_LITERAL,
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, new SymbolReference("ORDERS_OK")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));
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
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                filter(
                                        expression("LINEITEM_OK > 0"),
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
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
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                ImmutableMap.of(),
                                filter(
                                        expression("LINEITEM_OK > 0"),
                                        values("LINEITEM_OK")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))));
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
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineDisjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NOT NULL"),
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
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
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
                                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                                        combineConjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NOT NULL"),
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
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                filter(
                                        combineDisjuncts(
                                                metadata,
                                                expression("LINEITEM_OK IS NULL"),
                                                expression("LINEITEM_OK IS NOT NULL")),
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
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
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, expression("CAST(LINEITEM_DOUBLE_OK AS BIGINT)")),
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
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_DOUBLE_OK", "ORDERS_OK")),
                                ImmutableMap.of(),
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_DOUBLE_OK", "orderkey")),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))));
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
                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, expression("LEFT_SYMBOL + RIGHT_SYMBOL"))));
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
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
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
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
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
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
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
            PlanNode rewrittenPlan = new RemoveUnsupportedDynamicFilters(metadata).optimize(root, session, builder.getTypes(), new SymbolAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP);
            new DynamicFiltersChecker().validate(rewrittenPlan,
                    session,
                    metadata,
                    typeOperators,
                    new TypeAnalyzer(new SqlParser(), metadata),
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
            PlanAssert.assertPlan(session, metadata, getQueryRunner().getStatsCalculator(), new Plan(actual, builder.getTypes(), StatsAndCosts.empty()), pattern);
            return null;
        });
    }
}
