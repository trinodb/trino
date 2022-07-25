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
package io.trino.sql.planner.sanity;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.combineDisjuncts;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDynamicFiltersChecker
        extends BasePlanTest
{
    private Metadata metadata;
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;
    private PlannerContext plannerContext;

    @BeforeClass
    public void setup()
    {
        plannerContext = getQueryRunner().getPlannerContext();
        metadata = plannerContext.getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata, TEST_SESSION);
        CatalogName catalogName = getCurrentConnectorId();
        TableHandle lineitemTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("sf1", "lineitem", 1.0),
                TestingTransactionHandle.create());
        lineitemOrderKeySymbol = builder.symbol("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeySymbol), ImmutableMap.of(lineitemOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                catalogName,
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("Dynamic filters \\[DF\\] present in join were not fully consumed by it's probe side.");
    }

    @Test
    public void testDynamicFilterConsumedOnBuildSide()
    {
        PlanNode root = builder.join(
                INNER,
                builder.filter(
                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference()),
                        ordersTableScanNode),
                builder.filter(
                        createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference()),
                        lineitemTableScanNode),
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ImmutableList.of(ordersOrderKeySymbol),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(new DynamicFilterId("DF"), lineitemOrderKeySymbol));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("Dynamic filters \\[DF\\] present in join were consumed by it's build side.");
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
                        ImmutableList.of(ordersOrderKeySymbol),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("All consumed dynamic filters could not be matched with a join/semi-join.");
    }

    @Test
    public void testDynamicFilterNotAboveTableScan()
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("Dynamic filters \\[Descriptor\\{id=DF, input=\"ORDERS_OK\", operator=EQUAL, nullAllowed=false\\}\\] present in filter predicate whose source is not a table scan.");
    }

    @Test
    public void testUnmatchedNestedDynamicFilter()
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("All consumed dynamic filters could not be matched with a join/semi-join.");
    }

    @Test
    public void testUnsupportedDynamicFilterExpression()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, expression("LINEITEM_OK + BIGINT'1'")),
                                lineitemTableScanNode),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(lineitemOrderKeySymbol),
                        ImmutableList.of(ordersOrderKeySymbol),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("Dynamic filter expression \\(\"LINEITEM_OK\" \\+ BIGINT '1'\\) must be a SymbolReference or a CAST of SymbolReference.");
    }

    @Test
    public void testUnsupportedCastExpression()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, expression("CAST(CAST(LINEITEM_OK AS INT) AS BIGINT)")),
                                lineitemTableScanNode),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(lineitemOrderKeySymbol),
                        ImmutableList.of(ordersOrderKeySymbol),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        assertThatThrownBy(() -> validatePlan(root)).isInstanceOf(VerifyException.class).hasMessageMatching("The expression CAST\\(\"LINEITEM_OK\" AS INT\\) within in a CAST in dynamic filter must be a SymbolReference.");
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessage("The dynamic filter DF present in semi-join was not consumed by it's source side.");
    }

    @Test
    public void testDynamicFilterConsumedOnFilteringSourceSideInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                metadata,
                                expression("ORDERS_OK > 0"),
                                createDynamicFilterExpression(TEST_SESSION, metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                        ordersTableScanNode),
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessage("The dynamic filter DF present in semi-join was consumed by it's filtering source side.");
    }

    @Test
    public void testUnmatchedDynamicFilterInSemiJoin()
    {
        PlanNode root = builder.output(
                ImmutableList.of(),
                ImmutableList.of(),
                builder.semiJoin(
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
                        Optional.empty()));
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessage("All consumed dynamic filters could not be matched with a join/semi-join.");
    }

    @Test
    public void testDynamicFilterNotAboveTableScanWithSemiJoin()
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
        assertThatThrownBy(() -> validatePlan(root))
                .isInstanceOf(VerifyException.class)
                .hasMessageMatching("Dynamic filters \\[Descriptor\\{id=DF, input=\"ORDERS_OK\", operator=EQUAL, nullAllowed=false\\}\\] present in filter predicate whose source is not a table scan.");
    }

    private void validatePlan(PlanNode root)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new DynamicFiltersChecker().validate(root, session, plannerContext, createTestingTypeAnalyzer(plannerContext), TypeProvider.empty(), WarningCollector.NOOP);
            return null;
        });
    }
}
