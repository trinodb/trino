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
package io.prestosql.sql.planner.sanity;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.DynamicFilters.createDynamicFilterExpression;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.combineDisjuncts;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestDynamicFiltersChecker
        extends BasePlanTest
{
    private Metadata metadata;
    private TypeOperators typeOperators = new TypeOperators();
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        CatalogName catalogName = getCurrentConnectorId();
        TableHandle lineitemTableHandle = new TableHandle(
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

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Dynamic filters \\[DF\\] present in join were not fully consumed by it's probe side.")
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
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Dynamic filters \\[DF\\] present in join were consumed by it's build side.")
    public void testDynamicFilterConsumedOnBuildSide()
    {
        PlanNode root = builder.join(
                INNER,
                builder.filter(
                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference()),
                        ordersTableScanNode),
                builder.filter(
                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference()),
                        lineitemTableScanNode),
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ImmutableList.of(ordersOrderKeySymbol),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(new DynamicFilterId("DF"), lineitemOrderKeySymbol));
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "All consumed dynamic filters could not be matched with a join/semi-join.")
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
                        ImmutableList.of(ordersOrderKeySymbol),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()));
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Dynamic filters \\[Descriptor\\{id=DF, input=\"ORDERS_OK\", operator=EQUAL\\}\\] present in filter predicate whose source is not a table scan.")
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
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "All consumed dynamic filters could not be matched with a join/semi-join.")
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
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Dynamic filter expression \\(\"LINEITEM_OK\" \\+ BIGINT '1'\\) must be a SymbolReference or a CAST of SymbolReference.")
    public void testUnsupportedDynamicFilterExpression()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, expression("LINEITEM_OK + BIGINT'1'")),
                                lineitemTableScanNode),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(lineitemOrderKeySymbol),
                        ImmutableList.of(ordersOrderKeySymbol),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "The expression CAST\\(\"LINEITEM_OK\" AS INT\\) within in a CAST in dynamic filter must be a SymbolReference.")
    public void testUnsupportedCastExpression()
    {
        PlanNode root = builder.output(ImmutableList.of(), ImmutableList.of(),
                builder.join(
                        INNER,
                        builder.filter(
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, expression("CAST(CAST(LINEITEM_OK AS INT) AS BIGINT)")),
                                lineitemTableScanNode),
                        ordersTableScanNode,
                        ImmutableList.of(new JoinNode.EquiJoinClause(lineitemOrderKeySymbol, ordersOrderKeySymbol)),
                        ImmutableList.of(lineitemOrderKeySymbol),
                        ImmutableList.of(ordersOrderKeySymbol),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), ordersOrderKeySymbol)));
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "The dynamic filter DF present in semi-join was not consumed by it's source side.")
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
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "The dynamic filter DF present in semi-join was consumed by it's filtering source side.")
    public void testDynamicFilterConsumedOnFilteringSourceSideInSemiJoin()
    {
        PlanNode root = builder.semiJoin(
                builder.filter(
                        combineConjuncts(
                                metadata,
                                expression("ORDERS_OK > 0"),
                                createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, lineitemOrderKeySymbol.toSymbolReference())),
                        ordersTableScanNode),
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
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "All consumed dynamic filters could not be matched with a join/semi-join.")
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
                                        createDynamicFilterExpression(metadata, new DynamicFilterId("DF"), BIGINT, ordersOrderKeySymbol.toSymbolReference())),
                                ordersTableScanNode),
                        lineitemTableScanNode,
                        ordersOrderKeySymbol,
                        lineitemOrderKeySymbol,
                        new Symbol("SEMIJOIN_OUTPUT"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
        validatePlan(root);
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Dynamic filters \\[Descriptor\\{id=DF, input=\"ORDERS_OK\", operator=EQUAL\\}\\] present in filter predicate whose source is not a table scan.")
    public void testDynamicFilterNotAboveTableScanWithSemiJoin()
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
        validatePlan(root);
    }

    private void validatePlan(PlanNode root)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new DynamicFiltersChecker().validate(root, session, metadata, typeOperators, new TypeAnalyzer(new SqlParser(), metadata), TypeProvider.empty(), WarningCollector.NOOP);
            return null;
        });
    }
}
