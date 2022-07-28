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
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;

@Test(singleThreaded = true)
public class TestSkewJoinOptimizer
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

    public TestSkewJoinOptimizer()
    {
        super(ImmutableMap.of(
                "skewed_join_metadata", String.format("[[\"%s.sf1.orders\", \"orderkey\", \"11\", \"42\"]]", TEST_CATALOG_HANDLE.getCatalogName())));
    }

    @BeforeClass
    public void setup()
    {
        plannerContext = getQueryRunner().getPlannerContext();
        metadata = plannerContext.getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata, TEST_SESSION);
        lineitemTableHandle = new TableHandle(
                TEST_CATALOG_HANDLE,
                new TpchTableHandle("sf1", "lineitem", 1.0),
                TestingTransactionHandle.create());
        lineitemOrderKeySymbol = builder.symbol("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeySymbol), ImmutableMap.of(lineitemOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                TEST_CATALOG_HANDLE,
                new TpchTableHandle("sf1", "orders", 1.0),
                TestingTransactionHandle.create());
        ordersOrderKeySymbol = builder.symbol("ORDERS_OK", BIGINT);
        ordersTableScanNode = builder.tableScan(ordersTableHandle, ImmutableList.of(ordersOrderKeySymbol), ImmutableMap.of(ordersOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));
    }

    @Test
    public void testSkewJoinOptimizer()
    {
        PlanNode actualQuery = builder.join(
                JoinNode.Type.INNER,
                ordersTableScanNode,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of());

        assertPlan(optimizeSkewedJoins(actualQuery),
                project(
                        ImmutableMap.of("ORDERS_OK", expression("ORDERS_OK")),
                        join(
                                JoinNode.Type.INNER, builder -> builder
                                        .equiCriteria(ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK"), equiJoinClause("randPart", "skewPart")))
                                        .left(
                                                project(ImmutableMap.of("randPart", expression("IF((\"ORDERS_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), random(2), TINYINT '0')"), "ORDERS_OK", expression("ORDERS_OK")),
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                        .right(
                                                unnest(
                                                        ImmutableList.of("LINEITEM_OK"),
                                                        ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                                        project(ImmutableMap.of("skewPartitioner", expression("IF((\"LINEITEM_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), \"$array\"(TINYINT '0', TINYINT '1', TINYINT '2'), \"$array\"(TINYINT '0'))"), "LINEITEM_OK", expression("LINEITEM_OK")),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testSkewJoinOptimizerPassesThroughNodes()
    {
        PlanNode leftSort = builder.sort(ImmutableList.of(ordersOrderKeySymbol), ordersTableScanNode);

        PlanNode rightFilterProject = builder.filter(PlanBuilder.expression("LINEITEM_OK > 10"), builder.project(
                Assignments.of(builder.symbol("LINEITEM_OK"), PlanBuilder.expression("orderkey")),
                lineitemTableScanNode));

        PlanNode actualQuery = builder.join(
                JoinNode.Type.INNER,
                leftSort,
                rightFilterProject,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of());

        PlanMatchPattern expectedLeftSort = sort(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")));
        PlanMatchPattern expectedRightFilterProject = filter("LINEITEM_OK > 10", project(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))));

        assertPlan(optimizeSkewedJoins(actualQuery),
                project(
                        ImmutableMap.of("ORDERS_OK", expression("ORDERS_OK")),
                        join(JoinNode.Type.INNER, builder -> builder
                                .equiCriteria(ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK"), equiJoinClause("randPart", "skewPart")))
                                .left(
                                        project(ImmutableMap.of("randPart", expression("IF((\"ORDERS_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), random(2), TINYINT '0')"), "ORDERS_OK", expression("ORDERS_OK")),
                                                expectedLeftSort))
                                .right(
                                        unnest(
                                                ImmutableList.of("LINEITEM_OK"),
                                                ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                                project(ImmutableMap.of("skewPartitioner", expression("IF((\"LINEITEM_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), \"$array\"(TINYINT '0', TINYINT '1', TINYINT '2'), \"$array\"(TINYINT '0'))"), "LINEITEM_OK", expression("LINEITEM_OK")),
                                                        expectedRightFilterProject))))));
    }

    @Test
    public void testSkewJoinOptimizerReplicatedJoinsNotOptimized()
    {
        PlanNode actualQuery = builder.join(
                JoinNode.Type.INNER,
                ordersTableScanNode,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.REPLICATED),
                ImmutableMap.of());

        assertPlan(optimizeSkewedJoins(actualQuery),
                join(JoinNode.Type.INNER, builder -> builder
                        .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                        .left(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                        .right(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testSkewJoinOptimizerRightJoinsNotOptimized()
    {
        PlanNode actualQuery = builder.join(
                JoinNode.Type.RIGHT,
                ordersTableScanNode,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of());

        assertPlan(optimizeSkewedJoins(actualQuery),
                join(JoinNode.Type.RIGHT, builder -> builder
                        .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                        .left(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                        .right(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    @Test
    public void testSkewJoinOptimizerNestedJoin()
    {
        PlanNode childJoin = builder.join(
                JoinNode.Type.INNER,
                ordersTableScanNode,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of());

        PlanNode actualQuery = builder.join(
                JoinNode.Type.INNER,
                childJoin,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                childJoin.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of());

        assertPlan(optimizeSkewedJoins(actualQuery),
                join(JoinNode.Type.INNER, builder -> builder
                        .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                        .left(
                                project(
                                        ImmutableMap.of("ORDERS_OK", expression("ORDERS_OK")),
                                        join(JoinNode.Type.INNER, childBuilder -> childBuilder
                                                .equiCriteria(ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK"), equiJoinClause("randPart", "skewPart")))
                                                .left(
                                                        project(ImmutableMap.of("randPart", expression("IF((\"ORDERS_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), random(2), TINYINT '0')"), "ORDERS_OK", expression("ORDERS_OK")),
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                                .right(
                                                        unnest(
                                                                ImmutableList.of("LINEITEM_OK"),
                                                                ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                                                project(ImmutableMap.of("skewPartitioner", expression("IF((\"LINEITEM_OK\" IN (CAST('11' AS bigint), CAST('42' AS bigint))), \"$array\"(TINYINT '0', TINYINT '1', TINYINT '2'), \"$array\"(TINYINT '0'))"), "LINEITEM_OK", expression("LINEITEM_OK")),
                                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))))
                        .right(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))));
    }

    private PlanNode optimizeSkewedJoins(PlanNode root)
    {
        return getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            return new SkewJoinOptimizer(metadata).optimize(root, session, builder.getTypes(), new SymbolAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP, new CachingTableStatsProvider(metadata, session));
        });
    }

    void assertPlan(PlanNode actual, PlanMatchPattern pattern)
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
