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

package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogName;
import io.trino.execution.TableInfo;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.planprinter.anonymize.AggregationNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.AnonymizePlanPrinter;
import io.trino.sql.planner.planprinter.anonymize.AnonymizedNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.ExchangeNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.FilterNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.JoinNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.ProjectNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.RemoteSourceNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.SemiJoinNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.SortNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.TableScanNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.TopNNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.ValuesNodeRepresentation;
import io.trino.sql.planner.planprinter.anonymize.WindowNodeRepresentation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WindowFrame;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.planprinter.anonymize.AggregationNodeRepresentation.AggregationRepresentation;
import static io.trino.sql.planner.planprinter.anonymize.TableScanNodeRepresentation.DomainRepresentation;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnonymizePlanPrinter
{
    private static final TableInfo TABLE_INFO = new TableInfo(
            Optional.of("tpch"),
            new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"),
            TupleDomain.all());
    private static final ValuesNodeRepresentation VALUE_A_REPRESENTATION = new ValuesNodeRepresentation(
            new PlanNodeId("0"),
            ImmutableList.of(typedSymbol("symbol_97", "bigint")),
            0,
            Optional.of(ImmutableList.of()));
    private static final ResolvedFunction MIN_FUNCTION = new TestingFunctionResolution().resolveFunction(QualifiedName.of("min"), fromTypes(BIGINT));

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @Test
    public void testAggregationNodeAnonymization()
    {
        assertAnonymizedPlan(
                pb -> pb.aggregation(ab -> ab
                        .step(FINAL)
                        .addAggregation(pb.symbol("sum", BIGINT), expression("sum(x)"), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))),
                new AggregationNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(
                                typedSymbol("symbol_121", "bigint"),
                                typedSymbol("symbol_122", "bigint"),
                                typedSymbol("symbol_114251", "bigint")),
                        ImmutableList.of(
                                new ValuesNodeRepresentation(
                                        new PlanNodeId("0"),
                                        ImmutableList.of(
                                                typedSymbol("symbol_120", "bigint"),
                                                typedSymbol("symbol_121", "bigint"),
                                                typedSymbol("symbol_122", "bigint")),
                                        0,
                                        Optional.of(ImmutableList.of()))),
                        ImmutableMap.of(symbol("symbol_114251"), new AggregationRepresentation(
                                "sum",
                                ImmutableList.of("\"symbol_120\""),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())),
                        ImmutableList.of(symbol("symbol_121"), symbol("symbol_122")),
                        FINAL,
                        Optional.empty()));
    }

    @Test
    public void testJoinNodeAnonymization()
    {
        assertAnonymizedPlan(
                pb -> pb.join(
                        INNER,
                        pb.values(pb.symbol("a", BIGINT), pb.symbol("b", BIGINT)),
                        pb.values(pb.symbol("c", BIGINT), pb.symbol("d", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(pb.symbol("a", BIGINT), pb.symbol("d", BIGINT))),
                        ImmutableList.of(pb.symbol("b", BIGINT)),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), pb.symbol("d", BIGINT))),
                new JoinNodeRepresentation(
                        new PlanNodeId("2"),
                        ImmutableList.of(typedSymbol("symbol_98", "bigint")),
                        ImmutableList.of(
                                new ValuesNodeRepresentation(
                                        new PlanNodeId("0"),
                                        ImmutableList.of(
                                                typedSymbol("symbol_97", "bigint"),
                                                typedSymbol("symbol_98", "bigint")),
                                        0,
                                        Optional.of(ImmutableList.of())),
                                new ValuesNodeRepresentation(
                                        new PlanNodeId("1"),
                                        ImmutableList.of(
                                                typedSymbol("symbol_99", "bigint"),
                                                typedSymbol("symbol_100", "bigint")),
                                        0,
                                        Optional.of(ImmutableList.of()))),
                        INNER,
                        ImmutableList.of(new EquiJoinClause(symbol("symbol_97"), symbol("symbol_100"))),
                        false,
                        Optional.empty()));
    }

    @Test
    public void testTableScanNodeAnonymization()
    {
        ColumnHandle columnHandle = new ColumnHandle() {
            @Override
            public int hashCode()
            {
                return 1234;
            }

            @Override
            public boolean equals(Object obj)
            {
                return true;
            }
        };
        assertAnonymizedPlan(
                pb -> pb.tableScan(
                        new TableHandle(
                                new CatalogName("tpch"),
                                new TpchTableHandle(TINY_SCHEMA_NAME, "orders", TINY_SCALE_FACTOR),
                                TpchTransactionHandle.INSTANCE),
                        ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableMap.of(pb.symbol("a", BIGINT), columnHandle),
                        TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, Domain.singleValue(BIGINT, 1L)))),
                new TableScanNodeRepresentation(
                        new PlanNodeId("0"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        Optional.of("tpch"),
                        "catalog_3566561",
                        "schema_3560192",
                        "table_-1008770331",
                        ImmutableMap.of(symbol("symbol_97"), "column_1234"),
                        Optional.of(ImmutableMap.of("column_1234", new DomainRepresentation(
                                "SortedRangeSet",
                                "bigint",
                                Optional.of(1),
                                false,
                                false,
                                false,
                                true))),
                        Optional.of(ImmutableMap.of())));
    }

    @Test
    public void testExchangeNodeAnonymization()
    {
        assertAnonymizedPlan(
                pb -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    return pb.exchange(e -> e
                            .addSource(pb.values(a))
                            .addInputsSet(a)
                            .orderingScheme(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))
                            .singleDistributionPartitioningScheme(a));
                },
                new ExchangeNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        ImmutableList.of(VALUE_A_REPRESENTATION),
                        GATHER,
                        REMOTE,
                        ImmutableList.of(),
                        Optional.empty(),
                        false,
                        Optional.of(ImmutableMap.of(symbol("symbol_97"), ASC_NULLS_FIRST))));
    }

    @Test
    public void testFilterNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> pb.filter(
                        TRUE_LITERAL,
                        pb.values(pb.symbol("a", BIGINT))),
                new FilterNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        ImmutableList.of(VALUE_A_REPRESENTATION),
                        "true"));
    }

    @Test
    public void testProjectNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> pb.project(
                        Assignments.of(pb.symbol("x", BIGINT), expression("a")),
                        pb.values(pb.symbol("a", BIGINT))),
                new ProjectNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_120", "bigint")),
                        ImmutableList.of(VALUE_A_REPRESENTATION),
                        ImmutableMap.of(symbol("symbol_120"), "\"symbol_97\"")));
    }

    @Test
    public void testRemoteSourceNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    return pb.remoteSource(
                            ImmutableList.of(new PlanFragmentId("source")),
                            ImmutableList.of(a),
                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST))),
                            REPARTITION,
                            RetryPolicy.NONE);
                },
                new RemoteSourceNodeRepresentation(
                        new PlanNodeId("0"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        ImmutableList.of(new PlanFragmentId("source")),
                        Optional.of(ImmutableMap.of(symbol("symbol_97"), ASC_NULLS_FIRST)),
                        REPARTITION));
    }

    @Test
    public void testSemiJoinNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    Symbol b = pb.symbol("b", BIGINT);
                    Symbol c = pb.symbol("c", BIGINT);
                    return pb.semiJoin(
                            pb.values(a),
                            pb.values(b, c),
                            a,
                            c,
                            b,
                            Optional.of(a),
                            Optional.of(c),
                            Optional.of(REPLICATED));
                },
                new SemiJoinNodeRepresentation(
                        new PlanNodeId("2"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint"), typedSymbol("symbol_98", "bigint")),
                        ImmutableList.of(
                                VALUE_A_REPRESENTATION,
                                new ValuesNodeRepresentation(
                                        new PlanNodeId("1"),
                                        ImmutableList.of(typedSymbol("symbol_98", "bigint"), typedSymbol("symbol_99", "bigint")),
                                        0,
                                        Optional.of(ImmutableList.of()))),
                        symbol("symbol_97"),
                        symbol("symbol_99"),
                        Optional.of(symbol("symbol_97")),
                        Optional.of(symbol("symbol_99")),
                        Optional.of(REPLICATED)));
    }

    @Test
    public void testSortNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> pb.sort(ImmutableList.of(pb.symbol("a", BIGINT)), pb.values(pb.symbol("a", BIGINT))),
                new SortNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        ImmutableList.of(VALUE_A_REPRESENTATION),
                        ImmutableMap.of(symbol("symbol_97"), ASC_NULLS_FIRST),
                        false));
    }

    @Test
    public void testTopNNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> pb.topN(1, ImmutableList.of(pb.symbol("a", BIGINT)), TopNNode.Step.FINAL, pb.values(pb.symbol("a", BIGINT))),
                new TopNNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_97", "bigint")),
                        ImmutableList.of(VALUE_A_REPRESENTATION),
                        1,
                        ImmutableMap.of(symbol("symbol_97"), ASC_NULLS_FIRST),
                        TopNNode.Step.FINAL));
    }

    @Test
    public void testWindowNodeRepresentation()
    {
        assertAnonymizedPlan(
                pb -> {
                    Symbol orderKey = pb.symbol("orderKey", BIGINT);
                    Symbol partitionKey = pb.symbol("partitionKey", BIGINT);
                    Symbol hash = pb.symbol("hash", BIGINT);
                    Symbol startValue = pb.symbol("startValue", BIGINT);
                    Symbol endValue1 = pb.symbol("endValue1", BIGINT);
                    Symbol endValue2 = pb.symbol("endValue2", BIGINT);
                    Symbol input = pb.symbol("input", BIGINT);
                    Symbol output = pb.symbol("output", BIGINT);
                    return pb.window(
                            new WindowNode.Specification(
                                    ImmutableList.of(partitionKey),
                                    Optional.of(new OrderingScheme(
                                            ImmutableList.of(orderKey),
                                            ImmutableMap.of(orderKey, SortOrder.ASC_NULLS_FIRST)))),
                            ImmutableMap.of(
                                    output,
                                    new WindowNode.Function(
                                            MIN_FUNCTION,
                                            ImmutableList.of(input.toSymbolReference()),
                                            new WindowNode.Frame(
                                                    WindowFrame.Type.RANGE,
                                                    UNBOUNDED_PRECEDING,
                                                    Optional.of(startValue),
                                                    Optional.of(orderKey),
                                                    CURRENT_ROW,
                                                    Optional.of(endValue1),
                                                    Optional.of(orderKey),
                                                    Optional.of(startValue.toSymbolReference()),
                                                    Optional.of(endValue2.toSymbolReference())),
                                            false)),
                            hash,
                            pb.values(input));
                },
                new WindowNodeRepresentation(
                        new PlanNodeId("1"),
                        ImmutableList.of(typedSymbol("symbol_100358090", "bigint"), typedSymbol("symbol_-1005512447", "bigint")),
                        ImmutableList.of(new ValuesNodeRepresentation(
                                new PlanNodeId("0"),
                                ImmutableList.of(typedSymbol("symbol_100358090", "bigint")),
                                0,
                                Optional.of(ImmutableList.of()))),
                        ImmutableMap.of(symbol("symbol_-1005512447"), "min (\"symbol_100358090\")"),
                        ImmutableList.of(symbol("symbol_222376725")),
                        Optional.of(ImmutableMap.of(symbol("symbol_1234285617"), ASC_NULLS_FIRST)),
                        Optional.of(symbol("symbol_3195150")),
                        ImmutableSet.of(),
                        0));
    }

    private void assertAnonymizedPlan(Function<PlanBuilder, PlanNode> sourceNodeSupplier, AnonymizedNodeRepresentation representation)
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata(), queryRunner.getDefaultSession());
        AnonymizedNodeRepresentation actualRepresentation = new AnonymizePlanPrinter(
                sourceNodeSupplier.apply(planBuilder),
                planBuilder.getTypes(),
                tableScanNode -> TABLE_INFO)
                .generatePlanRepresentation();
        assertThat(actualRepresentation).isEqualTo(representation);
    }

    private static Symbol symbol(String symbol)
    {
        return new Symbol(symbol);
    }

    private static TypedSymbol typedSymbol(String symbol, String type)
    {
        return new TypedSymbol(new Symbol(symbol), type);
    }
}
