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
import io.airlift.json.JsonCodec;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.TableInfo;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.predicate.Domain.all;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol.typedSymbol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAnonymizeJsonRepresentation
{
    private static final JsonCodec<JsonRenderedNode> JSON_RENDERED_NODE_CODEC = jsonCodec(JsonRenderedNode.class);
    private static final ColumnHandle TEST_COLUMN_HANDLE_A = new TestingColumnHandle("test_column_a");
    private static final ColumnHandle TEST_COLUMN_HANDLE_B = new TestingColumnHandle("test_column_b");
    private static final ColumnHandle TEST_COLUMN_HANDLE_C = new TestingColumnHandle("test_column_c");
    private static final ColumnHandle TEST_COLUMN_HANDLE_D = new TestingColumnHandle("test_column_d");
    private static final TupleDomain<ColumnHandle> TEST_TUPLE_DOMAIN = withColumnDomains(ImmutableMap.of(
            TEST_COLUMN_HANDLE_A, singleValue(BIGINT, 1L),
            TEST_COLUMN_HANDLE_B, multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
            TEST_COLUMN_HANDLE_C, Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 3L, true)), false),
            TEST_COLUMN_HANDLE_D, all(BIGINT)));
    private static final TableInfo TABLE_INFO = new TableInfo(
            Optional.of("tpch"),
            new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"),
            TEST_TUPLE_DOMAIN);

    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TEST_SESSION.getCatalog().get(), "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testAggregationPlan()
    {
        assertAnonymizedRepresentation(
                pb -> pb.aggregation(ab -> ab
                        .step(FINAL)
                        .addAggregation(pb.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))),
                new JsonRenderedNode(
                        "1",
                        "Aggregate",
                        ImmutableMap.of(
                                "type", "FINAL",
                                "keys", "[symbol_1, symbol_2]",
                                "hash", "[]"),
                        ImmutableList.of(
                                typedSymbol("symbol_1", BIGINT),
                                typedSymbol("symbol_2", BIGINT),
                                typedSymbol("symbol_3", BIGINT)),
                        ImmutableList.of("symbol_3 := sum(\"symbol_4\")"),
                        ImmutableList.of(),
                        ImmutableList.of(valuesRepresentation(
                                "0",
                                ImmutableList.of(
                                        typedSymbol("symbol_4", BIGINT),
                                        typedSymbol("symbol_1", BIGINT),
                                        typedSymbol("symbol_2", BIGINT))))));
    }

    @Test
    public void testJoinPlan()
    {
        assertAnonymizedRepresentation(
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
                new JsonRenderedNode(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of(
                                "criteria", "(\"symbol_1\" = \"symbol_2\")",
                                "hash", "[]"),
                        ImmutableList.of(typedSymbol("symbol_3", BIGINT)),
                        ImmutableList.of("dynamicFilterAssignments = {symbol_2 -> #DF}"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesRepresentation(
                                        "0",
                                        ImmutableList.of(
                                                typedSymbol("symbol_1", BIGINT),
                                                typedSymbol("symbol_3", BIGINT))),
                                valuesRepresentation(
                                        "1",
                                        ImmutableList.of(
                                                typedSymbol("symbol_4", BIGINT),
                                                typedSymbol("symbol_2", BIGINT))))));
    }

    @Test
    public void testTableScanPlan()
    {
        assertAnonymizedRepresentation(
                pb -> pb.tableScan(
                        new TableHandle(
                                queryRunner.getPlannerContext().getMetadata().getCatalogHandle(pb.getSession(), "tpch").orElseThrow(),
                                new TpchTableHandle(TINY_SCHEMA_NAME, "orders", TINY_SCALE_FACTOR),
                                TpchTransactionHandle.INSTANCE),
                        ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", BIGINT), pb.symbol("c", BIGINT), pb.symbol("d", BIGINT)),
                        ImmutableMap.of(
                                pb.symbol("a", BIGINT), TEST_COLUMN_HANDLE_A,
                                pb.symbol("b", BIGINT), TEST_COLUMN_HANDLE_B,
                                pb.symbol("c", BIGINT), TEST_COLUMN_HANDLE_C,
                                pb.symbol("d", BIGINT), TEST_COLUMN_HANDLE_D),
                        TEST_TUPLE_DOMAIN),
                new JsonRenderedNode(
                        "0",
                        "TableScan",
                        ImmutableMap.of(
                                "table", "[table = catalog_1.schema_1.table_1, connector = tpch]"),
                        ImmutableList.of(
                                typedSymbol("symbol_1", BIGINT),
                                typedSymbol("symbol_2", BIGINT),
                                typedSymbol("symbol_3", BIGINT),
                                typedSymbol("symbol_4", BIGINT)),
                        ImmutableList.of(
                                "symbol_1 := column_1",
                                "    :: [[bigint_value_1]]",
                                "symbol_2 := column_2",
                                "    :: [[bigint_value_1], [bigint_value_2], [bigint_value_3]]",
                                "symbol_3 := column_3",
                                "    :: [[bigint_value_1, bigint_value_3]]",
                                "symbol_4 := column_4"),
                        ImmutableList.of(),
                        ImmutableList.of()));
    }

    @Test
    public void testSortPlan()
    {
        assertAnonymizedRepresentation(
                pb -> pb.sort(ImmutableList.of(pb.symbol("a", BIGINT)), pb.values(pb.symbol("a", BIGINT))),
                new JsonRenderedNode(
                        "1",
                        "Sort",
                        ImmutableMap.of("orderBy", "[symbol_1 ASC NULLS FIRST]"),
                        ImmutableList.of(typedSymbol("symbol_1", BIGINT)),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesRepresentation(
                                        "0",
                                        ImmutableList.of(typedSymbol("symbol_1", BIGINT))))));
    }

    private static JsonRenderedNode valuesRepresentation(String id, List<NodeRepresentation.TypedSymbol> outputs)
    {
        return new JsonRenderedNode(
                id,
                "Values",
                ImmutableMap.of(),
                outputs,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of());
    }

    private void assertAnonymizedRepresentation(Function<PlanBuilder, PlanNode> sourceNodeSupplier, JsonRenderedNode expectedRepresentation)
    {
        queryRunner.inTransaction(session -> {
            PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getPlannerContext(), session);
            ValuePrinter valuePrinter = new ValuePrinter(queryRunner.getPlannerContext().getMetadata(), queryRunner.getPlannerContext().getFunctionManager(), session);
            String jsonRenderedNode = new PlanPrinter(
                    sourceNodeSupplier.apply(planBuilder),
                    planBuilder.getTypes(),
                    scanNode -> TABLE_INFO,
                    ImmutableMap.of(),
                    valuePrinter,
                    StatsAndCosts.empty(),
                    Optional.empty(),
                    new CounterBasedAnonymizer())
                    .toJson();
            assertThat(jsonRenderedNode).isEqualTo(JSON_RENDERED_NODE_CODEC.toJson(expectedRepresentation));
            return null;
        });
    }
}
