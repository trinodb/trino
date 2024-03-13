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
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.TableInfo;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol.typedSymbol;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonRepresentation
{
    private static final JsonCodec<Map<String, JsonRenderedNode>> DISTRIBUTED_PLAN_JSON_CODEC = mapJsonCodec(String.class, JsonRenderedNode.class);
    private static final JsonCodec<JsonRenderedNode> JSON_RENDERED_NODE_CODEC = jsonCodec(JsonRenderedNode.class);
    private static final TableInfo TABLE_INFO = new TableInfo(
            Optional.of("tpch"),
            new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"),
            TupleDomain.all());

    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        // the expected values below simple non-distributed plans
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
    public void testJsonPlan()
    {
        String query = "SELECT * FROM (VALUES 1, 2) limit 1";
        JsonRenderedNode expectedPlan = new JsonRenderedNode(
                "8",
                "Output",
                ImmutableMap.of("columnNames", "[_col0]"),
                ImmutableList.of(typedSymbol("field", INTEGER)),
                ImmutableList.of("_col0 := field"),
                ImmutableList.of(new PlanNodeStatsAndCostSummary(1, 5, 0, 0, 0)),
                ImmutableList.of(new JsonRenderedNode(
                        "90",
                        "Limit",
                        ImmutableMap.of("count", "1", "withTies", "", "inputPreSortedBy", "[]"),
                        ImmutableList.of(typedSymbol("field", INTEGER)),
                        ImmutableList.of(),
                        ImmutableList.of(new PlanNodeStatsAndCostSummary(1, 5, 5, 0, 0)),
                        ImmutableList.of(new JsonRenderedNode(
                                "0",
                                "Values",
                                ImmutableMap.of(),
                                ImmutableList.of(typedSymbol("field", INTEGER)),
                                ImmutableList.of("(1)", "(2)"),
                                ImmutableList.of(new PlanNodeStatsAndCostSummary(2, 10, 0, 0, 0)),
                                ImmutableList.of())))));

        assertThat(queryRunner.execute("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) " + query).getOnlyValue())
                .isEqualTo(DISTRIBUTED_PLAN_JSON_CODEC.toJson(ImmutableMap.of("0", expectedPlan)));

        assertThat(queryRunner.execute("EXPLAIN (TYPE LOGICAL, FORMAT JSON) " + query).getOnlyValue())
                .isEqualTo(JSON_RENDERED_NODE_CODEC.toJson(expectedPlan));
    }

    @Test
    public void testAggregationPlan()
    {
        assertJsonRepresentation(
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
                                "keys", "[y, z]",
                                "hash", "[]"),
                        ImmutableList.of(
                                typedSymbol("y", BIGINT),
                                typedSymbol("z", BIGINT),
                                typedSymbol("sum", BIGINT)),
                        ImmutableList.of("sum := sum(\"x\")"),
                        ImmutableList.of(),
                        ImmutableList.of(valuesRepresentation(
                                "0",
                                ImmutableList.of(typedSymbol("x", BIGINT), typedSymbol("y", BIGINT), typedSymbol("z", BIGINT))))));
    }

    @Test
    public void testJoinPlan()
    {
        assertJsonRepresentation(
                pb -> pb.join(
                        INNER,
                        pb.values(pb.symbol("a", BIGINT), pb.symbol("b", BIGINT)),
                        pb.values(pb.symbol("c", BIGINT), pb.symbol("d", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(pb.symbol("a", BIGINT), pb.symbol("d", BIGINT))),
                        ImmutableList.of(pb.symbol("b", BIGINT)),
                        ImmutableList.of(),
                        Optional.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new SymbolReference("c"))),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), pb.symbol("d", BIGINT))),
                new JsonRenderedNode(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of("criteria", "(\"a\" = \"d\")", "filter", "(\"a\" < \"c\")", "hash", "[]"),
                        ImmutableList.of(typedSymbol("b", BIGINT)),
                        ImmutableList.of("dynamicFilterAssignments = {d -> #DF}"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesRepresentation("0", ImmutableList.of(typedSymbol("a", BIGINT), typedSymbol("b", BIGINT))),
                                valuesRepresentation("1", ImmutableList.of(typedSymbol("c", BIGINT), typedSymbol("d", BIGINT))))));
    }

    @Test
    public void testSourceFragmentIdsInRemoteSource()
    {
        // This test works as a validation to check if descriptor key "sourceFragmentIds" is present
        // because it is being used to create LivePlan in the Trino's UI.
        assertJsonRepresentation(
                pb -> pb.remoteSource(
                        ImmutableList.of(new PlanFragmentId("1"), new PlanFragmentId("2")),
                        ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", BIGINT)),
                        Optional.empty(),
                        REPARTITION,
                        NONE),
                new JsonRenderedNode(
                        "0",
                        "RemoteSource",
                        ImmutableMap.of("sourceFragmentIds", "[1, 2]"),
                        ImmutableList.of(typedSymbol("a", BIGINT), typedSymbol("b", BIGINT)),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of()));
    }

    private static JsonRenderedNode valuesRepresentation(String id, List<TypedSymbol> outputs)
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

    private void assertJsonRepresentation(Function<PlanBuilder, PlanNode> sourceNodeSupplier, JsonRenderedNode expectedRepresentation)
    {
        queryRunner.inTransaction(transactionSession -> {
            PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getPlannerContext(), transactionSession);
            ValuePrinter valuePrinter = new ValuePrinter(queryRunner.getPlannerContext().getMetadata(), queryRunner.getPlannerContext().getFunctionManager(), transactionSession);
            String jsonRenderedNode = new PlanPrinter(
                    sourceNodeSupplier.apply(planBuilder),
                    planBuilder.getTypes(),
                    scanNode -> TABLE_INFO,
                    ImmutableMap.of(),
                    valuePrinter,
                    StatsAndCosts.empty(),
                    Optional.empty(),
                    new NoOpAnonymizer())
                    .toJson();
            assertThat(jsonRenderedNode).isEqualTo(JSON_RENDERED_NODE_CODEC.toJson(expectedRepresentation));
            return null;
        });
    }
}
