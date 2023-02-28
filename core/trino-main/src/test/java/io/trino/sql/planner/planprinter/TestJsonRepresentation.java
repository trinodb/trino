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
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol.typedSymbol;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonRepresentation
{
    private static final JsonCodec<Map<String, JsonRenderedNode>> DISTRIBUTED_PLAN_JSON_CODEC = mapJsonCodec(String.class, JsonRenderedNode.class);
    private static final JsonCodec<JsonRenderedNode> JSON_RENDERED_NODE_CODEC = jsonCodec(JsonRenderedNode.class);
    private static final TableInfo TABLE_INFO = new TableInfo(
            Optional.of("tpch"),
            new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"),
            TupleDomain.all());

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testDistributedJsonPlan()
    {
        MaterializedResult actualPlan = queryRunner.execute("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT quantity FROM lineitem limit 10");
        Map<String, JsonRenderedNode> distributedPlan = ImmutableMap.of(
                "0", new JsonRenderedNode(
                        "6",
                        "Output",
                        ImmutableMap.of("columnNames", "[quantity]"),
                        ImmutableList.of(typedSymbol("quantity", "double")),
                        ImmutableList.of(),
                        ImmutableList.of(new PlanNodeStatsAndCostSummary(10, 90, 0, 0, 0)),
                        ImmutableList.of(new JsonRenderedNode(
                                "98",
                                "Limit",
                                ImmutableMap.of("count", "10", "withTies", "", "inputPreSortedBy", "[]"),
                                ImmutableList.of(typedSymbol("quantity", "double")),
                                ImmutableList.of(),
                                ImmutableList.of(new PlanNodeStatsAndCostSummary(10, 90, 90, 0, 0)),
                                ImmutableList.of(new JsonRenderedNode(
                                        "149",
                                        "LocalExchange",
                                        ImmutableMap.of(
                                                "partitioning", "SINGLE",
                                                "isReplicateNullsAndAny", "",
                                                "hashColumn", "[]",
                                                "arguments", "[]"),
                                        ImmutableList.of(typedSymbol("quantity", "double")),
                                        ImmutableList.of(),
                                        ImmutableList.of(new PlanNodeStatsAndCostSummary(60175, 541575, 0, 0, 0)),
                                        ImmutableList.of(new JsonRenderedNode(
                                                "0",
                                                "TableScan",
                                                ImmutableMap.of("table", "tpch:tiny:lineitem"),
                                                ImmutableList.of(typedSymbol("quantity", "double")),
                                                ImmutableList.of("quantity := tpch:quantity"),
                                                ImmutableList.of(new PlanNodeStatsAndCostSummary(60175, 541575, 541575, 0, 0)),
                                                ImmutableList.of()))))))));
        MaterializedResult expectedPlan = resultBuilder(queryRunner.getDefaultSession(), createVarcharType(2058))
                .row(DISTRIBUTED_PLAN_JSON_CODEC.toJson(distributedPlan))
                .build();
        assertThat(actualPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void testLogicalJsonPlan()
    {
        MaterializedResult actualPlan = queryRunner.execute("EXPLAIN (TYPE LOGICAL, FORMAT JSON) SELECT quantity FROM lineitem limit 10");
        JsonRenderedNode expectedJsonNode = new JsonRenderedNode(
                "6",
                "Output",
                ImmutableMap.of("columnNames", "[quantity]"),
                ImmutableList.of(typedSymbol("quantity", "double")),
                ImmutableList.of(),
                ImmutableList.of(new PlanNodeStatsAndCostSummary(10, 90, 0, 0, 0)),
                ImmutableList.of(new JsonRenderedNode(
                        "98",
                        "Limit",
                        ImmutableMap.of("count", "10", "withTies", "", "inputPreSortedBy", "[]"),
                        ImmutableList.of(typedSymbol("quantity", "double")),
                        ImmutableList.of(),
                        ImmutableList.of(new PlanNodeStatsAndCostSummary(10, 90, 90, 0, 0)),
                        ImmutableList.of(new JsonRenderedNode(
                                "149",
                                "LocalExchange",
                                ImmutableMap.of(
                                        "partitioning", "SINGLE",
                                        "isReplicateNullsAndAny", "",
                                        "hashColumn", "[]",
                                        "arguments", "[]"),
                                ImmutableList.of(typedSymbol("quantity", "double")),
                                ImmutableList.of(),
                                ImmutableList.of(new PlanNodeStatsAndCostSummary(60175, 541575, 0, 0, 0)),
                                ImmutableList.of(new JsonRenderedNode(
                                        "0",
                                        "TableScan",
                                        ImmutableMap.of("table", "tpch:tiny:lineitem"),
                                        ImmutableList.of(typedSymbol("quantity", "double")),
                                        ImmutableList.of("quantity := tpch:quantity"),
                                        ImmutableList.of(new PlanNodeStatsAndCostSummary(60175, 541575, 541575, 0, 0)),
                                        ImmutableList.of())))))));
        MaterializedResult expectedPlan = resultBuilder(queryRunner.getDefaultSession(), createVarcharType(1884))
                .row(JSON_RENDERED_NODE_CODEC.toJson(expectedJsonNode))
                .build();
        assertThat(actualPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void testAggregationPlan()
    {
        assertJsonRepresentation(
                pb -> pb.aggregation(ab -> ab
                        .step(FINAL)
                        .addAggregation(pb.symbol("sum", BIGINT), expression("sum(x)"), ImmutableList.of(BIGINT))
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
                                typedSymbol("y", "bigint"),
                                typedSymbol("z", "bigint"),
                                typedSymbol("sum", "bigint")),
                        ImmutableList.of("sum := sum(\"x\")"),
                        ImmutableList.of(),
                        ImmutableList.of(valuesRepresentation(
                                "0",
                                ImmutableList.of(typedSymbol("x", "bigint"), typedSymbol("y", "bigint"), typedSymbol("z", "bigint"))))));
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
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(new DynamicFilterId("DF"), pb.symbol("d", BIGINT))),
                new JsonRenderedNode(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of("criteria", "(\"a\" = \"d\")", "hash", "[]"),
                        ImmutableList.of(typedSymbol("b", "bigint")),
                        ImmutableList.of("dynamicFilterAssignments = {d -> #DF}"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesRepresentation("0", ImmutableList.of(typedSymbol("a", "bigint"), typedSymbol("b", "bigint"))),
                                valuesRepresentation("1", ImmutableList.of(typedSymbol("c", "bigint"), typedSymbol("d", "bigint"))))));
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
                        ImmutableList.of(typedSymbol("a", "bigint"), typedSymbol("b", "bigint")),
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
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata(), queryRunner.getDefaultSession());
        ValuePrinter valuePrinter = new ValuePrinter(queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getDefaultSession());
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
    }
}
