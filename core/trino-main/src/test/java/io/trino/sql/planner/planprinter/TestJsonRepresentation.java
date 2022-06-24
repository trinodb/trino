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
import io.trino.cost.StatsAndCosts;
import io.trino.execution.TableInfo;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import static io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol.typedSymbol;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonRepresentation
{
    private static final TableInfo TABLE_INFO = new TableInfo(
            new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, "orders"),
            TupleDomain.all());

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
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
                                "hash", ""),
                        ImmutableList.of(
                                typedSymbol("y", "bigint"),
                                typedSymbol("z", "bigint"),
                                typedSymbol("sum", "bigint")),
                        "sum := sum(\"x\")\n",
                        ImmutableList.of(),
                        ImmutableList.of(valuesRepresentation(
                                "0",
                                ImmutableList.of(typedSymbol("x", "bigint"), typedSymbol("y", "bigint"), typedSymbol("z", "bigint")))),
                        ImmutableList.of()));
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
                        ImmutableMap.of("criteria", "(\"a\" = \"d\")", "hash", ""),
                        ImmutableList.of(typedSymbol("b", "bigint")),
                        "dynamicFilterAssignments = {d -> #DF}",
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesRepresentation("0", ImmutableList.of(typedSymbol("a", "bigint"), typedSymbol("b", "bigint"))),
                                valuesRepresentation("1", ImmutableList.of(typedSymbol("c", "bigint"), typedSymbol("d", "bigint")))),
                        ImmutableList.of()));
    }

    private static JsonRenderedNode valuesRepresentation(String id, List<TypedSymbol> outputs)
    {
        return new JsonRenderedNode(
                id,
                "Values",
                ImmutableMap.of(),
                outputs,
                "",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of());
    }

    private void assertJsonRepresentation(Function<PlanBuilder, PlanNode> sourceNodeSupplier, JsonRenderedNode expectedRepresentation)
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata(), queryRunner.getDefaultSession());
        ValuePrinter valuePrinter = new ValuePrinter(queryRunner.getMetadata(), queryRunner.getFunctionManager(), queryRunner.getDefaultSession());
        PlanPrinter planPrinter = new PlanPrinter(
                sourceNodeSupplier.apply(planBuilder),
                planBuilder.getTypes(),
                Optional.empty(),
                scanNode -> TABLE_INFO,
                ImmutableMap.of(),
                valuePrinter,
                StatsAndCosts.empty(),
                Optional.empty());
        JsonRenderedNode jsonRenderedNode = new JsonRenderer().renderJson(planPrinter.getRepresentation(), planPrinter.getRepresentation().getRoot());
        assertThat(jsonRenderedNode).isEqualTo(expectedRepresentation);
    }
}
