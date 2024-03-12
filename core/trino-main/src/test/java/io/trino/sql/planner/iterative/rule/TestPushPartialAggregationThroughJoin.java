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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.lang.Double.NaN;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    private static final PlanNodeId JOIN_ID = new PlanNodeId("join_id");
    private static final PlanNodeId CHILD_ID = new PlanNodeId("child_id");

    @Test
    public void testPushesPartialAggregationThroughJoinToLeftChildWithoutProjection()
    {
        // push to left child
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        ImmutableList.of(),
                                        Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("AVG", ImmutableList.of(new SymbolReference("LEFT_AGGR"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression(new SymbolReference("LEFT_GROUP_BY")),
                                "LEFT_EQUI", PlanMatchPattern.expression(new SymbolReference("LEFT_EQUI")),
                                "LEFT_NON_EQUI", PlanMatchPattern.expression(new SymbolReference("LEFT_NON_EQUI")),
                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_EQUI", "RIGHT_EQUI")
                                .filter(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))
                                .left(
                                        aggregation(
                                                singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI"),
                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("LEFT_AGGR"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR")))
                                .right(
                                        values("RIGHT_EQUI", "RIGHT_NON_EQUI")))));

        // push to right child
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI"), p.symbol("RIGHT_GROUP_BY"), p.symbol("RIGHT_AGGR")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(),
                                        ImmutableList.of(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI"), p.symbol("RIGHT_GROUP_BY"), p.symbol("RIGHT_AGGR")),
                                        Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("RIGHT_AGGR"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("RIGHT_GROUP_BY"), p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "RIGHT_GROUP_BY", PlanMatchPattern.expression(new SymbolReference("RIGHT_GROUP_BY")),
                                "RIGHT_EQUI", PlanMatchPattern.expression(new SymbolReference("RIGHT_EQUI")),
                                "RIGHT_NON_EQUI", PlanMatchPattern.expression(new SymbolReference("RIGHT_NON_EQUI")),
                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_EQUI", "RIGHT_EQUI")
                                .filter(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))
                                .left(
                                        values("LEFT_EQUI", "LEFT_NON_EQUI"))
                                .right(
                                        aggregation(
                                                singleGroupingSet("RIGHT_GROUP_BY", "RIGHT_EQUI", "RIGHT_NON_EQUI"),
                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("RIGHT_AGGR"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_AGGR"))))));
    }

    @Test
    public void testDoesNotPushPartialAggregationForExpandingJoin()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .overrideStats(CHILD_ID.toString(), new PlanNodeStatsEstimate(10.0, ImmutableMap.of()))
                .overrideStats(JOIN_ID.toString(), new PlanNodeStatsEstimate(20.0, ImmutableMap.of()))
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(JOIN_ID,
                                        INNER,
                                        p.values(CHILD_ID, p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        ImmutableList.of(),
                                        Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableMap.of()))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("LEFT_AGGR"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushPartialAggregationIfPushedGroupingSetIsLarger()
    {
        // partial aggregation should not be pushed down because it would require extra grouping symbols
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        ImmutableList.of(),
                                        Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("LEFT_AGGR"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();

        // partial aggregation should not be pushed down because it would require extra grouping symbols (with projection)
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("LEFT_AGGR_PRJ"), new ArithmeticBinaryExpression(ADD, new SymbolReference("LEFT_AGGR"), new SymbolReference("LEFT_AGGR")))
                                                .putIdentity(p.symbol("LEFT_GROUP_BY"))
                                                .putIdentity(p.symbol("LEFT_EQUI"))
                                                .putIdentity(p.symbol("LEFT_NON_EQUI"))
                                                .build(),
                                        p.join(
                                                INNER,
                                                p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                                p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                                ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                                ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                                ImmutableList.of(),
                                                Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI"))))))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("LEFT_AGGR_PRJ"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushPartialAggregationIfPushedGroupingSetIsSame()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        p.values(p.symbol("DATE_DIM_DATE_ID"), p.symbol("DATE_DIM_YEAR")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("FACT_DATE_ID"), p.symbol("DATE_DIM_DATE_ID"))),
                                        ImmutableList.of(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        ImmutableList.of(p.symbol("DATE_DIM_YEAR")),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("AMOUNT"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("DATE_DIM_YEAR"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "DATE_DIM_YEAR", PlanMatchPattern.expression(new SymbolReference("DATE_DIM_YEAR")),
                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                        join(INNER, builder -> builder
                                .equiCriteria("FACT_DATE_ID", "DATE_DIM_DATE_ID")
                                .left(
                                        aggregation(
                                                singleGroupingSet("FACT_DATE_ID"),
                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("AMOUNT"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("FACT_DATE_ID", "AMOUNT")))
                                .right(
                                        values("DATE_DIM_DATE_ID", "DATE_DIM_YEAR")))));
    }

    @Test
    public void testDoesNotPushPartialAggregationIfGroupingSymbolHasBigNDV()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .overrideStats(
                        CHILD_ID.toString(),
                        new PlanNodeStatsEstimate(10.0, ImmutableMap.of(
                                new Symbol("FACT_DATE_ID"), new SymbolStatsEstimate(NaN, NaN, 0.0, NaN, 10.0))))
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(CHILD_ID, p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        p.values(p.symbol("DATE_DIM_DATE_ID"), p.symbol("DATE_DIM_YEAR")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("FACT_DATE_ID"), p.symbol("DATE_DIM_DATE_ID"))),
                                        ImmutableList.of(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        ImmutableList.of(p.symbol("DATE_DIM_YEAR")),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("AMOUNT"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("DATE_DIM_YEAR"))
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testKeepsIntermediateAggregation()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        p.values(p.symbol("DATE_DIM_DATE_ID"), p.symbol("DATE_DIM_YEAR")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("FACT_DATE_ID"), p.symbol("DATE_DIM_DATE_ID"))),
                                        ImmutableList.of(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        ImmutableList.of(p.symbol("DATE_DIM_YEAR")),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("AMOUNT"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("DATE_DIM_YEAR"))
                        .step(PARTIAL)
                        .exchangeInputAggregation(true)))
                .matches(
                        aggregation(
                                singleGroupingSet("DATE_DIM_YEAR"),
                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("AVG"))),
                                Optional.empty(),
                                INTERMEDIATE,
                                project(ImmutableMap.of(
                                                "DATE_DIM_YEAR", PlanMatchPattern.expression(new SymbolReference("DATE_DIM_YEAR")),
                                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                                        join(INNER, builder -> builder
                                                .equiCriteria("FACT_DATE_ID", "DATE_DIM_DATE_ID")
                                                .left(
                                                        aggregation(
                                                                singleGroupingSet("FACT_DATE_ID"),
                                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("AMOUNT"))),
                                                                Optional.empty(),
                                                                PARTIAL,
                                                                values("FACT_DATE_ID", "AMOUNT")))
                                                .right(
                                                        values("DATE_DIM_DATE_ID", "DATE_DIM_YEAR"))))));

        // intermediate aggregation should not be added if pushed aggregation has same (in terms of symbols) or smaller grouping set
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        p.values(p.symbol("DATE_DIM_DATE_ID"), p.symbol("DATE_DIM_YEAR")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("FACT_DATE_ID"), p.symbol("DATE_DIM_DATE_ID"))),
                                        ImmutableList.of(p.symbol("FACT_DATE_ID"), p.symbol("AMOUNT")),
                                        ImmutableList.of(p.symbol("DATE_DIM_YEAR")),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("AMOUNT"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("FACT_DATE_ID"))
                        .step(PARTIAL)
                        .exchangeInputAggregation(true)))
                .matches(project(ImmutableMap.of(
                                "FACT_DATE_ID", PlanMatchPattern.expression(new SymbolReference("FACT_DATE_ID")),
                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                        join(INNER, builder -> builder
                                .equiCriteria("FACT_DATE_ID", "DATE_DIM_DATE_ID")
                                .left(
                                        aggregation(
                                                singleGroupingSet("FACT_DATE_ID"),
                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("AMOUNT"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("FACT_DATE_ID", "AMOUNT")))
                                .right(
                                        values("DATE_DIM_DATE_ID", "DATE_DIM_YEAR")))));
    }

    @Test
    public void testPushesPartialAggregationThroughJoinWithProjection()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithProjection())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("LEFT_AGGR_PRJ"), new ArithmeticBinaryExpression(ADD, new SymbolReference("LEFT_AGGR"), new SymbolReference("LEFT_AGGR")))
                                                .putIdentity(p.symbol("LEFT_GROUP_BY"))
                                                .putIdentity(p.symbol("LEFT_EQUI"))
                                                .putIdentity(p.symbol("LEFT_NON_EQUI"))
                                                .build(),
                                        p.join(
                                                INNER,
                                                p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                                p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                                ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                                ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                                ImmutableList.of(),
                                                Optional.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI"))))))
                        .addAggregation(p.symbol("AVG", DOUBLE), aggregation("avg", ImmutableList.of(new SymbolReference("LEFT_AGGR_PRJ"))), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression(new SymbolReference("LEFT_GROUP_BY")),
                                "LEFT_EQUI", PlanMatchPattern.expression(new SymbolReference("LEFT_EQUI")),
                                "LEFT_NON_EQUI", PlanMatchPattern.expression(new SymbolReference("LEFT_NON_EQUI")),
                                "AVG", PlanMatchPattern.expression(new SymbolReference("AVG"))),
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_EQUI", "RIGHT_EQUI")
                                .filter(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("LEFT_NON_EQUI"), new SymbolReference("RIGHT_NON_EQUI")))
                                .left(
                                        aggregation(
                                                singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI"),
                                                ImmutableMap.of(Optional.of("AVG"), aggregationFunction("avg", ImmutableList.of("LEFT_AGGR_PRJ"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                project(
                                                        ImmutableMap.of("LEFT_AGGR_PRJ", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("LEFT_AGGR"), new SymbolReference("LEFT_AGGR")))),
                                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR"))))
                                .right(
                                        project(
                                                values("RIGHT_EQUI", "RIGHT_NON_EQUI"))))));
    }
}
