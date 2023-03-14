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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    private static final PlanNodeId JOIN_ID = new PlanNodeId("join_id");
    private static final PlanNodeId CHILD_ID = new PlanNodeId("child_id");

    @Test
    public void testPushesPartialAggregationThroughJoinWithoutProjection()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        ImmutableList.of(),
                                        Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI"))))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                "LEFT_EQUI", PlanMatchPattern.expression("LEFT_EQUI"),
                                "LEFT_NON_EQUI", PlanMatchPattern.expression("LEFT_NON_EQUI"),
                                "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_EQUI", "RIGHT_EQUI")
                                .filter("LEFT_NON_EQUI <= RIGHT_NON_EQUI")
                                .left(
                                        aggregation(
                                                singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI"),
                                                ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR")))
                                .right(
                                        values("RIGHT_EQUI", "RIGHT_NON_EQUI")))));

        // partial aggregation should not be pushed down because it would require extra grouping symbols
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        p.values(p.symbol("RIGHT_EQUI"), p.symbol("RIGHT_NON_EQUI")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("LEFT_EQUI"), p.symbol("RIGHT_EQUI"))),
                                        ImmutableList.of(p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"), p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_AGGR")),
                                        ImmutableList.of(),
                                        Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI"))))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushPartialAggregationForExpandingJoin()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithoutProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
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
                                        Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableMap.of()))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testPushesPartialAggregationThroughJoinWithProjection()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("LEFT_AGGR_PRJ"), PlanBuilder.expression("LEFT_AGGR + LEFT_AGGR"))
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
                                                Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")))))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR_PRJ)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"), p.symbol("LEFT_NON_EQUI"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                "LEFT_EQUI", PlanMatchPattern.expression("LEFT_EQUI"),
                                "LEFT_NON_EQUI", PlanMatchPattern.expression("LEFT_NON_EQUI"),
                                "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_EQUI", "RIGHT_EQUI")
                                .filter("LEFT_NON_EQUI <= RIGHT_NON_EQUI")
                                .left(
                                        aggregation(
                                                singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI"),
                                                ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR_PRJ"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                project(
                                                        ImmutableMap.of("LEFT_AGGR_PRJ", PlanMatchPattern.expression("LEFT_AGGR + LEFT_AGGR")),
                                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR"))))
                                .right(
                                        project(
                                                values("RIGHT_EQUI", "RIGHT_NON_EQUI"))))));

        // partial aggregation should not be pushed down because it would require extra grouping symbols
        tester().assertThat(new PushPartialAggregationThroughJoin(tester().getPlannerContext(), tester().getTypeAnalyzer()).pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("LEFT_AGGR_PRJ"), PlanBuilder.expression("LEFT_AGGR + LEFT_AGGR"))
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
                                                Optional.of(expression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")))))
                        .addAggregation(p.symbol("AVG", DOUBLE), expression("AVG(LEFT_AGGR_PRJ)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("LEFT_GROUP_BY"), p.symbol("LEFT_EQUI"))
                        .step(PARTIAL)))
                .doesNotFire();
    }
}
