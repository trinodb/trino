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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestPushAggregationThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesAggregationThroughLeftJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        LEFT,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                        p.values(p.symbol("COL2")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1")),
                                        ImmutableList.of(p.symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                        "COL1", expression("COL1"),
                                        "COALESCE", expression("coalesce(AVG, AVG_NULL)")),
                                join(INNER, builder -> builder
                                        .left(
                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("COL1", "COL2")
                                                        .left(values(ImmutableMap.of("COL1", 0)))
                                                        .right(aggregation(
                                                                singleGroupingSet("COL2"),
                                                                ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("COL2"))),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                values(ImmutableMap.of("COL2", 0))))))
                                        .right(
                                                aggregation(
                                                        globalAggregation(),
                                                        ImmutableMap.of(Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationThroughRightJoin()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                RIGHT,
                                p.values(p.symbol("COL2")),
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                ImmutableList.of(new EquiJoinClause(p.symbol("COL2"), p.symbol("COL1"))),
                                ImmutableList.of(p.symbol("COL2")),
                                ImmutableList.of(p.symbol("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                        "COALESCE", expression("coalesce(AVG, AVG_NULL)"),
                                        "COL1", expression("COL1")),
                                join(INNER, builder -> builder
                                        .left(
                                                join(RIGHT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("COL2", "COL1")
                                                        .left(aggregation(
                                                                singleGroupingSet("COL2"),
                                                                ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("COL2"))),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                values(ImmutableMap.of("COL2", 0))))
                                                        .right(values(ImmutableMap.of("COL1", 0)))))
                                        .right(
                                                aggregation(
                                                        globalAggregation(),
                                                        ImmutableMap.of(
                                                                Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal"))),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationWithMask()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        LEFT,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                        p.values(p.symbol("COL2"), p.symbol("MASK")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1")),
                                        ImmutableList.of(p.symbol("COL2"), p.symbol("MASK")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(
                                p.symbol("AVG", DOUBLE),
                                PlanBuilder.expression("avg(COL2)"),
                                ImmutableList.of(DOUBLE),
                                p.symbol("MASK"))
                        .singleGroupingSet(p.symbol("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                "COL1", expression("COL1"),
                                "COALESCE", expression("coalesce(AVG, AVG_NULL)")),
                                join(INNER, builder -> builder
                                        .left(
                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("COL1", "COL2")
                                                        .left(values(ImmutableMap.of("COL1", 0)))
                                                        .right(
                                                                aggregation(
                                                                        singleGroupingSet("COL2"),
                                                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("COL2"))),
                                                                        ImmutableList.of(),
                                                                        ImmutableList.of("MASK"),
                                                                        Optional.empty(),
                                                                        SINGLE,
                                                                        values(ImmutableMap.of("COL2", 0, "MASK", 1))))))
                                        .right(
                                                aggregation(
                                                        globalAggregation(),
                                                        ImmutableMap.of(Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal"))),
                                                        ImmutableList.of(),
                                                        ImmutableList.of("MASK_NULL"),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("null_literal", 0, "MASK_NULL", 1)))))));
    }

    @Test
    public void testPushCountAllAggregation()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        LEFT,
                                        p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                        p.values(p.symbol("COL2")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1")),
                                        ImmutableList.of(p.symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("COUNT"), PlanBuilder.expression("count(*)"), ImmutableList.of())
                        .singleGroupingSet(p.symbol("COL1"))))
                .matches(
                        project(ImmutableMap.of(
                                "COL1", expression("COL1"),
                                "COALESCE", expression("coalesce(COUNT, COUNT_NULL)")),
                                join(INNER, builder -> builder
                                        .left(
                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("COL1", "COL2")
                                                        .left(values(ImmutableMap.of("COL1", 0)))
                                                        .right(
                                                                aggregation(
                                                                        singleGroupingSet("COL2"),
                                                                        ImmutableMap.of(Optional.of("COUNT"), functionCall("count", ImmutableList.of())),
                                                                        Optional.empty(),
                                                                        SINGLE,
                                                                        values(ImmutableMap.of("COL2", 0))))))
                                        .right(
                                                aggregation(
                                                        globalAggregation(),
                                                        ImmutableMap.of(Optional.of("COUNT_NULL"), functionCall("count", ImmutableList.of())),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testDoesNotFireWhenMultipleGroupingSets()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        LEFT,
                                        p.values(
                                                ImmutableList.of(p.symbol("COL1"), p.symbol("COL2")),
                                                ImmutableList.of(expressions("1", "2"))),
                                        p.values(
                                                ImmutableList.of(p.symbol("COL3")),
                                                ImmutableList.of(expressions("1"))),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("COL1"), p.symbol("COL3"))),
                                        ImmutableList.of(p.symbol("COL1")),
                                        ImmutableList.of(p.symbol("COL3")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("COUNT"), PlanBuilder.expression("count(*)"), ImmutableList.of())
                        .groupingSets(groupingSets(ImmutableList.of(p.symbol("COL1"), p.symbol("COL2")), 2, ImmutableSet.of()))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNotDistinct()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(new Symbol("COL2")),
                                ImmutableList.of(new EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(p.symbol("COL1")),
                                ImmutableList.of(p.symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("AVG"), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();

        // https://github.com/prestodb/presto/issues/10592
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        LEFT,
                                        p.project(Assignments.builder()
                                                        .putIdentity(p.symbol("COL1", BIGINT))
                                                        .build(),
                                                p.aggregation(builder ->
                                                        builder.singleGroupingSet(p.symbol("COL1"), p.symbol("unused"))
                                                                .source(
                                                                        p.values(
                                                                                ImmutableList.of(p.symbol("COL1"), p.symbol("unused")),
                                                                                ImmutableList.of(expressions("10", "1"), expressions("10", "2")))))),
                                        p.values(p.symbol("COL2")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("COL1"), p.symbol("COL2"))),
                                        ImmutableList.of(p.symbol("COL1")),
                                        ImmutableList.of(p.symbol("COL2")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))
                        .addAggregation(p.symbol("AVG", DOUBLE), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(p.symbol("COL1"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingOnInner()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(new Symbol("COL2"), new Symbol("COL3")),
                                ImmutableList.of(new EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(p.symbol("COL1")),
                                ImmutableList.of(p.symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("AVG"), PlanBuilder.expression("avg(COL2)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"), new Symbol("COL3"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationDoesNotHaveSymbols()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(ImmutableList.of(p.symbol("COL2")), ImmutableList.of(expressions("20"))),
                                ImmutableList.of(new EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(p.symbol("COL1")),
                                ImmutableList.of(p.symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("SUM"), PlanBuilder.expression("sum(COL1)"), ImmutableList.of(DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationOnMultipleSymbolsDoesNotHaveSomeSymbols()
    {
        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(ImmutableList.of(p.symbol("COL2"), p.symbol("COL3")), ImmutableList.of(expressions("20", "30"))),
                                ImmutableList.of(new EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1")),
                                ImmutableList.of(new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("MIN_BY"), PlanBuilder.expression("min_by(COL2, COL1)"), ImmutableList.of(DOUBLE, DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();

        tester().assertThat(new PushAggregationThroughOuterJoin())
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                LEFT,
                                p.values(ImmutableList.of(p.symbol("COL1")), ImmutableList.of(expressions("10"))),
                                p.values(ImmutableList.of(p.symbol("COL2"), p.symbol("COL3")), ImmutableList.of(expressions("20", "30"))),
                                ImmutableList.of(new EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                                ImmutableList.of(new Symbol("COL1")),
                                ImmutableList.of(new Symbol("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(new Symbol("SUM"), PlanBuilder.expression("sum(COL2)"), ImmutableList.of(DOUBLE))
                        .addAggregation(new Symbol("MIN_BY"), PlanBuilder.expression("min_by(COL2, COL3)"), ImmutableList.of(DOUBLE, DOUBLE))
                        .addAggregation(new Symbol("MAX_BY"), PlanBuilder.expression("max_by(COL2, COL1)"), ImmutableList.of(DOUBLE, DOUBLE))
                        .singleGroupingSet(new Symbol("COL1"))))
                .doesNotFire();
    }
}
