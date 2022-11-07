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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.JoinNode.Type;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;

public class TestTransformCorrelatedGroupedAggregationWithoutProjection
        extends BaseRuleTest
{
    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester().assertThat(new TransformCorrelatedGroupedAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(),
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithNonGroupedAggregation()
    {
        tester().assertThat(new TransformCorrelatedGroupedAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutDistinct()
    {
        tester().assertThat(new TransformCorrelatedGroupedAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        INNER,
                        PlanBuilder.expression("true"),
                        p.aggregation(outerBuilder -> outerBuilder
                                .singleGroupingSet(p.symbol("a"))
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("count"), PlanBuilder.expression("count()"), ImmutableList.of())
                                .source(p.filter(
                                        PlanBuilder.expression("b > corr"),
                                        p.values(p.symbol("a"), p.symbol("b")))))))
                .matches(
                        project(ImmutableMap.of("corr", expression("corr"), "sum_agg", expression("sum_agg"), "count_agg", expression("count_agg")),
                                aggregation(
                                        singleGroupingSet("corr", "unique", "a"),
                                        ImmutableMap.of(Optional.of("sum_agg"), functionCall("sum", ImmutableList.of("a")), Optional.of("count_agg"), functionCall("count", ImmutableList.of())),
                                        Optional.empty(),
                                        SINGLE,
                                        join(Type.INNER, builder -> builder
                                                .filter("b > corr")
                                                .left(
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")))
                                                .right(
                                                        filter(
                                                                "true",
                                                                values("a", "b")))))));
    }

    @Test
    public void rewritesOnSubqueryWithDistinct()
    {
        tester().assertThat(new TransformCorrelatedGroupedAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        INNER,
                        PlanBuilder.expression("true"),
                        p.aggregation(outerBuilder -> outerBuilder
                                .singleGroupingSet(p.symbol("a"))
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("count"), PlanBuilder.expression("count()"), ImmutableList.of())
                                .source(p.aggregation(innerBuilder -> innerBuilder
                                        .singleGroupingSet(p.symbol("a"))
                                        .source(p.filter(
                                                PlanBuilder.expression("b > corr"),
                                                p.values(p.symbol("a"), p.symbol("b")))))))))
                .matches(
                        project(ImmutableMap.of("corr", expression("corr"), "sum_agg", expression("sum_agg"), "count_agg", expression("count_agg")),
                                aggregation(
                                        singleGroupingSet("corr", "unique", "a"),
                                        ImmutableMap.of(Optional.of("sum_agg"), functionCall("sum", ImmutableList.of("a")), Optional.of("count_agg"), functionCall("count", ImmutableList.of())),
                                        Optional.empty(),
                                        SINGLE,
                                        aggregation(
                                                singleGroupingSet("corr", "unique", "a"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                join(Type.INNER, builder -> builder
                                                        .filter("b > corr")
                                                        .left(
                                                                assignUniqueId(
                                                                        "unique",
                                                                        values("corr")))
                                                        .right(
                                                                filter(
                                                                        "true",
                                                                        values("a", "b"))))))));
    }

    @Test
    public void rewritesOnSubqueryWithDecorrelatableDistinct()
    {
        // distinct aggregation can be decorrelated in the subquery by PlanNodeDecorrelator
        // because the correlated predicate is equality comparison
        tester().assertThat(new TransformCorrelatedGroupedAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        INNER,
                        PlanBuilder.expression("true"),
                        p.aggregation(outerBuilder -> outerBuilder
                                .singleGroupingSet(p.symbol("a"))
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(a)"), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("count"), PlanBuilder.expression("count()"), ImmutableList.of())
                                .source(p.aggregation(innerBuilder -> innerBuilder
                                        .singleGroupingSet(p.symbol("a"))
                                        .source(p.filter(
                                                PlanBuilder.expression("b = corr"),
                                                p.values(p.symbol("a"), p.symbol("b")))))))))
                .matches(
                        project(ImmutableMap.of("corr", expression("corr"), "sum_agg", expression("sum_agg"), "count_agg", expression("count_agg")),
                                aggregation(
                                        singleGroupingSet("corr", "unique", "a"),
                                        ImmutableMap.of(Optional.of("sum_agg"), functionCall("sum", ImmutableList.of("a")), Optional.of("count_agg"), functionCall("count", ImmutableList.of())),
                                        Optional.empty(),
                                        SINGLE,
                                        join(Type.INNER, builder -> builder
                                                .filter("b = corr")
                                                .left(
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")))
                                                .right(
                                                        aggregation(
                                                                singleGroupingSet("a", "b"),
                                                                ImmutableMap.of(),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                filter(
                                                                        "true",
                                                                        values("a", "b"))))))));
    }
}
