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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PushFilterThroughCountAggregation.PushFilterThroughCountAggregationWithProject;
import io.trino.sql.planner.iterative.rule.PushFilterThroughCountAggregation.PushFilterThroughCountAggregationWithoutProject;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushFilterThroughCountAggregation
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWithNonGroupedAggregation()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > 0"),
                            p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithMultipleAggregations()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    Symbol avg = p.symbol("avg");
                    return p.filter(
                            PlanBuilder.expression("count > 0"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .addAggregation(avg, PlanBuilder.expression("avg(g)"), ImmutableList.of(BIGINT), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoAggregations()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    return p.filter(
                            PlanBuilder.expression("true"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoMask()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > 0"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of())
                                    .source(p.values(g))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoCountAggregation()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > 0"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count(g)"), ImmutableList.of(BIGINT), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();

        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol avg = p.symbol("avg");
                    return p.filter(
                            PlanBuilder.expression("avg > 0"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(avg, PlanBuilder.expression("avg(g)"), ImmutableList.of(BIGINT), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testFilterPredicateFalse()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count < BIGINT '0' AND count > BIGINT '0'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        values("g", "count"));
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateTrue()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("true"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateSatisfiedByAllCountValues()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("(count < BIGINT '0' OR count >= BIGINT '0') AND g = BIGINT '5'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testPushDownMaskAndRemoveFilter()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '0'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                filter(
                                        "mask",
                                        values("g", "mask"))));
    }

    @Test
    public void testPushDownMaskAndSimplifyFilter()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '0' AND g > BIGINT '5'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                "g > BIGINT '5'",
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                        filter(
                                                "mask",
                                                values("g", "mask")))));

        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '0' AND count % 2 = BIGINT '0'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                "count % 2 = BIGINT '0'",
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                        filter(
                                                "mask",
                                                values("g", "mask")))));
    }

    @Test
    public void testPushDownMaskAndRetainFilter()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '5'"),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                "count > BIGINT '5'",
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                        filter(
                                                "mask",
                                                values("g", "mask")))));
    }

    @Test
    public void testWithProject()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '0'"),
                            p.project(
                                    Assignments.identity(count),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("count", expression("count")),
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                        filter(
                                                "mask",
                                                values("g", "mask")))));

        tester().assertThat(new PushFilterThroughCountAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '0' AND g > BIGINT '5'"),
                            p.project(
                                    Assignments.identity(count, g),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        filter(
                                "g > BIGINT '5'",
                                project(
                                        ImmutableMap.of("count", expression("count"), "g", expression("g")),
                                        aggregation(
                                                ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                                filter(
                                                        "mask",
                                                        values("g", "mask"))))));

        tester().assertThat(new PushFilterThroughCountAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            PlanBuilder.expression("count > BIGINT '5'"),
                            p.project(
                                    Assignments.identity(count),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.expression("count()"), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        filter(
                                "count > BIGINT '5'",
                                project(
                                        ImmutableMap.of("count", expression("count")),
                                        aggregation(
                                                ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                                filter(
                                                        "mask",
                                                        values("g", "mask"))))));
    }
}
