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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PushFilterThroughCountAggregation.PushFilterThroughCountAggregationWithProject;
import io.trino.sql.planner.iterative.rule.PushFilterThroughCountAggregation.PushFilterThroughCountAggregationWithoutProject;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushFilterThroughCountAggregation
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testDoesNotFireWithNonGroupedAggregation()
    {
        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(INTEGER, "count"), new Constant(INTEGER, 0L)),
                            p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
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
                            new Comparison(GREATER_THAN, new Reference(INTEGER, "count"), new Constant(INTEGER, 0L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                    .addAggregation(avg, PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "g"))), ImmutableList.of(BIGINT), mask)
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
                            TRUE,
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
                            new Comparison(GREATER_THAN, new Reference(INTEGER, "count"), new Constant(INTEGER, 0L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
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
                            new Comparison(GREATER_THAN, new Reference(INTEGER, "count"), new Constant(INTEGER, 0L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "g"))), ImmutableList.of(BIGINT), mask)
                                    .source(p.values(g, mask))));
                })
                .doesNotFire();

        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol avg = p.symbol("avg");
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(INTEGER, "avg"), new Constant(INTEGER, 0L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(avg, PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "g"))), ImmutableList.of(BIGINT), mask)
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
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
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
                            TRUE,
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
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
                            new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)))), new Comparison(EQUAL, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
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
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                filter(
                                        new Reference(BOOLEAN, "mask"),
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
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)),
                                aggregation(
                                        ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                        filter(
                                                new Reference(BOOLEAN, "mask"),
                                                values("g", "mask")))));

        tester().assertThat(new PushFilterThroughCountAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)), new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "count"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 0L)))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "count"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 0L)),
                                aggregation(
                                        ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                        filter(
                                                new Reference(BOOLEAN, "mask"),
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
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 5L)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                    .source(p.values(g, mask))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 5L)),
                                aggregation(
                                        ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                        filter(
                                                new Reference(BOOLEAN, "mask"),
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
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)),
                            p.project(
                                    Assignments.identity(count),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("count", expression(new Reference(BIGINT, "count"))),
                                aggregation(
                                        ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                        filter(
                                                new Reference(BOOLEAN, "mask"),
                                                values("g", "mask")))));

        tester().assertThat(new PushFilterThroughCountAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)))),
                            p.project(
                                    Assignments.identity(count, g),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)),
                                project(
                                        ImmutableMap.of("count", expression(new Reference(BIGINT, "count")), "g", expression(new Reference(BIGINT, "g"))),
                                        aggregation(
                                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                                filter(
                                                        new Reference(BOOLEAN, "mask"),
                                                        values("g", "mask"))))));

        tester().assertThat(new PushFilterThroughCountAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask");
                    Symbol count = p.symbol("count");
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 5L)),
                            p.project(
                                    Assignments.identity(count),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of(), mask)
                                            .source(p.values(g, mask)))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "count"), new Constant(BIGINT, 5L)),
                                project(
                                        ImmutableMap.of("count", expression(new Reference(BIGINT, "count"))),
                                        aggregation(
                                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                                filter(
                                                        new Reference(BOOLEAN, "mask"),
                                                        values("g", "mask"))))));
    }
}
