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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.PushFilterThroughBoolOrAggregation.PushFilterThroughBoolOrAggregationWithProject;
import io.trino.sql.planner.iterative.rule.PushFilterThroughBoolOrAggregation.PushFilterThroughBoolOrAggregationWithoutProject;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushFilterThroughBoolOrAggregation
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWithNonGroupedAggregation()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                            p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())), ImmutableList.of(BOOLEAN), bool)
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithMultipleAggregations()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    Symbol avg = p.symbol("avg", BIGINT);
                    return p.filter(
                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())), ImmutableList.of(BOOLEAN), bool)
                                    .addAggregation(avg, PlanBuilder.aggregation("avg", ImmutableList.of(g.toSymbolReference())), ImmutableList.of(BIGINT))
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoAggregations()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    return p.filter(
                            TRUE,
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();

        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    return p.filter(
                            TRUE,
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .source(p.values(g))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoBoolOrAggregation()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol avg = p.symbol("avg", DOUBLE);
                    return p.filter(
                            new Comparison(GREATER_THAN, avg.toSymbolReference(), new Constant(DOUBLE, 0d)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            avg,
                                            PlanBuilder.aggregation("avg", ImmutableList.of(g.toSymbolReference())),
                                            ImmutableList.of(BIGINT))
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testFilterPredicateFalse()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Logical(AND,
                                    ImmutableList.of(new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                                            new Comparison(EQUAL, aggrBool.toSymbolReference(), FALSE))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .matches(
                        values("g", "aggrbool"));
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateTrue()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            TRUE,
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateCoalesceWithOtherSymbol()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol other = p.symbol("other", BOOLEAN);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Coalesce(other.toSymbolReference(), TRUE, FALSE),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateNotCoalesce()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            not(tester().getMetadata(), new Coalesce(aggrBool.toSymbolReference(), TRUE)),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .doesNotFire();
    }

    @Test
    public void testPushDownSymbolAndRemoveFilter()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Coalesce(aggrBool.toSymbolReference(), TRUE),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g, bool)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                }).doesNotFire();

        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Coalesce(aggrBool.toSymbolReference(), FALSE),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g, bool)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .matches(
                        project(
                                ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                        "bool", expression(new Reference(BOOLEAN, "bool")),
                                        "aggrbool", expression(TRUE)),
                                aggregation(
                                        singleGroupingSet("g", "bool"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        filter(
                                                new Reference(BOOLEAN, "bool"),
                                                values("g", "bool")))));
    }

    @Test
    public void testPushDownSymbolAndSimplifyFilter()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Logical(AND,
                                    ImmutableList.of(
                                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                                            new Coalesce(aggrBool.toSymbolReference(), FALSE))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g, bool)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .matches(
                        project(
                                ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                        "bool", expression(new Reference(BOOLEAN, "bool")),
                                        "aggrbool", expression(TRUE)),
                                aggregation(
                                        singleGroupingSet("g", "bool"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        filter(
                                                new Reference(BOOLEAN, "bool"),
                                                values("g", "bool")))));
    }

    @Test
    public void testPushDownSymbolAndRetainFilter()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Logical(AND,
                                    ImmutableList.of(
                                            new Comparison(GREATER_THAN, g.toSymbolReference(), new Constant(BIGINT, 5L)),
                                            new Coalesce(aggrBool.toSymbolReference(), FALSE))),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g, bool)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)),
                                project(
                                        ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                                "bool", expression(new Reference(BOOLEAN, "bool")),
                                                "aggrbool", expression(TRUE)),
                                        aggregation(
                                                singleGroupingSet("g", "bool"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                AggregationNode.Step.SINGLE,
                                                filter(
                                                        new Reference(BOOLEAN, "bool"),
                                                        values("g", "bool"))))));
    }

    @Test
    public void testWithProject()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                            p.project(
                                    Assignments.identity(aggrBool),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(
                                                    aggrBool,
                                                    PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                                    ImmutableList.of(BOOLEAN))
                                            .source(p.values(g, bool)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("aggrbool", expression(new Reference(BOOLEAN, "aggrbool"))),
                                project(
                                        ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                                "aggrbool", expression(TRUE)),
                                        aggregation(
                                                ImmutableMap.of(),
                                                filter(
                                                        new Reference(BOOLEAN, "bool"),
                                                        values("g", "bool"))))));

        tester().assertThat(new PushFilterThroughBoolOrAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Logical(AND,
                                    ImmutableList.of(
                                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                                            new Coalesce(aggrBool.toSymbolReference(), FALSE))),
                            p.project(
                                    Assignments.identity(aggrBool, g),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(
                                                    aggrBool,
                                                    PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                                    ImmutableList.of(BOOLEAN))
                                            .source(p.values(g, bool)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("aggrbool", expression(new Reference(BOOLEAN, "aggrbool"))),
                                project(
                                        ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                                "aggrbool", expression(TRUE)),
                                        aggregation(
                                                ImmutableMap.of(),
                                                filter(
                                                        new Reference(BOOLEAN, "bool"),
                                                        values("g", "bool"))))));

        tester().assertThat(new PushFilterThroughBoolOrAggregationWithProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Logical(AND,
                                    ImmutableList.of(
                                            new Comparison(GREATER_THAN, g.toSymbolReference(), new Constant(BIGINT, 5L)),
                                            new Coalesce(aggrBool.toSymbolReference(), FALSE))),
                            p.project(
                                    Assignments.identity(aggrBool, g),
                                    p.aggregation(builder -> builder
                                            .singleGroupingSet(g)
                                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())), ImmutableList.of(BOOLEAN))
                                            .source(p.values(g, bool)))));
                })
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "g"), new Constant(BIGINT, 5L)),
                                project(
                                        ImmutableMap.of("aggrbool", expression(new Reference(BOOLEAN, "aggrbool")), "g", expression(new Reference(BIGINT, "g"))),
                                        project(
                                                ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                                        "aggrbool", expression(TRUE)),
                                                aggregation(
                                                        ImmutableMap.of(),
                                                        filter(
                                                                new Reference(BOOLEAN, "bool"),
                                                                values("g", "bool")))))));
    }

    @Test
    public void testFilterAggregation()
    {
        tester().assertThat(new PushFilterThroughBoolOrAggregationWithoutProject(tester().getPlannerContext()))
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.filter(
                            new Comparison(EQUAL, aggrBool.toSymbolReference(), TRUE),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(g)
                                    .addAggregation(
                                            aggrBool,
                                            PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())),
                                            ImmutableList.of(BOOLEAN))
                                    .source(p.values(g, bool))));
                })
                .matches(
                        project(
                                ImmutableMap.of("g", expression(new Reference(BIGINT, "g")),
                                        "aggrbool", expression(TRUE)),
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        filter(
                                                new Reference(BOOLEAN, "bool"),
                                                values("g", "bool")))));
    }
}
