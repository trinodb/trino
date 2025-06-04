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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinType.LEFT;

public class TestTransformCorrelatedGlobalAggregationWithProjection
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testDoesNotFireOnPlanWithoutCorrelatedJoinNode()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCorrelatedWithoutAggregation()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnUncorrelated()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(),
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCorrelatedWithNonScalarAggregation()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .singleGroupingSet(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnMultipleProjections()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(p.symbol("expr_2", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "expr"), new Constant(INTEGER, 1L)))),
                                p.project(
                                        Assignments.of(p.symbol("expr", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L)))),
                                        p.aggregation(ab -> ab
                                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                                .globalGrouping())))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnSubqueryWithoutProjection()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .doesNotFire();
    }

    @Test
    public void testRewritesOnSubqueryWithProjection()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(Assignments.of(p.symbol("expr", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L)))),
                                p.aggregation(ab -> ab
                                        .source(p.values(p.symbol("a"), p.symbol("b")))
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                        .globalGrouping()))))
                .matches(
                        project(ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")), "expr", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum_1"), new Constant(INTEGER, 1L))))),
                                aggregation(ImmutableMap.of("sum_1", aggregationFunction("sum", ImmutableList.of("a"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId("unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(project(ImmutableMap.of("non_null", expression(TRUE)),
                                                        values(ImmutableMap.of("a", 0, "b", 1))))))));
    }

    @Test
    public void testRewritesOnSubqueryWithDistinct()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_sum", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L))),
                                        p.symbol("expr_count", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "count"), new Constant(INTEGER, 1L)))),
                                p.aggregation(outerBuilder -> outerBuilder
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                        .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                        .globalGrouping()
                                        .source(p.aggregation(innerBuilder -> innerBuilder
                                                .singleGroupingSet(p.symbol("a"))
                                                .source(p.filter(
                                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")),
                                                        p.values(p.symbol("a"), p.symbol("b"))))))))))
                .matches(
                        project(ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "expr_sum", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum_agg"), new Constant(INTEGER, 1L)))),
                                        "expr_count", expression(new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "count_agg"), new Constant(INTEGER, 1L))))),
                                aggregation(
                                        singleGroupingSet("corr", "unique"),
                                        ImmutableMap.of(Optional.of("sum_agg"), aggregationFunction("sum", ImmutableList.of("a")), Optional.of("count_agg"), aggregationFunction("count", ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of("non_null"),
                                        Optional.empty(),
                                        SINGLE,
                                        aggregation(
                                                singleGroupingSet("corr", "unique", "non_null", "a"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                join(LEFT, builder -> builder
                                                        .filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")))
                                                        .left(
                                                                assignUniqueId(
                                                                        "unique",
                                                                        values("corr")))
                                                        .right(
                                                                project(
                                                                        ImmutableMap.of("non_null", expression(TRUE)),
                                                                        filter(
                                                                                TRUE,
                                                                                values("a", "b")))))))));
    }

    @Test
    public void testRewritesOnSubqueryWithDecorrelatableDistinct()
    {
        // distinct aggregation can be decorrelated in the subquery by PlanNodeDecorrelator
        // because the correlated predicate is equality comparison
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_sum", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L))),
                                        p.symbol("expr_count", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "count"), new Constant(INTEGER, 1L)))),
                                p.aggregation(outerBuilder -> outerBuilder
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                        .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                        .globalGrouping()
                                        .source(p.aggregation(innerBuilder -> innerBuilder
                                                .singleGroupingSet(p.symbol("a"))
                                                .source(p.filter(
                                                        new Comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")),
                                                        p.values(p.symbol("a"), p.symbol("b"))))))))))
                .matches(
                        project(ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "expr_sum", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum_agg"), new Constant(INTEGER, 1L)))),
                                        "expr_count", expression(new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "count_agg"), new Constant(INTEGER, 1L))))),
                                aggregation(
                                        singleGroupingSet("corr", "unique"),
                                        ImmutableMap.of(Optional.of("sum_agg"), aggregationFunction("sum", ImmutableList.of("a")), Optional.of("count_agg"), aggregationFunction("count", ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of("non_null"),
                                        Optional.empty(),
                                        SINGLE,
                                        join(LEFT, builder -> builder
                                                .filter(new Comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")))
                                                .left(
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")))
                                                .right(
                                                        project(
                                                                ImmutableMap.of("non_null", expression(TRUE)),
                                                                aggregation(
                                                                        singleGroupingSet("a", "b"),
                                                                        ImmutableMap.of(),
                                                                        Optional.empty(),
                                                                        SINGLE,
                                                                        filter(
                                                                                TRUE,
                                                                                values("a", "b")))))))));
    }

    @Test
    public void testWithPreexistingMask()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(Assignments.of(p.symbol("expr", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L)))),
                                p.aggregation(ab -> ab
                                        .source(p.values(p.symbol("a"), p.symbol("mask", BOOLEAN)))
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT), p.symbol("mask", BOOLEAN))
                                        .globalGrouping()))))
                .matches(
                        project(ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")), "expr", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum_1"), new Constant(INTEGER, 1L))))),
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum_1"), aggregationFunction("sum", ImmutableList.of("a"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("new_mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("new_mask", expression(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "mask"), new Reference(BOOLEAN, "non_null"))))),
                                                join(LEFT, builder -> builder
                                                        .left(assignUniqueId("unique",
                                                                values(ImmutableMap.of("corr", 0))))
                                                        .right(project(ImmutableMap.of("non_null", expression(TRUE)),
                                                                values(ImmutableMap.of("a", 0, "mask", 1)))))))));
    }

    @Test
    public void testRewritesOnSubqueryWithBoolOr()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        p.project(
                                Assignments.of(p.symbol("exists", BOOLEAN),
                                        new Coalesce(new Reference(BOOLEAN, "aggrbool"), FALSE)),
                                p.aggregation(ab -> ab
                                        .source(p.values(p.symbol("subquery", BOOLEAN)))
                                        .addAggregation(
                                                p.symbol("aggrbool", BOOLEAN),
                                                PlanBuilder.aggregation("bool_or", ImmutableList.of(new Reference(BOOLEAN, "subquery"))), ImmutableList.of(BOOLEAN))
                                        .globalGrouping()))))
                .matches(
                        project(
                                ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")),
                                        "exists", expression(new Coalesce(new Reference(BOOLEAN, "aggrbool"), FALSE))),
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("aggrbool"), aggregationFunction("bool_or", ImmutableList.of("subquery"))),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        join(LEFT, builder -> builder
                                                .left(
                                                        assignUniqueId("unique",
                                                                values(ImmutableMap.of("corr", 0))))
                                                .right(
                                                        values(ImmutableMap.of("subquery", 0)))))));
    }
}
