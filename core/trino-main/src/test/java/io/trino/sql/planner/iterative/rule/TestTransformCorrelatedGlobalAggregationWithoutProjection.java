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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
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

public class TestTransformCorrelatedGlobalAggregationWithoutProjection
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void doesNotFireOnPlanWithoutCorrelatedJoinNode()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithoutAggregation()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(),
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithNonScalarAggregation()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
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
    public void doesNotFireOnMultipleProjections()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
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
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .matches(
                        project(ImmutableMap.of("sum_1", expression(new Reference(BIGINT, "sum_1")), "corr", expression(new Reference(BIGINT, "corr"))),
                                aggregation(ImmutableMap.of("sum_1", aggregationFunction("sum", ImmutableList.of("a"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(Assignments.of(p.symbol("expr", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L)))),
                                p.aggregation(ab -> ab
                                        .source(p.values(p.symbol("a"), p.symbol("b")))
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                        .globalGrouping()))))
                .doesNotFire();
    }

    @Test
    public void skipsMaskForNullInsensitiveMin()
    {
        // min({}) == min({NULL}) == NULL, so the synthetic non_null mask added to compensate for LEFT-join NULL rows
        // is redundant. This is the TPC-H Q02 case.
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("min"), PlanBuilder.aggregation("min", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .matches(
                        project(ImmutableMap.of("min_1", expression(new Reference(BIGINT, "min_1")), "corr", expression(new Reference(BIGINT, "corr"))),
                                aggregation(ImmutableMap.of("min_1", aggregationFunction("min", ImmutableList.of("a"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void skipsMaskForMultipleNullInsensitiveAggregations()
    {
        // When all aggregations in the node are null-insensitive (e.g. min and max together), no synthetic mask is needed.
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("min"), PlanBuilder.aggregation("min", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("max"), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "b"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .matches(
                        project(
                                aggregation(
                                        ImmutableMap.of(
                                                "min_1", aggregationFunction("min", ImmutableList.of("a")),
                                                "max_1", aggregationFunction("max", ImmutableList.of("b"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void skipsMaskForStddev()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("stddev_agg", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .matches(
                        project(ImmutableMap.of("stddev_agg", expression(new Reference(DOUBLE, "stddev_agg")), "corr", expression(new Reference(BIGINT, "corr"))),
                                aggregation(ImmutableMap.of("stddev_agg", aggregationFunction("stddev", ImmutableList.of("a"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void skipsMaskForCorrelation()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a", DOUBLE), p.symbol("b", DOUBLE)))
                                .addAggregation(
                                        p.symbol("corr_agg", DOUBLE),
                                        PlanBuilder.aggregation("corr", ImmutableList.of(new Reference(DOUBLE, "a"), new Reference(DOUBLE, "b"))),
                                        ImmutableList.of(DOUBLE, DOUBLE))
                                .globalGrouping())))
                .matches(
                        project(ImmutableMap.of("corr_agg", expression(new Reference(DOUBLE, "corr_agg")), "corr", expression(new Reference(BIGINT, "corr"))),
                                aggregation(ImmutableMap.of("corr_agg", aggregationFunction("corr", ImmutableList.of("a", "b"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void skipsMaskForApproxPercentileWithProjectedConstant()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.project(
                                        Assignments.builder()
                                                .put(p.symbol("a", DOUBLE), new Reference(DOUBLE, "a"))
                                                .put(p.symbol("lit_0_5", DOUBLE), new Constant(DOUBLE, 0.5))
                                                .build(),
                                        p.values(p.symbol("a", DOUBLE))))
                                .addAggregation(
                                        p.symbol("percentile_agg", DOUBLE),
                                        PlanBuilder.aggregation("approx_percentile", ImmutableList.of(new Reference(DOUBLE, "a"), new Reference(DOUBLE, "lit_0_5"))),
                                        ImmutableList.of(DOUBLE, DOUBLE))
                                .globalGrouping())))
                .matches(
                        project(ImmutableMap.of("percentile_agg", expression(new Reference(DOUBLE, "percentile_agg")), "corr", expression(new Reference(BIGINT, "corr"))),
                                aggregation(ImmutableMap.of("percentile_agg", aggregationFunction("approx_percentile", ImmutableList.of("a", "lit_0_5"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(project(
                                                        ImmutableMap.of(
                                                                "a", expression(new Reference(DOUBLE, "a")),
                                                                "lit_0_5", expression(new Constant(DOUBLE, 0.5))),
                                                        values(ImmutableMap.of("a", 0))))))));
    }

    @Test
    public void testSubqueryWithCount()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("b")))
                                .addAggregation(p.symbol("count_rows"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                .addAggregation(p.symbol("count_non_null_values"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .globalGrouping())))
                .matches(
                        project(
                                aggregation(ImmutableMap.of(
                                                "count_rows", aggregationFunction("count", ImmutableList.of()),
                                                "count_non_null_values", aggregationFunction("count", ImmutableList.of("a"))),
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(project(ImmutableMap.of("non_null", expression(TRUE)),
                                                        values(ImmutableMap.of("a", 0, "b", 1))))))));
    }

    @Test
    public void rewritesOnSubqueryWithDistinct()
    {
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(outerBuilder -> outerBuilder
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                .globalGrouping()
                                .source(p.aggregation(innerBuilder -> innerBuilder
                                        .singleGroupingSet(p.symbol("a"))
                                        .source(p.filter(
                                                comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")),
                                                p.values(p.symbol("a"), p.symbol("b")))))))))
                .matches(
                        project(ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")), "sum_agg", expression(new Reference(BIGINT, "sum_agg")), "count_agg", expression(new Reference(BIGINT, "count_agg"))),
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
                                                        .filter(comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")))
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
    public void rewritesOnSubqueryWithDecorrelatableDistinct()
    {
        // distinct aggregation can be decorrelated in the subquery by PlanNodeDecorrelator
        // because the correlated predicate is equality comparison
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(outerBuilder -> outerBuilder
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                .addAggregation(p.symbol("count"), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                                .globalGrouping()
                                .source(p.aggregation(innerBuilder -> innerBuilder
                                        .singleGroupingSet(p.symbol("a"))
                                        .source(p.filter(
                                                comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")),
                                                p.values(p.symbol("a"), p.symbol("b")))))))))
                .matches(
                        project(ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")), "sum_agg", expression(new Reference(BIGINT, "sum_agg")), "count_agg", expression(new Reference(BIGINT, "count_agg"))),
                                aggregation(
                                        singleGroupingSet("corr", "unique"),
                                        ImmutableMap.of(Optional.of("sum_agg"), aggregationFunction("sum", ImmutableList.of("a")), Optional.of("count_agg"), aggregationFunction("count", ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of("non_null"),
                                        Optional.empty(),
                                        SINGLE,
                                        join(LEFT, builder -> builder
                                                .filter(comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "corr")))
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
        tester().assertThat(new TransformCorrelatedGlobalAggregationWithoutProjection(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(ab -> ab
                                .source(p.values(p.symbol("a"), p.symbol("mask", BOOLEAN)))
                                .addAggregation(p.symbol("count_non_null_values"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT), p.symbol("mask", BOOLEAN))
                                .globalGrouping())))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "unique"),
                                        ImmutableMap.of(Optional.of("count_non_null_values"), aggregationFunction("count", ImmutableList.of("a"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        join(LEFT, builder -> builder
                                                .left(assignUniqueId(
                                                        "unique",
                                                        values(ImmutableMap.of("corr", 0))))
                                                .right(values(ImmutableMap.of("a", 0, "mask", 1)))))));
    }
}
