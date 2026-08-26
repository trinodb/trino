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
import io.trino.operator.scalar.DivideRoundToScale;
import io.trino.spi.type.DecimalType;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

class TestReplaceDecimalSumAndAvgWithSumAndCount
        extends BaseRuleTest
{
    private static final DecimalType DECIMAL = createDecimalType(10, 2);
    private static final DecimalType SUM_TYPE = createDecimalType(38, 2);
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction DIVIDE_ROUND_TO_SCALE = FUNCTIONS.resolveFunction(
            DivideRoundToScale.NAME,
            TypeDescriptorProvider.fromTypes(SUM_TYPE, BIGINT));

    // avg is rebuilt as $divide_round_to_scale(sum, count) cast back to avg's decimal(10, 2) output type
    private static ExpressionMatcher reconstructedAverage(String sumSymbol, String countSymbol)
    {
        return expression(new Cast(
                new Call(DIVIDE_ROUND_TO_SCALE, ImmutableList.of(
                        new Reference(SUM_TYPE, sumSymbol),
                        new Reference(BIGINT, countSymbol))),
                DECIMAL));
    }

    @Test
    void testReplacesDecimalAvgReusingSum()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input)));
                })
                .matches(project(
                        ImmutableMap.of("avg_out", reconstructedAverage("sum_out", "count")),
                        aggregation(
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                values("col"))));
    }

    @Test
    void testReplacesDecimalAvgWithGroupBy()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol groupKey = p.symbol("grp", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .singleGroupingSet(groupKey)
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input, groupKey)));
                })
                .matches(project(
                        ImmutableMap.of("avg_out", reconstructedAverage("sum_out", "count")),
                        aggregation(
                                singleGroupingSet("grp"),
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                values("col", "grp"))));
    }

    @Test
    void testReusesExistingCount()
    {
        // When the query already computes count(x), the rewrite must reuse it rather than add a
        // second identical count. The predicate asserts exactly two aggregations survive (the reused
        // sum and count), so no duplicate count is introduced.
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    Symbol countOutput = p.symbol("count_out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    countOutput,
                                    PlanBuilder.aggregation("count", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input)));
                })
                .matches(project(
                        ImmutableMap.of("avg_out", reconstructedAverage("sum_out", "count_out")),
                        aggregation(
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("col")),
                                        "count_out", aggregationFunction("count", ImmutableList.of("col"))),
                                (Predicate<AggregationNode>) node -> node.getAggregations().size() == 2,
                                values("col"))));
    }

    @Test
    void testDoesNotReuseCountWithDifferentMask()
    {
        // avg and sum share a mask so the rewrite fires, but the existing count(x) carries a
        // different mask, so it must not be reused: a new count with avg's mask is added, leaving
        // three aggregations (sum, the untouched count, and the new count).
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgSumMask = p.symbol("avg_sum_mask", BOOLEAN);
                    Symbol countMask = p.symbol("count_mask", BOOLEAN);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    Symbol countOutput = p.symbol("count_out", BIGINT);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    avgSumMask)
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    avgSumMask)
                            .addAggregation(
                                    countOutput,
                                    PlanBuilder.aggregation("count", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    countMask)
                            .source(p.values(input, avgSumMask, countMask)));
                })
                .matches(project(
                        aggregation(
                                ImmutableMap.of("sum_out", aggregationFunction("sum", ImmutableList.of("col"))),
                                (Predicate<AggregationNode>) node -> node.getAggregations().size() == 3,
                                values("col", "avg_sum_mask", "count_mask"))));
    }

    @Test
    void testPropagatesMaskToCount()
    {
        // avg and sum share the same mask, so the rewrite fires; the new count must carry that mask,
        // otherwise the reconstructed average would divide the masked sum by an unmasked count.
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    mask)
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    mask)
                            .source(p.values(input, mask)));
                })
                .matches(project(
                        ImmutableMap.of("avg_out", reconstructedAverage("sum_out", "count")),
                        aggregation(
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("col")),
                                        "count", aggregationFunction("count", ImmutableList.of("col"))),
                                // every surviving aggregation (sum and the new count) keeps the mask
                                (Predicate<AggregationNode>) node -> node.getAggregations().values().stream()
                                        .allMatch(aggregation -> aggregation.getMask().isPresent()),
                                values("col", "mask"))));
    }

    @Test
    void testDoesNotFireWhenAvgAndSumMasksDiffer()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgMask = p.symbol("avg_mask", BOOLEAN);
                    Symbol sumMask = p.symbol("sum_mask", BOOLEAN);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    avgMask)
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL),
                                    sumMask)
                            .source(p.values(input, avgMask, sumMask)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireWithoutMatchingSum()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireWhenSumOverDifferentColumn()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol other = p.symbol("other", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "other"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input, other)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForDoubleAvg()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DOUBLE);
                    Symbol avgOutput = p.symbol("avg_out", DOUBLE);
                    Symbol sumOutput = p.symbol("sum_out", DOUBLE);
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DOUBLE, "col"))),
                                    ImmutableList.of(DOUBLE))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DOUBLE, "col"))),
                                    ImmutableList.of(DOUBLE))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForDistinctAvg()
    {
        tester().assertThat(new ReplaceDecimalSumAndAvgWithSumAndCount(tester().getPlannerContext()))
                .on(p -> {
                    Symbol input = p.symbol("col", DECIMAL);
                    Symbol avgOutput = p.symbol("avg_out", DECIMAL);
                    Symbol sumOutput = p.symbol("sum_out", createDecimalType(38, 2));
                    return p.aggregation(a -> a
                            .globalGrouping()
                            .addAggregation(
                                    avgOutput,
                                    PlanBuilder.aggregation("avg", true, ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .addAggregation(
                                    sumOutput,
                                    PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DECIMAL, "col"))),
                                    ImmutableList.of(DECIMAL))
                            .source(p.values(input)));
                })
                .doesNotFire();
    }
}
