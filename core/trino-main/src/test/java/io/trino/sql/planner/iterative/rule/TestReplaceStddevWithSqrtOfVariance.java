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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;

public class TestReplaceStddevWithSqrtOfVariance
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction SQRT = FUNCTIONS.resolveFunction("sqrt", fromTypes(DOUBLE));

    @Test
    public void testDeriveSampleAndPopulationStddev()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value", DOUBLE);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(p.symbol("stddev_samp", DOUBLE), aggregation("stddev_samp", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("var_samp", DOUBLE), aggregation("var_samp", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("stddev_pop", DOUBLE), aggregation("stddev_pop", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("var_pop", DOUBLE), aggregation("var_pop", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(group, value)));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "group", expression(new Reference(BIGINT, "group")),
                                "var_samp", expression(new Reference(DOUBLE, "var_samp")),
                                "var_pop", expression(new Reference(DOUBLE, "var_pop")),
                                "stddev_samp", expression(new Call(SQRT, List.of(new Reference(DOUBLE, "var_samp")))),
                                "stddev_pop", expression(new Call(SQRT, List.of(new Reference(DOUBLE, "var_pop"))))),
                        aggregation(
                                ImmutableMap.of(
                                        "var_samp", aggregationFunction("variance", List.of("value")),
                                        "var_pop", aggregationFunction("var_pop", List.of("value"))),
                                node -> node.getAggregations().size() == 2,
                                values("group", "value"))));
    }

    @Test
    public void testDeriveForBigintInput()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference())), List.of(BIGINT))
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(value.toSymbolReference())), List.of(BIGINT))
                            .source(p.values(value)));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "variance", expression(new Reference(DOUBLE, "variance")),
                                "stddev", expression(new Call(SQRT, List.of(new Reference(DOUBLE, "variance"))))),
                        aggregation(
                                ImmutableMap.of("variance", aggregationFunction("variance", List.of("value"))),
                                node -> node.getAggregations().size() == 1,
                                values("value"))));
    }

    @Test
    public void testDoesNotFireWithoutVariance()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("stddev_pop", DOUBLE), aggregation("stddev_pop", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOtherVarianceKind()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev_samp", DOUBLE), aggregation("stddev_samp", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("var_pop", DOUBLE), aggregation("var_pop", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("stddev_pop", DOUBLE), aggregation("stddev_pop", List.of(p.symbol("other", DOUBLE).toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value, p.symbol("other", DOUBLE))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDifferentArguments()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    Symbol other = p.symbol("other", DOUBLE);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(other.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value, other)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDifferentDistinct()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", true, List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDifferentFilter()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference()), filter), List.of(DOUBLE))
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value, filter)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDifferentMask()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference())), List.of(DOUBLE), mask)
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value, mask)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonSingleStep()
    {
        tester().assertThat(new ReplaceStddevWithSqrtOfVariance(tester().getMetadata()))
                .on(p -> {
                    Symbol value = p.symbol("value", DOUBLE);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .step(FINAL)
                            .addAggregation(p.symbol("stddev", DOUBLE), aggregation("stddev", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .addAggregation(p.symbol("variance", DOUBLE), aggregation("variance", List.of(value.toSymbolReference())), List.of(DOUBLE))
                            .source(p.values(value)));
                })
                .doesNotFire();
    }
}
