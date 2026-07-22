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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestLimitBoolOrAggregationSource
        extends BaseRuleTest
{
    @Test
    public void testAddLimit()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, TRUE),
                                    p.values(2, a))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("aggrbool", aggregationFunction("bool_or", ImmutableList.of("subquerytrue"))),
                                project(
                                        ImmutableMap.of("subquerytrue", expression(TRUE)),
                                        limit(1, values("a")))));
    }

    @Test
    public void testDoesNotFireOnGroupedAggregation()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(a)
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(a)
                                            .put(subqueryTrue, TRUE)
                                            .build(),
                                    p.values(2, a))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnOtherAggregationFunction()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_and", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, TRUE),
                                    p.values(2, a))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnMultipleAggregations()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                            .source(p.project(
                                    Assignments.of(subqueryTrue, TRUE),
                                    p.values(2, a))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnMaskedAggregation()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN), mask)
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(mask)
                                            .put(subqueryTrue, TRUE)
                                            .build(),
                                    p.values(2, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenArgumentNotConstantTrue()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, bool.toSymbolReference()),
                                    p.values(2, bool))));
                })
                .doesNotFire();

        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, FALSE),
                                    p.values(2, a))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutProject()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol bool = p.symbol("bool", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(bool.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.values(2, bool)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSourceProducesAtMostSingleRow()
    {
        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, TRUE),
                                    p.limit(1, p.values(2, a)))));
                })
                .doesNotFire();

        tester().assertThat(new LimitBoolOrAggregationSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol subqueryTrue = p.symbol("subquerytrue", BOOLEAN);
                    Symbol aggrBool = p.symbol("aggrbool", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(aggrBool, PlanBuilder.aggregation("bool_or", ImmutableList.of(subqueryTrue.toSymbolReference())), ImmutableList.of(BOOLEAN))
                            .source(p.project(
                                    Assignments.of(subqueryTrue, TRUE),
                                    p.values(1, a))));
                })
                .doesNotFire();
    }
}
