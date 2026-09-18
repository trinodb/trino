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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.PREFER_PARTIAL_AGGREGATION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestPushPartialAggregationThroughExchange
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testPushesPartialAggregationThroughExchangeWithoutProjection()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(tester().getPlannerContext())
                        .pushPartialAggregationThroughExchangeWithoutProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(aggregationBuilder -> aggregationBuilder
                            .singleGroupingSet(a)
                            .step(PARTIAL)
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(INTEGER, "b"))), ImmutableList.of(INTEGER))
                            .source(p.exchange(e -> e
                                    .type(REPARTITION)
                                    .addSource(p.values(a, b))
                                    .addInputsSet(a, b)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(a)))));
                })
                .matches(
                        exchange(
                                project(
                                        aggregation(
                                                singleGroupingSet("a"),
                                                ImmutableMap.of(Optional.empty(), aggregationFunction("count", ImmutableList.of("b"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                values("a", "b")))));
    }

    @Test
    public void testPushesPartialAggregationThroughProjectionAndExchange()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(tester().getPlannerContext())
                        .pushPartialAggregationThroughExchangeWithProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol expression = p.symbol("expression", INTEGER);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(af -> af
                            .singleGroupingSet(a)
                            .step(PARTIAL)
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(INTEGER, "expression"))), ImmutableList.of(INTEGER))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(a)
                                            .put(expression, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))
                                            .build(),
                                    p.exchange(e -> e
                                            .type(REPARTITION)
                                            .addSource(p.values(a, b))
                                            .addInputsSet(a, b)
                                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(a))))));
                })
                .matches(
                        exchange(
                                project(
                                        aggregation(
                                                singleGroupingSet("a"),
                                                ImmutableMap.of(Optional.empty(), aggregationFunction("count", ImmutableList.of("expression"))),
                                                Optional.empty(),
                                                PARTIAL,
                                                project(
                                                        ImmutableMap.of("expression", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))),
                                                        values("a", "b"))))));
    }

    @Test
    public void testWithProjectionDoesNotFireForSymbolToSymbolProjection()
    {
        // a projection that only renames/passes symbols is left to the plain PushProjectionThroughExchange rule
        tester().assertThat(new PushPartialAggregationThroughExchange(tester().getPlannerContext())
                        .pushPartialAggregationThroughExchangeWithProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(af -> af
                            .singleGroupingSet(a)
                            .step(PARTIAL)
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(INTEGER, "b"))), ImmutableList.of(INTEGER))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(a)
                                            .putIdentity(b)
                                            .build(),
                                    p.exchange(e -> e
                                            .type(REPARTITION)
                                            .addSource(p.values(a, b))
                                            .addInputsSet(a, b)
                                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(a))))));
                })
                .doesNotFire();
    }

    @Test
    public void testWithProjectionDoesNotFireWhenPartitioningColumnsNotGroupingKeys()
    {
        // partitioning on 'a' but grouping only on the projected expression, so partial aggregation can't be pushed down
        tester().assertThat(new PushPartialAggregationThroughExchange(tester().getPlannerContext())
                        .pushPartialAggregationThroughExchangeWithProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol bTimes5 = p.symbol("b_times_5", INTEGER);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(af -> af
                            .singleGroupingSet(bTimes5)
                            .step(PARTIAL)
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(INTEGER, "a"))), ImmutableList.of(INTEGER))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(a)
                                            .put(bTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))
                                            .build(),
                                    p.exchange(e -> e
                                            .type(REPARTITION)
                                            .addSource(p.values(a, b))
                                            .addInputsSet(a, b)
                                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(a))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPreferPartialAggregationDisabled()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(tester().getPlannerContext())
                        .pushPartialAggregationThroughExchangeWithProjection())
                .setSystemProperty(PREFER_PARTIAL_AGGREGATION, "false")
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol bTimes5 = p.symbol("b_times_5", INTEGER);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(af -> af
                            .singleGroupingSet(a)
                            .step(PARTIAL)
                            .addAggregation(count, PlanBuilder.aggregation("count", ImmutableList.of(new Reference(INTEGER, "b_times_5"))), ImmutableList.of(INTEGER))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(a)
                                            .put(bTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))
                                            .build(),
                                    p.exchange(e -> e
                                            .type(REPARTITION)
                                            .addSource(p.values(a, b))
                                            .addInputsSet(a, b)
                                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(a, b), ImmutableList.of(a))))));
                })
                .doesNotFire();
    }
}
