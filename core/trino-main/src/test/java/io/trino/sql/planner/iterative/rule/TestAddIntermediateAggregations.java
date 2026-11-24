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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestAddIntermediateAggregations
        extends BaseRuleTest
{
    @Test
    public void testSessionDisable()
    {
        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "false")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "b"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                                    .source(p.values(p.symbol("a"))))));
                }))
                .doesNotFire();
    }

    @Test
    public void testWithGroups()
    {
        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.singleGroupingSet(p.symbol("c"))
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "b"))), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.singleGroupingSet(p.symbol("b"))
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                                                    .source(p.values(p.symbol("a"))))));
                }))
                .doesNotFire();
    }

    @Test
    public void testFinalAggregationsSharingPartialAggregation()
    {
        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "partial"))), ImmutableList.of(DOUBLE))
                            .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "partial"))), ImmutableList.of(DOUBLE))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("partial", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                                                    .source(p.values(p.symbol("x", DOUBLE))))));
                }))
                .matches(
                        aggregation(
                                ImmutableMap.of(
                                        "variance", aggregationFunction("variance", ImmutableList.of("partial")),
                                        "stddev", aggregationFunction("stddev", ImmutableList.of("partial"))),
                                AggregationNode.Step.FINAL,
                                exchange(LOCAL, GATHER,
                                        intermediateAggregation("stddev",
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE, GATHER,
                                                                intermediateAggregation("variance",
                                                                        exchange(LOCAL, GATHER,
                                                                                aggregation(
                                                                                        ImmutableMap.of("partial", aggregationFunction("variance", ImmutableList.of("x"))),
                                                                                        AggregationNode.Step.PARTIAL,
                                                                                        values("x"))))))))));
    }

    private static PlanMatchPattern intermediateAggregation(String function, PlanMatchPattern source)
    {
        return aggregation(
                ImmutableMap.of("partial", aggregationFunction(function, ImmutableList.of("partial"))),
                node -> node.getStep() == AggregationNode.Step.INTERMEDIATE && node.getAggregations().size() == 1,
                source);
    }
}
