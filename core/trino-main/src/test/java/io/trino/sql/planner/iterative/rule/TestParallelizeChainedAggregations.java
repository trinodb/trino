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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.PARALLELIZE_CHAINED_AGGREGATIONS;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestParallelizeChainedAggregations
        extends BaseRuleTest
{
    @Test
    public void testInsertsRoundRobinExchange()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key2", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .matches(
                        node(AggregationNode.class,
                                exchange(LOCAL, REPARTITION,
                                        node(AggregationNode.class,
                                                values("key1", "key2", "value")))));
    }

    @Test
    public void testInsertsRoundRobinExchangeBelowProjection()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("renamed", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("renamed", BIGINT), new Reference(BIGINT, "key2"))
                                        .put(p.symbol("sum", BIGINT), new Reference(BIGINT, "sum"))
                                        .build(),
                                innerAggregation(p)))))
                .matches(
                        node(AggregationNode.class,
                                node(ProjectNode.class,
                                        exchange(LOCAL, REPARTITION,
                                                node(AggregationNode.class,
                                                        values("key1", "key2", "value"))))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "false")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key2", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithSingleThreadedTasks()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key2", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingKeysAreNotStrictSubset()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key1", BIGINT), p.symbol("key2", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingKeysDoNotOverlap()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("sum", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "key1"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonPartialOuterAggregation()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key2", BIGINT))
                        .step(SINGLE)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(innerAggregation(p))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnPartialInnerAggregation()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("key2", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(p.aggregation(inner -> innerAggregationSpec(p, inner).step(PARTIAL)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingKeyIsNotSimpleRename()
    {
        tester().assertThat(new ParallelizeChainedAggregations())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATIONS, "true")
                .on(p -> p.aggregation(outer -> outer
                        .singleGroupingSet(p.symbol("computed", BIGINT))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("computed", BIGINT), new Coalesce(ImmutableList.of(new Reference(BIGINT, "key2"), new Constant(BIGINT, 0L))))
                                        .put(p.symbol("sum", BIGINT), new Reference(BIGINT, "sum"))
                                        .build(),
                                innerAggregation(p)))))
                .doesNotFire();
    }

    private static AggregationNode innerAggregation(PlanBuilder planBuilder)
    {
        return planBuilder.aggregation(inner -> innerAggregationSpec(planBuilder, inner).step(FINAL));
    }

    private static PlanBuilder.AggregationBuilder innerAggregationSpec(PlanBuilder planBuilder, PlanBuilder.AggregationBuilder aggregationBuilder)
    {
        return aggregationBuilder
                .singleGroupingSet(planBuilder.symbol("key1", BIGINT), planBuilder.symbol("key2", BIGINT))
                .addAggregation(planBuilder.symbol("sum", BIGINT), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "value"))), ImmutableList.of(BIGINT))
                .source(planBuilder.values(planBuilder.symbol("key1", BIGINT), planBuilder.symbol("key2", BIGINT), planBuilder.symbol("value", BIGINT)));
    }
}
