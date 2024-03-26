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
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestMultipleDistinctAggregationToMarkDistinct
        extends BaseRuleTest
{
    private static final int NODES_COUNT = 4;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODES_COUNT);
    private static final DistinctAggregationController DISTINCT_AGGREGATION_CONTROLLER = new DistinctAggregationController(TASK_COUNT_ESTIMATOR, createTestMetadataManager());

    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "input1"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "input2"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testSingleDistinct()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "input"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input")))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("output1"),
                                PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1")), new Symbol(UNKNOWN, "filter1")), ImmutableList.of(BIGINT))
                        .addAggregation(
                                p.symbol("output2"),
                                PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input2")), new Symbol(UNKNOWN, "filter2")), ImmutableList.of(BIGINT))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(p.symbol("input1"))
                                                .putIdentity(p.symbol("input2"))
                                                .put(p.symbol("filter1", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "input2"), new Constant(INTEGER, 0L)))
                                                .put(p.symbol("filter2", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "input1"), new Constant(INTEGER, 0L)))
                                                .build(),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();

        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1")), new Symbol(UNKNOWN, "filter1")), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input2"))), ImmutableList.of(BIGINT))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(p.symbol("input1"))
                                                .putIdentity(p.symbol("input2"))
                                                .put(p.symbol("filter1", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "input2"), new Constant(INTEGER, 0L)))
                                                .put(p.symbol("filter2", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "input1"), new Constant(INTEGER, 0L)))
                                                .build(),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();
    }

    @Test
    public void testGlobalAggregation()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(DISTINCT_AGGREGATION_CONTROLLER))
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input1"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", true, ImmutableList.of(new Reference(BIGINT, "input2"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input1"), p.symbol("input2")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                Optional.of("output1"), aggregationFunction("count", ImmutableList.of("input1")),
                                Optional.of("output2"), aggregationFunction("count", ImmutableList.of("input2"))),
                        ImmutableList.of(),
                        ImmutableList.of("mark_input1", "mark_input2"),
                        Optional.empty(),
                        SINGLE,
                        markDistinct(
                                "mark_input2",
                                ImmutableList.of("input2"),
                                markDistinct(
                                        "mark_input1",
                                        ImmutableList.of("input1"),
                                        values(ImmutableMap.of("input1", 0, "input2", 1))))));
    }
}
