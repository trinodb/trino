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
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.SystemSessionProperties.MARK_DISTINCT_STRATEGY;
import static io.trino.SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestMultipleDistinctAggregationToMarkDistinct
        extends BaseRuleTest
{
    private static final int NODES_COUNT = 4;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODES_COUNT);

    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(input1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("count(input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testSingleDistinct()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input1)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("sum(DISTINCT input)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input")))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input1) filter (where filter1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("count(DISTINCT input2) filter (where filter2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(p.symbol("input1"))
                                                .putIdentity(p.symbol("input2"))
                                                .put(p.symbol("filter1"), expression("input2 > 0"))
                                                .put(p.symbol("filter2"), expression("input1 > 0"))
                                                .build(),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();

        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input1) filter (where filter1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("count(DISTINCT input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(p.symbol("input1"))
                                                .putIdentity(p.symbol("input2"))
                                                .put(p.symbol("filter1"), expression("input2 > 0"))
                                                .put(p.symbol("filter2"), expression("input1 > 0"))
                                                .build(),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();
    }

    @Test
    public void testGlobalAggregation()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("count(DISTINCT input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input1"), p.symbol("input2")))))
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(
                                Optional.of("output1"), functionCall("count", ImmutableList.of("input1")),
                                Optional.of("output2"), functionCall("count", ImmutableList.of("input2"))),
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

    @Test
    public void testAggregationNDV()
    {
        PlanNodeId aggregationNodeId = new PlanNodeId("aggregationNodeId");
        Function<PlanBuilder, PlanNode> plan = p -> p.aggregation(builder -> builder
                .nodeId(aggregationNodeId)
                .singleGroupingSet(p.symbol("key"))
                .addAggregation(p.symbol("output1"), expression("count(DISTINCT input)"), ImmutableList.of(BIGINT))
                .addAggregation(p.symbol("output2"), expression("sum(input)"), ImmutableList.of(BIGINT))
                .source(
                        p.values(p.symbol("input"), p.symbol("key"))));
        PlanMatchPattern expectedMarkDistinct = aggregation(
                singleGroupingSet("key"),
                ImmutableMap.of(
                        Optional.of("output1"), functionCall("count", ImmutableList.of("input")),
                        Optional.of("output2"), functionCall("sum", ImmutableList.of("input"))),
                ImmutableList.of(),
                ImmutableList.of("mark_input"),
                Optional.empty(),
                SINGLE,
                markDistinct(
                        "mark_input",
                        ImmutableList.of("input", "key"),
                        values(ImmutableMap.of("input", 0, "key", 1))));

        int clusterThreadCount = NODES_COUNT * tester().getSession().getSystemProperty(TASK_CONCURRENCY, Integer.class);

        // small NDV
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(2 * clusterThreadCount).build())
                .matches(expectedMarkDistinct);

        // unknown estimate
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(Double.NaN).build())
                .matches(expectedMarkDistinct);

        // medium NDV, optimize_mixed_distinct_aggregations enabled
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .setSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(50 * clusterThreadCount).build())
                .matches(expectedMarkDistinct);

        // medium NDV, optimize_mixed_distinct_aggregations disabled
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .setSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, "false")
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(50 * clusterThreadCount).build())
                .doesNotFire();

        // medium NDV, optimize_mixed_distinct_aggregations enabled but plan has multiple distinct aggregations
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .nodeId(aggregationNodeId)
                        .singleGroupingSet(p.symbol("key"))
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("count(DISTINCT input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input1"), p.symbol("input2"), p.symbol("key")))))
                .setSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(50 * clusterThreadCount).build())
                .doesNotFire();

        // big NDV
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(1000 * clusterThreadCount).build())
                .doesNotFire();

        // big NDV, mark_distinct_strategy = always
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .setSystemProperty(MARK_DISTINCT_STRATEGY, "always")
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(1000 * clusterThreadCount).build())
                .matches(expectedMarkDistinct);
        // small NDV, mark_distinct_strategy = none
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(plan)
                .setSystemProperty(MARK_DISTINCT_STRATEGY, "none")
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(2 * clusterThreadCount).build())
                .doesNotFire();

        // big NDV but on multiple grouping keys
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .on(p -> p.aggregation(builder -> builder
                        .nodeId(aggregationNodeId)
                        .singleGroupingSet(p.symbol("key1"), p.symbol("key2"))
                        .addAggregation(p.symbol("output1"), expression("count(DISTINCT input)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), expression("sum(input)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input"), p.symbol("key1"), p.symbol("key2")))))
                .overrideStats(aggregationNodeId.toString(), PlanNodeStatsEstimate.builder().setOutputRowCount(1000 * clusterThreadCount).build())
                .matches(aggregation(
                        singleGroupingSet("key1", "key2"),
                        ImmutableMap.of(
                                Optional.of("output1"), functionCall("count", ImmutableList.of("input")),
                                Optional.of("output2"), functionCall("sum", ImmutableList.of("input"))),
                        ImmutableList.of(),
                        ImmutableList.of("mark_input"),
                        Optional.empty(),
                        SINGLE,
                        markDistinct(
                                "mark_input",
                                ImmutableList.of("input", "key1", "key2"),
                                values(ImmutableMap.of("input", 0, "key1", 1, "key2", 2)))));
    }
}
