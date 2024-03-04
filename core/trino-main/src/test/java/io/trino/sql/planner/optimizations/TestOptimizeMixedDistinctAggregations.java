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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.SystemSessionProperties;
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestOptimizeMixedDistinctAggregations
        extends BasePlanTest
{
    public TestOptimizeMixedDistinctAggregations()
    {
        super(ImmutableMap.of(
                SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true",
                SystemSessionProperties.MARK_DISTINCT_STRATEGY, "always"));
    }

    @Test
    public void testMixedDistinctAggregationOptimizer()
    {
        @Language("SQL") String sql = "SELECT custkey, max(totalprice) AS s, count(DISTINCT orderdate) AS d FROM orders GROUP BY custkey";

        String group = "GROUP";

        // Original keys
        String groupBy = "CUSTKEY";
        String aggregate = "TOTALPRICE";
        String distinctAggregation = "ORDERDATE";

        // Second Aggregation data
        List<String> groupByKeysSecond = ImmutableList.of(groupBy);
        Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregationsSecond = ImmutableMap.of(
                Optional.of("any_value"), aggregationFunction("any_value", false, ImmutableList.of(anySymbol())),
                Optional.of("count"), aggregationFunction("count", false, ImmutableList.of(anySymbol())));

        // First Aggregation data
        List<String> groupByKeysFirst = ImmutableList.of(groupBy, distinctAggregation, group);
        Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregationsFirst = ImmutableMap.of(
                Optional.of("MAX"), aggregationFunction("max", ImmutableList.of("TOTALPRICE")));

        PlanMatchPattern tableScan = tableScan("orders", ImmutableMap.of("TOTALPRICE", "totalprice", "CUSTKEY", "custkey", "ORDERDATE", "orderdate"));

        // GroupingSet symbols
        ImmutableList.Builder<List<String>> groups = ImmutableList.builder();
        groups.add(ImmutableList.of(groupBy, aggregate));
        groups.add(ImmutableList.of(groupBy, distinctAggregation));
        PlanMatchPattern expectedPlanPattern = anyTree(
                aggregation(singleGroupingSet(groupByKeysSecond), aggregationsSecond, Optional.empty(), SINGLE,
                        project(
                                aggregation(singleGroupingSet(groupByKeysFirst), aggregationsFirst, Optional.empty(), SINGLE,
                                        groupId(groups.build(), group,
                                                tableScan)))));

        assertUnitPlan(sql, expectedPlanPattern);
    }

    @Test
    public void testNestedType()
    {
        // Second Aggregation data
        Map<String, ExpectedValueProvider<AggregationFunction>> aggregationsSecond = ImmutableMap.of(
                "any_value", aggregationFunction("any_value", false, ImmutableList.of(anySymbol())),
                "count", aggregationFunction("count", false, ImmutableList.of(anySymbol())));

        // First Aggregation data
        Map<String, ExpectedValueProvider<AggregationFunction>> aggregationsFirst = ImmutableMap.of(
                "max", aggregationFunction("max", false, ImmutableList.of(anySymbol())));

        assertUnitPlan("SELECT count(DISTINCT a), max(b) FROM (VALUES (ROW(1, 2), 3)) t(a, b)",
                anyTree(
                        aggregation(aggregationsSecond,
                                project(
                                        aggregation(aggregationsFirst,
                                                anyTree(values(ImmutableMap.of())))))));
    }

    private void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getPlanTester().getPlannerContext().getMetadata()),
                new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new SingleDistinctAggregationToGroupBy(),
                                new MultipleDistinctAggregationToMarkDistinct(new TaskCountEstimator(() -> 4)))),
                new OptimizeMixedDistinctAggregations(getPlanTester().getPlannerContext().getMetadata()),
                new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(columnPruningRules(getPlanTester().getPlannerContext().getMetadata()))
                                .build()));
        assertPlan(sql, pattern, optimizers);
    }
}
