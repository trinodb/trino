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
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.DistinctAggregationController;
import io.trino.sql.planner.iterative.rule.DistinctAggregationToGroupBy;
import io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

public class TestOptimizeMixedDistinctAggregations
        extends BasePlanTest
{
    public TestOptimizeMixedDistinctAggregations()
    {
        super(ImmutableMap.of(DISTINCT_AGGREGATIONS_STRATEGY, "pre_aggregate"));
    }

    @Test
    public void testMixedDistinctAggregationOptimizer()
    {
        @Language("SQL") String sql = "SELECT custkey, max(totalprice) AS s, count(DISTINCT orderdate) AS d FROM orders GROUP BY custkey";

        String group = "group_id";

        // Original keys
        String groupBy = "CUSTKEY";
        String aggregate = "TOTALPRICE";
        String distinctAggregation = "ORDERDATE";

        // Second Aggregation data
        List<String> groupByKeysSecond = ImmutableList.of(groupBy);
        Map<Optional<String>, ExpectedValueProvider<AggregationFunction>> aggregationsSecond = ImmutableMap.of(
                Optional.of("any_value"), aggregationFunction("any_value", false, ImmutableList.of(anySymbol()), "gid-filter-0"),
                Optional.of("count"), aggregationFunction("count", false, ImmutableList.of(anySymbol()), "gid-filter-1"));

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
                                ImmutableMap.of(
                                        "gid-filter-0", expression(new ComparisonExpression(EQUAL, new SymbolReference(group), new Cast(new LongLiteral("0"), dataType("bigint")))),
                                        "gid-filter-1", expression(new ComparisonExpression(EQUAL, new SymbolReference(group), new Cast(new LongLiteral("1"), dataType("bigint"))))),
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
                "any_value", aggregationFunction("any_value", false, ImmutableList.of(anySymbol()), "gid-filter-0"),
                "count", aggregationFunction("count", false, ImmutableList.of(anySymbol()), "gid-filter-1"));

        // First Aggregation data
        Map<String, ExpectedValueProvider<AggregationFunction>> aggregationsFirst = ImmutableMap.of(
                "max", aggregationFunction("max", false, ImmutableList.of(anySymbol())));

        assertUnitPlan("SELECT count(DISTINCT a), max(b) FROM (VALUES (ROW(1, 2), 3)) t(a, b)",
                anyTree(
                        aggregation(aggregationsSecond,
                                project(
                                        ImmutableMap.of(
                                                "gid-filter-0", expression(new ComparisonExpression(EQUAL, new SymbolReference("group_id"), new Cast(new LongLiteral("0"), dataType("bigint")))),
                                                "gid-filter-1", expression(new ComparisonExpression(EQUAL, new SymbolReference("group_id"), new Cast(new LongLiteral("1"), dataType("bigint"))))),
                                        aggregation(aggregationsFirst,
                                                groupId(
                                                        ImmutableList.of(ImmutableList.of("b"), ImmutableList.of("a")),
                                                        "group_id",
                                                        values(ImmutableMap.of("a", 0, "b", 1))))))));
    }

    private void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        DistinctAggregationController distinctAggregationController = new DistinctAggregationController(new TaskCountEstimator(() -> 4), createTestMetadataManager());
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
                                new DistinctAggregationToGroupBy(getPlanTester().getPlannerContext(), distinctAggregationController),
                                new MultipleDistinctAggregationToMarkDistinct(distinctAggregationController))),
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
