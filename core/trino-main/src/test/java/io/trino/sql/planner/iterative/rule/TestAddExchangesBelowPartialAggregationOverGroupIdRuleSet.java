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
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.execution.TaskManagerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet.AddExchangesBelowExchangePartialAggregationGroupId;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestAddExchangesBelowPartialAggregationOverGroupIdRuleSet
        extends BaseRuleTest
{
    @Test
    public void testAddExchangesWithoutProjection()
    {
        testAddExchangesWithoutProjection(1000, 10_000, 1_000_000, ImmutableSet.of("groupingKey3"));
        testAddExchangesWithoutProjection(1000, 1000, 1000, ImmutableSet.of("groupingKey1"));
        // stats not available on any symbol make the rule to not fire
        testAddExchangesWithoutProjection(1000, 10_000, Double.NaN, ImmutableSet.of());
        testAddExchangesWithoutProjection(1000, Double.NaN, 10_000, ImmutableSet.of());
        testAddExchangesWithoutProjection(1000, 10_000, Double.NaN, ImmutableSet.of());
    }

    // empty partitionedBy means exchanges should not be added
    private void testAddExchangesWithoutProjection(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV, Set<String> partitionedBy)
    {
        RuleTester ruleTester = tester();
        String groupIdSourceId = "groupIdSourceId";
        RuleAssert ruleAssert = ruleTester.assertThat(belowExchangeRule(ruleTester))
                .overrideStats(groupIdSourceId, PlanNodeStatsEstimate
                        .builder()
                        .setOutputRowCount(100_000_000)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol(BIGINT, "groupingKey1"), SymbolStatsEstimate.builder().setDistinctValuesCount(groupingKey1NDV).build(),
                                new Symbol(BIGINT, "groupingKey2"), SymbolStatsEstimate.builder().setDistinctValuesCount(groupingKey2NDV).build(),
                                new Symbol(BIGINT, "groupingKey3"), SymbolStatsEstimate.builder().setDistinctValuesCount(groupingKey3NDV).build()))
                        .build())
                .on(p -> {
                    Symbol groupingKey1 = p.symbol("groupingKey1", BIGINT);
                    Symbol groupingKey2 = p.symbol("groupingKey2", BIGINT);
                    Symbol groupingKey3 = p.symbol("groupingKey3", BIGINT);
                    Symbol groupId = p.symbol("groupId", BIGINT);
                    return p.exchange(
                            exchangeBuilder -> exchangeBuilder
                                    .scope(REMOTE)
                                    .fixedArbitraryDistributionPartitioningScheme(ImmutableList.of(groupingKey1, groupingKey2, groupingKey3, groupId), 2)
                                    .addInputsSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                                    .addSource(
                                            p.aggregation(builder -> builder
                                                    .singleGroupingSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                                                    .step(PARTIAL)
                                                    .source(p.groupId(
                                                            ImmutableList.of(
                                                                    ImmutableList.of(groupingKey1, groupingKey2),
                                                                    ImmutableList.of(groupingKey1, groupingKey3)),
                                                            ImmutableList.of(),
                                                            groupId,
                                                            p.values(new PlanNodeId(groupIdSourceId), groupingKey1, groupingKey2, groupingKey3))))));
                });

        if (partitionedBy.isEmpty()) {
            ruleAssert.doesNotFire();
        }
        else {
            ruleAssert
                    .matches(exchange(
                            REMOTE,
                            aggregation(
                                    singleGroupingSet(ImmutableList.of("groupingKey1", "groupingKey2", "groupingKey3", "groupId")),
                                    ImmutableMap.of(),
                                    Optional.empty(),
                                    PARTIAL,
                                    groupId(
                                            ImmutableList.of(
                                                    ImmutableList.of("groupingKey1", "groupingKey2"),
                                                    ImmutableList.of("groupingKey1", "groupingKey3")),
                                            "groupId",
                                            exchange(
                                                    LOCAL,
                                                    REPARTITION,
                                                    ImmutableList.of(),
                                                    partitionedBy,
                                                    exchange(
                                                            REMOTE,
                                                            REPARTITION,
                                                            ImmutableList.of(),
                                                            partitionedBy,
                                                            values("groupingKey1", "groupingKey2", "groupingKey3")))))));
        }
    }

    private static AddExchangesBelowExchangePartialAggregationGroupId belowExchangeRule(RuleTester ruleTester)
    {
        return new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(
                ruleTester.getPlannerContext(),
                ruleTester.getPlanTester().getTaskCountEstimator(),
                new TaskManagerConfig()).belowExchangeRule();
    }
}
