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
import io.trino.cost.CostComparator;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.remoteSource;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestAdaptiveBroadcastToPartitionedJoin
        extends BaseRuleTest
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(75, 10, 15);
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> 10);

    @Test
    public void testBroadcastToPartitionedJoinWithArbitrarySource()
    {
        testBroadcastToPartitionedWithArbitrarySource(100_000_000, 200_000_000);
    }

    @Test
    public void testBroadcastToPartitionedJoinWithExistingExchange()
    {
        testBroadcastToPartitionedWithExistingExchange(100_000_000, 200_000_000);
    }

    @Test
    void testBroadcastToPartitionedWhenProbeSideIsNaN()
    {
        // In this scenario cost based comparison is not possible, so the rule simply change
        // the distribution type based on stats from build side
        // (buildRowCount > joinMaxBroadcastTableSize)
        testBroadcastToPartitionedWithArbitrarySource(200_000_000, Double.NaN);
        testBroadcastToPartitionedWithExistingExchange(200_000_000, Double.NaN);
    }

    @Test
    void testWhenCostOfExtraRemoteExchangeIsHigh()
    {
        // No change in distribution type as the cost of extra remote exchange is high
        assertArbitrarySourceWithoutPartialAgg(100_000_000, 1000_000_000)
                .doesNotFire();
        assertArbitrarySourceWithPartialAgg(100_000_000, 1000_000_000)
                .doesNotFire();

        // In this case, we don't have to add an extra exchange, so the rule should fire
        testBroadcastToPartitionedWithExistingExchange(100_000_000, 1000_000_000);
    }

    @Test
    public void testNoChangeWhenBuildSideIsSmaller()
    {
        assertArbitrarySourceWithoutPartialAgg(20_000, 200_000_000)
                .doesNotFire();
        assertArbitrarySourceWithPartialAgg(20_000, 200_000_000)
                .doesNotFire();
        assertRemoteExchangeWithoutPartialAgg(20_000, 200_000_000)
                .doesNotFire();
        assertRemoteExchangeWithPartialAgg(20_000, 200_000_000)
                .doesNotFire();
    }

    @Test
    public void testNoChangeWhenBuildSideIsNaN()
    {
        assertArbitrarySourceWithoutPartialAgg(Double.NaN, 20_000)
                .doesNotFire();
        assertArbitrarySourceWithPartialAgg(Double.NaN, 20_000)
                .doesNotFire();
        assertRemoteExchangeWithoutPartialAgg(Double.NaN, 20_000)
                .doesNotFire();
        assertRemoteExchangeWithPartialAgg(Double.NaN, 20_000)
                .doesNotFire();
    }

    private void testBroadcastToPartitionedWithArbitrarySource(double buildRowCount, double probeRowCount)
    {
        assertArbitrarySourceWithoutPartialAgg(buildRowCount, probeRowCount)
                .matches(join(INNER, builder -> builder
                        .equiCriteria("probeSymbol", "buildSymbol")
                        .distributionType(PARTITIONED)
                        .left(exchange(
                                REMOTE,
                                REPARTITION,
                                ImmutableList.of(),
                                ImmutableSet.of("probeSymbol"),
                                Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                remoteSource(
                                        ImmutableList.of(new PlanFragmentId("1")),
                                        ImmutableList.of("probeSymbol", "symbol2"),
                                        REPARTITION)))
                        .right(exchange(
                                LOCAL,
                                REPARTITION,
                                ImmutableList.of(),
                                ImmutableSet.of("buildSymbol"),
                                Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                exchange(
                                        REMOTE,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("buildSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("2")),
                                                ImmutableList.of("buildSymbol", "symbol1"),
                                                REPARTITION))))));
        assertArbitrarySourceWithPartialAgg(buildRowCount, probeRowCount)
                .matches(join(INNER, builder -> builder
                        .equiCriteria("probeSymbol", "buildSymbol")
                        .distributionType(PARTITIONED)
                        .left(aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                exchange(
                                        REMOTE,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("1")),
                                                ImmutableList.of("probeSymbol", "symbol2"),
                                                REPARTITION))))
                        .right(aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("buildSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                        exchange(
                                                REMOTE,
                                                REPARTITION,
                                                ImmutableList.of(),
                                                ImmutableSet.of("buildSymbol"),
                                                Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                                remoteSource(
                                                        ImmutableList.of(new PlanFragmentId("2")),
                                                        ImmutableList.of("buildSymbol", "symbol1"),
                                                        REPARTITION)))))));
    }

    private void testBroadcastToPartitionedWithExistingExchange(double buildRowCount, double probeRowCount)
    {
        assertRemoteExchangeWithoutPartialAgg(buildRowCount, probeRowCount)
                .matches(join(INNER, builder -> builder
                        .equiCriteria("probeSymbol", "buildSymbol")
                        .distributionType(PARTITIONED)
                        .left(exchange(
                                REMOTE,
                                REPARTITION,
                                ImmutableList.of(),
                                ImmutableSet.of("probeSymbol"),
                                Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                values(ImmutableList.of("probeSymbol", "symbol2"))))
                        .right(exchange(
                                LOCAL,
                                REPARTITION,
                                ImmutableList.of(),
                                ImmutableSet.of("buildSymbol"),
                                Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                exchange(
                                        REMOTE,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("buildSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                        values(ImmutableList.of("buildSymbol", "symbol1")))))));

        assertRemoteExchangeWithPartialAgg(buildRowCount, probeRowCount)
                .matches(join(INNER, builder -> builder
                        .equiCriteria("probeSymbol", "buildSymbol")
                        .distributionType(PARTITIONED)
                        .left(aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                exchange(
                                        REMOTE,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                        values(ImmutableList.of("probeSymbol", "symbol2")))))
                        .right(aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("buildSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                        exchange(
                                                REMOTE,
                                                REPARTITION,
                                                ImmutableList.of(),
                                                ImmutableSet.of("buildSymbol"),
                                                Optional.of(ImmutableList.of(ImmutableList.of("buildSymbol", "symbol1"))),
                                                values(ImmutableList.of("buildSymbol", "symbol1"))))))));
    }

    private RuleAssert assertArbitrarySourceWithoutPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveBroadcastToPartitionedJoin(COST_COMPARATOR, TASK_COUNT_ESTIMATOR))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    return p.join(
                            INNER,
                            REPLICATED,
                            p.remoteSource(
                                    new PlanNodeId(probeRemoteSourceId),
                                    ImmutableList.of(new PlanFragmentId("1")),
                                    ImmutableList.of(probeSymbol, symbol2),
                                    Optional.empty(),
                                    REPARTITION,
                                    TASK),
                            p.exchange(builder -> builder
                                    .addInputsSet(buildSymbol, symbol1)
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(buildRemoteSourceId),
                                            ImmutableList.of(new PlanFragmentId("2")),
                                            ImmutableList.of(buildSymbol, symbol1),
                                            Optional.empty(),
                                            REPLICATE,
                                            TASK))
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(buildSymbol, symbol1),
                                            ImmutableList.of(buildSymbol))
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertArbitrarySourceWithPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveBroadcastToPartitionedJoin(COST_COMPARATOR, TASK_COUNT_ESTIMATOR))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    return p.join(
                            INNER,
                            REPLICATED,
                            p.aggregation(ab -> ab
                                    .step(PARTIAL)
                                    .singleGroupingSet(probeSymbol, symbol2)
                                    .source(p.remoteSource(
                                            new PlanNodeId(probeRemoteSourceId),
                                            ImmutableList.of(new PlanFragmentId("1")),
                                            ImmutableList.of(probeSymbol, symbol2),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))),
                            p.aggregation(ab -> ab
                                    .step(PARTIAL)
                                    .singleGroupingSet(buildSymbol, symbol1)
                                    .source(p.exchange(builder -> builder
                                            .addInputsSet(buildSymbol, symbol1)
                                            .addSource(p.remoteSource(
                                                    new PlanNodeId(buildRemoteSourceId),
                                                    ImmutableList.of(new PlanFragmentId("2")),
                                                    ImmutableList.of(buildSymbol, symbol1),
                                                    Optional.empty(),
                                                    REPLICATE,
                                                    TASK))
                                            .fixedHashDistributionPartitioningScheme(
                                                    ImmutableList.of(buildSymbol, symbol1),
                                                    ImmutableList.of(buildSymbol))
                                            .type(REPARTITION)
                                            .scope(LOCAL)))),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertRemoteExchangeWithoutPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveBroadcastToPartitionedJoin(COST_COMPARATOR, TASK_COUNT_ESTIMATOR))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    return p.join(
                            INNER,
                            REPLICATED,
                            p.exchange(builder -> builder
                                    .addInputsSet(probeSymbol, symbol2)
                                    .addSource(p.values(
                                            new PlanNodeId(probeRemoteSourceId),
                                            5,
                                            probeSymbol,
                                            symbol2))
                                    .fixedArbitraryDistributionPartitioningScheme(
                                            ImmutableList.of(probeSymbol, symbol2),
                                            3)
                                    .type(REPARTITION)
                                    .scope(REMOTE)),
                            p.exchange(builder -> builder
                                    .addInputsSet(buildSymbol, symbol1)
                                    .addSource(p.exchange(reBuilder -> reBuilder
                                            .addInputsSet(buildSymbol, symbol1)
                                            .addSource(p.values(
                                                    new PlanNodeId(buildRemoteSourceId),
                                                    5,
                                                    buildSymbol,
                                                    symbol1))
                                            .broadcastPartitioningScheme(ImmutableList.of(buildSymbol, symbol1))
                                            .type(REPLICATE)
                                            .scope(REMOTE)))
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(buildSymbol, symbol1),
                                            ImmutableList.of(buildSymbol))
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertRemoteExchangeWithPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveBroadcastToPartitionedJoin(COST_COMPARATOR, TASK_COUNT_ESTIMATOR))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceId", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    return p.join(
                            INNER,
                            REPLICATED,
                            p.aggregation(ab -> ab
                                    .step(PARTIAL)
                                    .singleGroupingSet(probeSymbol, symbol2)
                                    .source(p.exchange(builder -> builder
                                            .addInputsSet(probeSymbol, symbol2)
                                            .addSource(p.values(
                                                    new PlanNodeId(probeRemoteSourceId),
                                                    5,
                                                    probeSymbol,
                                                    symbol2))
                                            .fixedArbitraryDistributionPartitioningScheme(
                                                    ImmutableList.of(probeSymbol, symbol2),
                                                    3)
                                            .type(REPARTITION)
                                            .scope(REMOTE)))),
                            p.aggregation(ab -> ab
                                    .step(PARTIAL)
                                    .singleGroupingSet(buildSymbol, symbol1)
                                    .source(p.exchange(builder -> builder
                                            .addInputsSet(buildSymbol, symbol1)
                                            .addSource(p.exchange(reBuilder -> reBuilder
                                                    .addInputsSet(buildSymbol, symbol1)
                                                    .addSource(p.values(
                                                            new PlanNodeId(buildRemoteSourceId),
                                                            5,
                                                            buildSymbol,
                                                            symbol1))
                                                    .broadcastPartitioningScheme(ImmutableList.of(buildSymbol, symbol1))
                                                    .type(REPLICATE)
                                                    .scope(REMOTE)))
                                            .fixedHashDistributionPartitioningScheme(
                                                    ImmutableList.of(buildSymbol, symbol1),
                                                    ImmutableList.of(buildSymbol))
                                            .type(REPARTITION)
                                            .scope(LOCAL)))),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }
}
