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
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.remoteSource;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestAdaptiveReorderPartitionedJoin
        extends BaseRuleTest
{
    @Test
    public void testReorderPartitionedJoin()
    {
        assertWithoutPartialAgg(20_000_000_000L, 10_000_000_000L)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("buildSymbol", "probeSymbol")
                                .distributionType(PARTITIONED)
                                .left(remoteSource(
                                        ImmutableList.of(new PlanFragmentId("2")),
                                        ImmutableList.of("buildSymbol", "symbol1"),
                                        REPARTITION))
                                .right(exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("1")),
                                                ImmutableList.of("probeSymbol", "symbol2"),
                                                REPARTITION)))));

        assertWithPartialAgg(20_000_000_000L, 10_000_000_000L)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("buildSymbol", "probeSymbol")
                                .distributionType(PARTITIONED)
                                .left(aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("2")),
                                                ImmutableList.of("buildSymbol", "symbol1"),
                                                REPARTITION)))
                                .right(exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(ImmutableList.of("probeSymbol", "symbol2"))),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("1")),
                                                ImmutableList.of("probeSymbol", "symbol2"),
                                                REPARTITION)))));
    }

    @Test
    public void testReorderPartitionedJoinWithMultipleSources()
    {
        assertWithPartialAggAndMultipleSources(20_000_000_000L, 10_000_000_000L)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("buildSymbol", "probeSymbol")
                                .distributionType(PARTITIONED)
                                .left(aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        exchange(
                                                LOCAL,
                                                Optional.of(REPARTITION),
                                                Optional.of(FIXED_ARBITRARY_DISTRIBUTION),
                                                ImmutableList.of(),
                                                ImmutableSet.of(),
                                                Optional.of(ImmutableList.of(
                                                        ImmutableList.of("buildSymbol1", "symbol11"),
                                                        ImmutableList.of("buildSymbol2", "symbol12"))),
                                                ImmutableList.of("buildSymbol", "symbol1"),
                                                Optional.empty(),
                                                remoteSource(
                                                        ImmutableList.of(new PlanFragmentId("3")),
                                                        ImmutableList.of("buildSymbol1", "symbol11"),
                                                        REPARTITION),
                                                remoteSource(
                                                        ImmutableList.of(new PlanFragmentId("4")),
                                                        ImmutableList.of("buildSymbol2", "symbol12"),
                                                        REPARTITION))))
                                .right(exchange(
                                        LOCAL,
                                        Optional.of(REPARTITION),
                                        Optional.of(FIXED_HASH_DISTRIBUTION),
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(
                                                ImmutableList.of("probeSymbol1", "symbol21"),
                                                ImmutableList.of("probeSymbol2", "symbol22"))),
                                        ImmutableList.of("probeSymbol", "symbol2"),
                                        Optional.empty(),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("1")),
                                                ImmutableList.of("probeSymbol1", "symbol21"),
                                                REPARTITION),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("2")),
                                                ImmutableList.of("probeSymbol2", "symbol22"),
                                                REPARTITION)))));

        assertWithoutPartialAggAndMultipleSources(20_000_000_000L, 10_000_000_000L)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("buildSymbol", "probeSymbol")
                                .distributionType(PARTITIONED)
                                .left(exchange(
                                        LOCAL,
                                        Optional.of(REPARTITION),
                                        Optional.of(FIXED_ARBITRARY_DISTRIBUTION),
                                        ImmutableList.of(),
                                        ImmutableSet.of(),
                                        Optional.of(ImmutableList.of(
                                                ImmutableList.of("buildSymbol1", "symbol11"),
                                                ImmutableList.of("buildSymbol2", "symbol12"))),
                                        ImmutableList.of("buildSymbol", "symbol1"),
                                        Optional.empty(),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("3")),
                                                ImmutableList.of("buildSymbol1", "symbol11"),
                                                REPARTITION),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("4")),
                                                ImmutableList.of("buildSymbol2", "symbol12"),
                                                REPARTITION)))
                                .right(exchange(
                                        LOCAL,
                                        Optional.of(REPARTITION),
                                        Optional.of(FIXED_HASH_DISTRIBUTION),
                                        ImmutableList.of(),
                                        ImmutableSet.of("probeSymbol"),
                                        Optional.of(ImmutableList.of(
                                                ImmutableList.of("probeSymbol1", "symbol21"),
                                                ImmutableList.of("probeSymbol2", "symbol22"))),
                                        ImmutableList.of("probeSymbol", "symbol2"),
                                        Optional.empty(),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("1")),
                                                ImmutableList.of("probeSymbol1", "symbol21"),
                                                REPARTITION),
                                        remoteSource(
                                                ImmutableList.of(new PlanFragmentId("2")),
                                                ImmutableList.of("probeSymbol2", "symbol22"),
                                                REPARTITION)))));
    }

    @Test
    public void testNoChangesWhenBuildSourceIsSmaller()
    {
        assertWithPartialAgg(10_000_000_000L, 20_000_000_000L)
                .doesNotFire();
        assertWithoutPartialAgg(10_000_000_000L, 20_000_000_000L)
                .doesNotFire();
    }

    @Test
    public void testNoChangesWhenBuildSideIsBelowMinSizeLimit()
    {
        // Right size is below the default min size limit of 5 GB data size
        assertWithPartialAgg(1_00_000_000L, 1_000_000L)
                .doesNotFire();
        assertWithoutPartialAgg(1_00_000_000L, 1_000_000L)
                .doesNotFire();
    }

    @Test
    public void testNoChangesWhenEitherBuildOrProbeSideIsNan()
    {
        assertWithoutPartialAgg(Double.NaN, 10_000_000_000L)
                .doesNotFire();
        assertWithoutPartialAgg(20_000_000_000L, Double.NaN)
                .doesNotFire();
        assertWithoutPartialAgg(Double.NaN, Double.NaN)
                .doesNotFire();
    }

    private RuleAssert assertWithPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveReorderPartitionedJoin(ruleTester.getMetadata()))
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
                            PARTITIONED,
                            p.remoteSource(
                                    new PlanNodeId(probeRemoteSourceId),
                                    ImmutableList.of(new PlanFragmentId("1")),
                                    ImmutableList.of(probeSymbol, symbol2),
                                    Optional.empty(),
                                    REPARTITION,
                                    TASK),
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
                                                    REPARTITION,
                                                    TASK))
                                            .fixedHashDistributionPartitioningScheme(
                                                    ImmutableList.of(buildSymbol, symbol1),
                                                    ImmutableList.of(buildSymbol))
                                            .type(REPARTITION)
                                            .scope(LOCAL)))),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertWithPartialAggAndMultipleSources(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceA = "buildRemoteSourceA";
        String buildRemoteSourceB = "buildRemoteSourceB";
        String probeRemoteSourceA = "probeRemoteSourceA";
        String probeRemoteSourceB = "probeRemoteSourceB";
        return ruleTester.assertThat(new AdaptiveReorderPartitionedJoin(ruleTester.getMetadata()))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("buildRemoteSourceB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .overrideStats("probeRemoteSourceB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol buildSymbol1 = p.symbol("buildSymbol1", BIGINT);
                    Symbol symbol11 = p.symbol("symbol11", BIGINT);
                    Symbol buildSymbol2 = p.symbol("buildSymbol2", BIGINT);
                    Symbol symbol12 = p.symbol("symbol12", BIGINT);

                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    Symbol probeSymbol1 = p.symbol("probeSymbol1", BIGINT);
                    Symbol symbol21 = p.symbol("symbol21", BIGINT);
                    Symbol probeSymbol2 = p.symbol("probeSymbol2", BIGINT);
                    Symbol symbol22 = p.symbol("symbol22", BIGINT);
                    return p.join(
                            INNER,
                            PARTITIONED,
                            p.exchange(builder -> builder
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(probeRemoteSourceA),
                                            ImmutableList.of(new PlanFragmentId("1")),
                                            ImmutableList.of(probeSymbol1, symbol21),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(probeSymbol1, symbol21))
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(probeRemoteSourceB),
                                            ImmutableList.of(new PlanFragmentId("2")),
                                            ImmutableList.of(probeSymbol2, symbol22),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(probeSymbol2, symbol22))
                                    .fixedArbitraryDistributionPartitioningScheme(ImmutableList.of(probeSymbol, symbol2), 2)
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            p.aggregation(ab -> ab
                                    .step(PARTIAL)
                                    .singleGroupingSet(buildSymbol, symbol1)
                                    .source(p.exchange(builder -> builder
                                            .addSource(p.remoteSource(
                                                    new PlanNodeId(buildRemoteSourceA),
                                                    ImmutableList.of(new PlanFragmentId("3")),
                                                    ImmutableList.of(buildSymbol1, symbol11),
                                                    Optional.empty(),
                                                    REPARTITION,
                                                    TASK))
                                            .addInputsSet(ImmutableList.of(buildSymbol1, symbol11))
                                            .addSource(p.remoteSource(
                                                    new PlanNodeId(buildRemoteSourceB),
                                                    ImmutableList.of(new PlanFragmentId("4")),
                                                    ImmutableList.of(buildSymbol2, symbol12),
                                                    Optional.empty(),
                                                    REPARTITION,
                                                    TASK))
                                            .addInputsSet(ImmutableList.of(buildSymbol2, symbol12))
                                            .fixedHashDistributionPartitioningScheme(
                                                    ImmutableList.of(buildSymbol, symbol1),
                                                    ImmutableList.of(buildSymbol))
                                            .type(REPARTITION)
                                            .scope(LOCAL)))),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertWithoutPartialAggAndMultipleSources(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceA = "buildRemoteSourceA";
        String buildRemoteSourceB = "buildRemoteSourceB";
        String probeRemoteSourceA = "probeRemoteSourceA";
        String probeRemoteSourceB = "probeRemoteSourceB";
        return ruleTester.assertThat(new AdaptiveReorderPartitionedJoin(ruleTester.getMetadata()))
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .overrideStats("buildRemoteSourceA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("buildRemoteSourceB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(buildRowCount)
                        .build())
                .overrideStats("probeRemoteSourceA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .overrideStats("probeRemoteSourceB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(probeRowCount)
                        .build())
                .on(p -> {
                    Symbol buildSymbol = p.symbol("buildSymbol", BIGINT);
                    Symbol symbol1 = p.symbol("symbol1", BIGINT);
                    Symbol buildSymbol1 = p.symbol("buildSymbol1", BIGINT);
                    Symbol symbol11 = p.symbol("symbol11", BIGINT);
                    Symbol buildSymbol2 = p.symbol("buildSymbol2", BIGINT);
                    Symbol symbol12 = p.symbol("symbol12", BIGINT);

                    Symbol probeSymbol = p.symbol("probeSymbol", BIGINT);
                    Symbol symbol2 = p.symbol("symbol2", BIGINT);
                    Symbol probeSymbol1 = p.symbol("probeSymbol1", BIGINT);
                    Symbol symbol21 = p.symbol("symbol21", BIGINT);
                    Symbol probeSymbol2 = p.symbol("probeSymbol2", BIGINT);
                    Symbol symbol22 = p.symbol("symbol22", BIGINT);
                    return p.join(
                            INNER,
                            PARTITIONED,
                            p.exchange(builder -> builder
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(probeRemoteSourceA),
                                            ImmutableList.of(new PlanFragmentId("1")),
                                            ImmutableList.of(probeSymbol1, symbol21),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(probeSymbol1, symbol21))
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(probeRemoteSourceB),
                                            ImmutableList.of(new PlanFragmentId("2")),
                                            ImmutableList.of(probeSymbol2, symbol22),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(probeSymbol2, symbol22))
                                    .fixedArbitraryDistributionPartitioningScheme(ImmutableList.of(probeSymbol, symbol2), 2)
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            p.exchange(builder -> builder
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(buildRemoteSourceA),
                                            ImmutableList.of(new PlanFragmentId("3")),
                                            ImmutableList.of(buildSymbol1, symbol11),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(buildSymbol1, symbol11))
                                    .addSource(p.remoteSource(
                                            new PlanNodeId(buildRemoteSourceB),
                                            ImmutableList.of(new PlanFragmentId("4")),
                                            ImmutableList.of(buildSymbol2, symbol12),
                                            Optional.empty(),
                                            REPARTITION,
                                            TASK))
                                    .addInputsSet(ImmutableList.of(buildSymbol2, symbol12))
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(buildSymbol, symbol1),
                                            ImmutableList.of(buildSymbol))
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }

    private RuleAssert assertWithoutPartialAgg(double buildRowCount, double probeRowCount)
    {
        RuleTester ruleTester = tester();
        String buildRemoteSourceId = "buildRemoteSourceId";
        String probeRemoteSourceId = "probeRemoteSourceId";
        return ruleTester.assertThat(new AdaptiveReorderPartitionedJoin(ruleTester.getMetadata()))
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
                            PARTITIONED,
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
                                            REPARTITION,
                                            TASK))
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(buildSymbol, symbol1),
                                            ImmutableList.of(buildSymbol))
                                    .type(REPARTITION)
                                    .scope(LOCAL)),
                            new JoinNode.EquiJoinClause(probeSymbol, buildSymbol));
                });
    }
}
