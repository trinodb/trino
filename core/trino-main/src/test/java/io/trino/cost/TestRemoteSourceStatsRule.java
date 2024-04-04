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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableLongArray;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingMetadata.TestingColumnHandle;
import static java.lang.Double.NaN;

public class TestRemoteSourceStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsRule()
    {
        assertRemoteSourceStats(0.1, 3325.333333);
    }

    @Test
    public void testStatsRuleWithNaNNullFraction()
    {
        // NaN null fraction is replaced with 0.0
        assertRemoteSourceStats(NaN, 2992);
    }

    private void assertRemoteSourceStats(double nullFraction, double avgRowSize)
    {
        StatsAndCosts statsAndCosts = createStatsAndCosts(nullFraction);
        tester().assertStatsFor(pb -> pb
                        .remoteSource(
                                ImmutableList.of(new PlanFragmentId("fragment")),
                                ImmutableList.of(
                                        pb.symbol("col_a", VARCHAR),
                                        pb.symbol("col_b", VARCHAR),
                                        pb.symbol("col_c", BIGINT),
                                        pb.symbol("col_d", DOUBLE)),
                                Optional.empty(),
                                REPARTITION,
                                TASK))
                .withRuntimeInfoProvider(createRuntimeInfoProvider(statsAndCosts))
                .check(check -> check
                        .outputRowsCount(1_000_000)
                        .symbolStats(new Symbol(VARCHAR, "col_a"), assertion -> assertion
                                .averageRowSize(avgRowSize)
                                .distinctValuesCount(100)
                                .nullsFraction(nullFraction)
                                .lowValueUnknown()
                                .highValueUnknown())
                        .symbolStats(new Symbol(VARCHAR, "col_b"), assertion -> assertion
                                .averageRowSize(avgRowSize)
                                .distinctValuesCount(233)
                                .nullsFraction(nullFraction)
                                .lowValueUnknown()
                                .highValueUnknown())
                        .symbolStats(new Symbol(BIGINT, "col_c"), assertion -> assertion
                                .averageRowSize(NaN)
                                .distinctValuesCount(98)
                                .nullsFraction(nullFraction)
                                .highValue(100)
                                .lowValue(3))
                        .symbolStats(new Symbol(DOUBLE, "col_d"), assertion -> assertion
                                .averageRowSize(NaN)
                                .distinctValuesCount(300)
                                .nullsFraction(nullFraction)
                                .highValue(100)
                                .lowValue(3)));
    }

    private RuntimeInfoProvider createRuntimeInfoProvider(StatsAndCosts statsAndCosts)
    {
        PlanFragment planFragment = createPlanFragment(statsAndCosts);
        return new StaticRuntimeInfoProvider(
                ImmutableMap.of(planFragment.getId(), createRuntimeOutputStatsEstimate()),
                ImmutableMap.of(planFragment.getId(), planFragment));
    }

    private OutputStatsEstimateResult createRuntimeOutputStatsEstimate()
    {
        return new OutputStatsEstimateResult(ImmutableLongArray.of(1_000_000_000L, 2_000_000_000L, 3_000_000_000L), 1_000_000L, "FINISHED", true);
    }

    private PlanFragment createPlanFragment(StatsAndCosts statsAndCosts)
    {
        return new PlanFragment(
                new PlanFragmentId("fragment"),
                TableScanNode.newInstance(
                        new PlanNodeId("plan_id"),
                        TEST_TABLE_HANDLE,
                        ImmutableList.of(new Symbol(VARCHAR, "col_a"), new Symbol(VARCHAR, "col_b"), new Symbol(BIGINT, "col_c"), new Symbol(DOUBLE, "col_d")),
                        ImmutableMap.of(
                                new Symbol(VARCHAR, "col_a"), new TestingColumnHandle("col_a", 0, VARCHAR),
                                new Symbol(VARCHAR, "col_b"), new TestingColumnHandle("col_b", 1, VARCHAR),
                                new Symbol(BIGINT, "col_c"), new TestingColumnHandle("col_c", 2, BIGINT),
                                new Symbol(DOUBLE, "col_d"), new TestingColumnHandle("col_d", 3, DOUBLE)),
                        false,
                        Optional.empty()),
                ImmutableSet.of(
                        new Symbol(VARCHAR, "col_a"),
                        new Symbol(VARCHAR, "col_b"),
                        new Symbol(BIGINT, "col_c"),
                        new Symbol(DOUBLE, "col_d")),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(new PlanNodeId("plan_id")),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(new Symbol(BIGINT, "col_c"))),
                statsAndCosts,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private StatsAndCosts createStatsAndCosts(double nullFraction)
    {
        PlanNodeStatsEstimate symbolEstimate = new PlanNodeStatsEstimate(10000,
                ImmutableMap.of(
                        new Symbol(VARCHAR, "col_a"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(nullFraction)
                                .setDistinctValuesCount(100)
                                .build(),
                        new Symbol(VARCHAR, "col_b"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(nullFraction)
                                .setDistinctValuesCount(233)
                                .build(),
                        new Symbol(BIGINT, "col_c"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(nullFraction)
                                .setDistinctValuesCount(98)
                                .setHighValue(100)
                                .setLowValue(3)
                                .build(),
                        new Symbol(DOUBLE, "col_d"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(nullFraction)
                                .setDistinctValuesCount(300)
                                .setHighValue(100)
                                .setLowValue(3)
                                .build()));
        return new StatsAndCosts(ImmutableMap.of(new PlanNodeId("plan_id"), symbolEstimate), ImmutableMap.of());
    }
}
