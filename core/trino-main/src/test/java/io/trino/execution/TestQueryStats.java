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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.OperatorStats;
import io.trino.operator.TableWriterOperator;
import io.trino.spi.eventlistener.QueryPlanOptimizerStatistics;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestQueryStats
{
    public static final List<OperatorStats> operatorSummaries = ImmutableList.of(
            new OperatorStats(
                    10,
                    11,
                    12,
                    new PlanNodeId("13"),
                    TableWriterOperator.class.getSimpleName(),
                    14L,
                    15L,
                    new Duration(16, NANOSECONDS),
                    new Duration(17, NANOSECONDS),
                    succinctBytes(181L),
                    1811,
                    new Duration(18, NANOSECONDS),
                    succinctBytes(182L),
                    1822,
                    succinctBytes(18L),
                    succinctBytes(19L),
                    110L,
                    111.0,
                    112L,
                    new Duration(113, NANOSECONDS),
                    new Duration(114, NANOSECONDS),
                    succinctBytes(116L),
                    117L,
                    1833,
                    Metrics.EMPTY,
                    Metrics.EMPTY,
                    succinctBytes(118L),
                    new Duration(119, NANOSECONDS),
                    120L,
                    new Duration(121, NANOSECONDS),
                    new Duration(122, NANOSECONDS),
                    succinctBytes(124L),
                    succinctBytes(125L),
                    succinctBytes(127L),
                    succinctBytes(128L),
                    succinctBytes(130L),
                    succinctBytes(131L),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    20,
                    21,
                    22,
                    new PlanNodeId("23"),
                    FilterAndProjectOperator.class.getSimpleName(),
                    24L,
                    25L,
                    new Duration(26, NANOSECONDS),
                    new Duration(27, NANOSECONDS),
                    succinctBytes(281L),
                    2811,
                    new Duration(28, NANOSECONDS),
                    succinctBytes(282L),
                    2822,
                    succinctBytes(28L),
                    succinctBytes(29L),
                    210L,
                    211.0,
                    212L,
                    new Duration(213, NANOSECONDS),
                    new Duration(214, NANOSECONDS),
                    succinctBytes(216L),
                    217L,
                    2833,
                    Metrics.EMPTY,
                    Metrics.EMPTY,
                    succinctBytes(218L),
                    new Duration(219, NANOSECONDS),
                    220L,
                    new Duration(221, NANOSECONDS),
                    new Duration(222, NANOSECONDS),
                    succinctBytes(224L),
                    succinctBytes(225L),
                    succinctBytes(227L),
                    succinctBytes(228L),
                    succinctBytes(230L),
                    succinctBytes(231L),
                    Optional.empty(),
                    null),
            new OperatorStats(
                    30,
                    31,
                    32,
                    new PlanNodeId("33"),
                    TableWriterOperator.class.getSimpleName(),
                    34L,
                    35L,
                    new Duration(36, NANOSECONDS),
                    new Duration(37, NANOSECONDS),
                    succinctBytes(381L),
                    3811,
                    new Duration(38, NANOSECONDS),
                    succinctBytes(382L),
                    3822,
                    succinctBytes(38L),
                    succinctBytes(39L),
                    310L,
                    311.0,
                    312L,
                    new Duration(313, NANOSECONDS),
                    new Duration(314, NANOSECONDS),
                    succinctBytes(316L),
                    317L,
                    3833,
                    Metrics.EMPTY,
                    Metrics.EMPTY,
                    succinctBytes(318L),
                    new Duration(319, NANOSECONDS),
                    320L,
                    new Duration(321, NANOSECONDS),
                    new Duration(322, NANOSECONDS),
                    succinctBytes(324L),
                    succinctBytes(325L),
                    succinctBytes(327L),
                    succinctBytes(328L),
                    succinctBytes(329L),
                    succinctBytes(331L),
                    Optional.empty(),
                    null));

    private static final List<QueryPlanOptimizerStatistics> optimizerRulesSummaries = ImmutableList.of(
            new QueryPlanOptimizerStatistics("io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan",
                    10L,
                    1L,
                    4600,
                    0),
            new QueryPlanOptimizerStatistics("io.trino.sql.planner.iterative.rule.PushTopNThroughUnion",
                    5L,
                    0L,
                    499,
                    0));

    public static final QueryStats EXPECTED = new QueryStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),
            new DateTime(4),
            new Duration(6, NANOSECONDS),
            new Duration(5, NANOSECONDS),
            new Duration(31, NANOSECONDS),
            new Duration(32, NANOSECONDS),
            new Duration(41, NANOSECONDS),
            new Duration(33, NANOSECONDS),

            new Duration(100, NANOSECONDS),
            new Duration(150, NANOSECONDS),
            new Duration(200, NANOSECONDS),

            9,
            10,
            11,
            12,

            13,
            14,
            15,
            30,
            16,

            17.0,
            18.0,
            DataSize.ofBytes(19),
            DataSize.ofBytes(20),
            DataSize.ofBytes(21),
            DataSize.ofBytes(22),
            DataSize.ofBytes(23),
            DataSize.ofBytes(24),
            DataSize.ofBytes(25),
            DataSize.ofBytes(26),
            DataSize.ofBytes(27),

            true,
            OptionalDouble.of(8.88),
            OptionalDouble.of(0),
            new Duration(28, NANOSECONDS),
            new Duration(29, NANOSECONDS),
            new Duration(30, NANOSECONDS),
            new Duration(31, NANOSECONDS),
            new Duration(32, NANOSECONDS),
            false,
            ImmutableSet.of(),

            DataSize.ofBytes(241),
            DataSize.ofBytes(242),
            251,
            252,
            new Duration(33, NANOSECONDS),
            new Duration(34, NANOSECONDS),

            DataSize.ofBytes(242),
            DataSize.ofBytes(243),
            253,
            254,

            DataSize.ofBytes(35),
            DataSize.ofBytes(36),
            37,
            38,

            DataSize.ofBytes(39),
            DataSize.ofBytes(40),
            41,
            42,

            new Duration(101, SECONDS),
            new Duration(102, SECONDS),

            DataSize.ofBytes(43),
            DataSize.ofBytes(44),
            45,
            46,

            new Duration(103, SECONDS),
            new Duration(104, SECONDS),

            DataSize.ofBytes(47),
            DataSize.ofBytes(48),

            ImmutableList.of(new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107)),

            DynamicFiltersStats.EMPTY,

            operatorSummaries,
            optimizerRulesSummaries);

    @Test
    public void testJson()
    {
        JsonCodec<QueryStats> codec = JsonCodec.jsonCodec(QueryStats.class);

        String json = codec.toJson(EXPECTED);
        QueryStats actual = codec.fromJson(json);

        assertExpectedQueryStats(actual);
    }

    public static void assertExpectedQueryStats(QueryStats actual)
    {
        assertThat(actual.getCreateTime()).isEqualTo(new DateTime(1, UTC));
        assertThat(actual.getExecutionStartTime()).isEqualTo(new DateTime(2, UTC));
        assertThat(actual.getLastHeartbeat()).isEqualTo(new DateTime(3, UTC));
        assertThat(actual.getEndTime()).isEqualTo(new DateTime(4, UTC));

        assertThat(actual.getElapsedTime()).isEqualTo(new Duration(6, NANOSECONDS));
        assertThat(actual.getQueuedTime()).isEqualTo(new Duration(5, NANOSECONDS));
        assertThat(actual.getResourceWaitingTime()).isEqualTo(new Duration(31, NANOSECONDS));
        assertThat(actual.getDispatchingTime()).isEqualTo(new Duration(32, NANOSECONDS));
        assertThat(actual.getExecutionTime()).isEqualTo(new Duration(41, NANOSECONDS));
        assertThat(actual.getAnalysisTime()).isEqualTo(new Duration(33, NANOSECONDS));

        assertThat(actual.getPlanningTime()).isEqualTo(new Duration(100, NANOSECONDS));
        assertThat(actual.getPlanningCpuTime()).isEqualTo(new Duration(150, NANOSECONDS));
        assertThat(actual.getFinishingTime()).isEqualTo(new Duration(200, NANOSECONDS));

        assertThat(actual.getTotalTasks()).isEqualTo(9);
        assertThat(actual.getRunningTasks()).isEqualTo(10);
        assertThat(actual.getCompletedTasks()).isEqualTo(11);
        assertThat(actual.getFailedTasks()).isEqualTo(12);

        assertThat(actual.getTotalDrivers()).isEqualTo(13);
        assertThat(actual.getQueuedDrivers()).isEqualTo(14);
        assertThat(actual.getRunningDrivers()).isEqualTo(15);
        assertThat(actual.getBlockedDrivers()).isEqualTo(30);
        assertThat(actual.getCompletedDrivers()).isEqualTo(16);

        assertThat(actual.getCumulativeUserMemory()).isEqualTo(17.0);
        assertThat(actual.getFailedCumulativeUserMemory()).isEqualTo(18.0);
        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(19));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(20));
        assertThat(actual.getTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(21));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(22));
        assertThat(actual.getPeakRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(23));
        assertThat(actual.getPeakTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(24));
        assertThat(actual.getPeakTaskUserMemory()).isEqualTo(DataSize.ofBytes(25));
        assertThat(actual.getPeakTaskRevocableMemory()).isEqualTo(DataSize.ofBytes(26));
        assertThat(actual.getPeakTaskTotalMemory()).isEqualTo(DataSize.ofBytes(27));
        assertThat(actual.getSpilledDataSize()).isEqualTo(DataSize.ofBytes(693));

        assertThat(actual.getTotalScheduledTime()).isEqualTo(new Duration(28, NANOSECONDS));
        assertThat(actual.getFailedScheduledTime()).isEqualTo(new Duration(29, NANOSECONDS));
        assertThat(actual.getTotalCpuTime()).isEqualTo(new Duration(30, NANOSECONDS));
        assertThat(actual.getFailedCpuTime()).isEqualTo(new Duration(31, NANOSECONDS));
        assertThat(actual.getTotalBlockedTime()).isEqualTo(new Duration(32, NANOSECONDS));

        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(241));
        assertThat(actual.getFailedPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(242));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(251);
        assertThat(actual.getFailedPhysicalInputPositions()).isEqualTo(252);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(33, NANOSECONDS));
        assertThat(actual.getFailedPhysicalInputReadTime()).isEqualTo(new Duration(34, NANOSECONDS));

        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(242));
        assertThat(actual.getFailedInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(243));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(253);
        assertThat(actual.getFailedInternalNetworkInputPositions()).isEqualTo(254);

        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(35));
        assertThat(actual.getFailedRawInputDataSize()).isEqualTo(DataSize.ofBytes(36));
        assertThat(actual.getRawInputPositions()).isEqualTo(37);
        assertThat(actual.getFailedRawInputPositions()).isEqualTo(38);

        assertThat(actual.getProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(39));
        assertThat(actual.getFailedProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(40));
        assertThat(actual.getProcessedInputPositions()).isEqualTo(41);
        assertThat(actual.getFailedProcessedInputPositions()).isEqualTo(42);

        assertThat(actual.getInputBlockedTime()).isEqualTo(new Duration(101, SECONDS));
        assertThat(actual.getFailedInputBlockedTime()).isEqualTo(new Duration(102, SECONDS));

        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(43));
        assertThat(actual.getFailedOutputDataSize()).isEqualTo(DataSize.ofBytes(44));
        assertThat(actual.getOutputPositions()).isEqualTo(45);
        assertThat(actual.getFailedOutputPositions()).isEqualTo(46);

        assertThat(actual.getOutputBlockedTime()).isEqualTo(new Duration(103, SECONDS));
        assertThat(actual.getFailedOutputBlockedTime()).isEqualTo(new Duration(104, SECONDS));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(47));
        assertThat(actual.getFailedPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(48));

        assertThat(actual.getStageGcStatistics().size()).isEqualTo(1);
        StageGcStatistics gcStatistics = actual.getStageGcStatistics().get(0);
        assertThat(gcStatistics.getStageId()).isEqualTo(101);
        assertThat(gcStatistics.getTasks()).isEqualTo(102);
        assertThat(gcStatistics.getFullGcTasks()).isEqualTo(103);
        assertThat(gcStatistics.getMinFullGcSec()).isEqualTo(104);
        assertThat(gcStatistics.getMaxFullGcSec()).isEqualTo(105);
        assertThat(gcStatistics.getTotalFullGcSec()).isEqualTo(106);
        assertThat(gcStatistics.getAverageFullGcSec()).isEqualTo(107);

        assertThat(420).isEqualTo(actual.getWrittenPositions());
        assertThat(58).isEqualTo(actual.getLogicalWrittenDataSize().toBytes());

        assertThat(DynamicFiltersStats.EMPTY).isEqualTo(actual.getDynamicFiltersStats());
        assertThat(actual.getOptimizerRulesSummaries().size()).isEqualTo(optimizerRulesSummaries.size());
        for (int i = 0, end = optimizerRulesSummaries.size(); i < end; i++) {
            QueryPlanOptimizerStatistics actualRule = actual.getOptimizerRulesSummaries().get(i);
            QueryPlanOptimizerStatistics expectedRule = optimizerRulesSummaries.get(i);

            assertThat(actualRule.rule()).isEqualTo(expectedRule.rule());
            assertThat(actualRule.applied()).isEqualTo(expectedRule.applied());
            assertThat(actualRule.totalTime()).isEqualTo(expectedRule.totalTime());
            assertThat(actualRule.invocations()).isEqualTo(expectedRule.invocations());
            assertThat(actualRule.failures()).isEqualTo(expectedRule.failures());
        }
    }
}
