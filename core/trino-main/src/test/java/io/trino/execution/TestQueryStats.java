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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

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
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getExecutionStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getLastHeartbeat(), new DateTime(3, UTC));
        assertEquals(actual.getEndTime(), new DateTime(4, UTC));

        assertEquals(actual.getElapsedTime(), new Duration(6, NANOSECONDS));
        assertEquals(actual.getQueuedTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getResourceWaitingTime(), new Duration(31, NANOSECONDS));
        assertEquals(actual.getDispatchingTime(), new Duration(32, NANOSECONDS));
        assertEquals(actual.getExecutionTime(), new Duration(41, NANOSECONDS));
        assertEquals(actual.getAnalysisTime(), new Duration(33, NANOSECONDS));

        assertEquals(actual.getPlanningTime(), new Duration(100, NANOSECONDS));
        assertEquals(actual.getPlanningCpuTime(), new Duration(150, NANOSECONDS));
        assertEquals(actual.getFinishingTime(), new Duration(200, NANOSECONDS));

        assertEquals(actual.getTotalTasks(), 9);
        assertEquals(actual.getRunningTasks(), 10);
        assertEquals(actual.getCompletedTasks(), 11);
        assertEquals(actual.getFailedTasks(), 12);

        assertEquals(actual.getTotalDrivers(), 13);
        assertEquals(actual.getQueuedDrivers(), 14);
        assertEquals(actual.getRunningDrivers(), 15);
        assertEquals(actual.getBlockedDrivers(), 30);
        assertEquals(actual.getCompletedDrivers(), 16);

        assertEquals(actual.getCumulativeUserMemory(), 17.0);
        assertEquals(actual.getFailedCumulativeUserMemory(), 18.0);
        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(19));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(20));
        assertEquals(actual.getTotalMemoryReservation(), DataSize.ofBytes(21));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(22));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(23));
        assertEquals(actual.getPeakTotalMemoryReservation(), DataSize.ofBytes(24));
        assertEquals(actual.getPeakTaskUserMemory(), DataSize.ofBytes(25));
        assertEquals(actual.getPeakTaskRevocableMemory(), DataSize.ofBytes(26));
        assertEquals(actual.getPeakTaskTotalMemory(), DataSize.ofBytes(27));
        assertEquals(actual.getSpilledDataSize(), DataSize.ofBytes(693));

        assertEquals(actual.getTotalScheduledTime(), new Duration(28, NANOSECONDS));
        assertEquals(actual.getFailedScheduledTime(), new Duration(29, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(30, NANOSECONDS));
        assertEquals(actual.getFailedCpuTime(), new Duration(31, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(32, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(241));
        assertEquals(actual.getFailedPhysicalInputDataSize(), DataSize.ofBytes(242));
        assertEquals(actual.getPhysicalInputPositions(), 251);
        assertEquals(actual.getFailedPhysicalInputPositions(), 252);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(33, NANOSECONDS));
        assertEquals(actual.getFailedPhysicalInputReadTime(), new Duration(34, NANOSECONDS));

        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(242));
        assertEquals(actual.getFailedInternalNetworkInputDataSize(), DataSize.ofBytes(243));
        assertEquals(actual.getInternalNetworkInputPositions(), 253);
        assertEquals(actual.getFailedInternalNetworkInputPositions(), 254);

        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(35));
        assertEquals(actual.getFailedRawInputDataSize(), DataSize.ofBytes(36));
        assertEquals(actual.getRawInputPositions(), 37);
        assertEquals(actual.getFailedRawInputPositions(), 38);

        assertEquals(actual.getProcessedInputDataSize(), DataSize.ofBytes(39));
        assertEquals(actual.getFailedProcessedInputDataSize(), DataSize.ofBytes(40));
        assertEquals(actual.getProcessedInputPositions(), 41);
        assertEquals(actual.getFailedProcessedInputPositions(), 42);

        assertEquals(actual.getInputBlockedTime(), new Duration(101, SECONDS));
        assertEquals(actual.getFailedInputBlockedTime(), new Duration(102, SECONDS));

        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(43));
        assertEquals(actual.getFailedOutputDataSize(), DataSize.ofBytes(44));
        assertEquals(actual.getOutputPositions(), 45);
        assertEquals(actual.getFailedOutputPositions(), 46);

        assertEquals(actual.getOutputBlockedTime(), new Duration(103, SECONDS));
        assertEquals(actual.getFailedOutputBlockedTime(), new Duration(104, SECONDS));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(47));
        assertEquals(actual.getFailedPhysicalWrittenDataSize(), DataSize.ofBytes(48));

        assertEquals(actual.getStageGcStatistics().size(), 1);
        StageGcStatistics gcStatistics = actual.getStageGcStatistics().get(0);
        assertEquals(gcStatistics.getStageId(), 101);
        assertEquals(gcStatistics.getTasks(), 102);
        assertEquals(gcStatistics.getFullGcTasks(), 103);
        assertEquals(gcStatistics.getMinFullGcSec(), 104);
        assertEquals(gcStatistics.getMaxFullGcSec(), 105);
        assertEquals(gcStatistics.getTotalFullGcSec(), 106);
        assertEquals(gcStatistics.getAverageFullGcSec(), 107);

        assertEquals(420, actual.getWrittenPositions());
        assertEquals(58, actual.getLogicalWrittenDataSize().toBytes());

        assertEquals(DynamicFiltersStats.EMPTY, actual.getDynamicFiltersStats());
        assertEquals(actual.getOptimizerRulesSummaries().size(), optimizerRulesSummaries.size());
        for (int i = 0, end = optimizerRulesSummaries.size(); i < end; i++) {
            QueryPlanOptimizerStatistics actualRule = actual.getOptimizerRulesSummaries().get(i);
            QueryPlanOptimizerStatistics expectedRule = optimizerRulesSummaries.get(i);

            assertEquals(actualRule.rule(), expectedRule.rule());
            assertEquals(actualRule.applied(), expectedRule.applied());
            assertEquals(actualRule.totalTime(), expectedRule.totalTime());
            assertEquals(actualRule.invocations(), expectedRule.invocations());
            assertEquals(actualRule.failures(), expectedRule.failures());
        }
    }
}
