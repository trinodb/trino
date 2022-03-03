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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.operator.BlockedReason;
import io.trino.spi.QueryId;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.resourcegroups.QueryType;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestBasicQueryInfo
{
    @Test
    public void testConstructor()
    {
        BasicQueryInfo basicInfo = new BasicQueryInfo(
                new QueryInfo(
                        new QueryId("0"),
                        TEST_SESSION.toSessionRepresentation(),
                        RUNNING,
                        false,
                        URI.create("1"),
                        ImmutableList.of("2", "3"),
                        "SELECT 4",
                        Optional.empty(),
                        new QueryStats(
                                DateTime.parse("2991-09-06T05:00-05:30"),
                                DateTime.parse("2991-09-06T05:01-05:30"),
                                DateTime.parse("2991-09-06T05:02-05:30"),
                                DateTime.parse("2991-09-06T06:00-05:30"),
                                new Duration(18, MINUTES),
                                new Duration(17, MINUTES),
                                new Duration(135, MINUTES),
                                new Duration(135, MINUTES),
                                new Duration(144, MINUTES),
                                new Duration(19, MINUTES),
                                new Duration(199, SECONDS),
                                new Duration(112, MINUTES),
                                113,
                                114,
                                115,
                                116,
                                117,
                                118,
                                134,
                                119,
                                120.0,
                                DataSize.valueOf("121GB"),
                                DataSize.valueOf("122GB"),
                                DataSize.valueOf("123GB"),
                                DataSize.valueOf("124GB"),
                                DataSize.valueOf("125GB"),
                                DataSize.valueOf("126GB"),
                                DataSize.valueOf("127GB"),
                                DataSize.valueOf("128GB"),
                                DataSize.valueOf("129GB"),
                                true,
                                new Duration(123, MINUTES),
                                new Duration(124, MINUTES),
                                new Duration(126, MINUTES),
                                true,
                                ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                                DataSize.valueOf("1271GB"),
                                1281,
                                new Duration(120, MINUTES),
                                DataSize.valueOf("1272GB"),
                                1282,
                                DataSize.valueOf("127GB"),
                                128,
                                DataSize.valueOf("129GB"),
                                130,
                                DataSize.valueOf("131GB"),
                                132,
                                DataSize.valueOf("132GB"),
                                ImmutableList.of(new StageGcStatistics(
                                        1101,
                                        1102,
                                        1103,
                                        1104,
                                        1105,
                                        1106,
                                        1107)),
                                DynamicFiltersStats.EMPTY,
                                ImmutableList.of()),
                        new QueryStats(
                                DateTime.parse("1991-09-06T05:00-05:30"),
                                DateTime.parse("1991-09-06T05:01-05:30"),
                                DateTime.parse("1991-09-06T05:02-05:30"),
                                DateTime.parse("1991-09-06T06:00-05:30"),
                                new Duration(8, MINUTES),
                                new Duration(7, MINUTES),
                                new Duration(35, MINUTES),
                                new Duration(35, MINUTES),
                                new Duration(44, MINUTES),
                                new Duration(9, MINUTES),
                                new Duration(99, SECONDS),
                                new Duration(12, MINUTES),
                                13,
                                14,
                                15,
                                16,
                                17,
                                18,
                                34,
                                19,
                                20.0,
                                DataSize.valueOf("21GB"),
                                DataSize.valueOf("22GB"),
                                DataSize.valueOf("23GB"),
                                DataSize.valueOf("24GB"),
                                DataSize.valueOf("25GB"),
                                DataSize.valueOf("26GB"),
                                DataSize.valueOf("27GB"),
                                DataSize.valueOf("28GB"),
                                DataSize.valueOf("29GB"),
                                true,
                                new Duration(23, MINUTES),
                                new Duration(24, MINUTES),
                                new Duration(26, MINUTES),
                                true,
                                ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                                DataSize.valueOf("271GB"),
                                281,
                                new Duration(20, MINUTES),
                                DataSize.valueOf("272GB"),
                                282,
                                DataSize.valueOf("27GB"),
                                28,
                                DataSize.valueOf("29GB"),
                                30,
                                DataSize.valueOf("31GB"),
                                32,
                                DataSize.valueOf("32GB"),
                                ImmutableList.of(new StageGcStatistics(
                                        101,
                                        102,
                                        103,
                                        104,
                                        105,
                                        106,
                                        107)),
                                DynamicFiltersStats.EMPTY,
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        ImmutableSet.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        false,
                        "33",
                        Optional.empty(),
                        null,
                        StandardErrorCode.ABANDONED_QUERY.toErrorCode(),
                        ImmutableList.of(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.of(QueryType.SELECT)));

        assertEquals(basicInfo.getQueryId().getId(), "0");
        assertEquals(basicInfo.getState(), RUNNING);
        assertEquals(basicInfo.isScheduled(), false);
        assertEquals(basicInfo.getQuery(), "SELECT 4");
        assertEquals(basicInfo.getQueryType().get(), QueryType.SELECT);

        assertEquals(basicInfo.getQueryStats().getCreateTime(), DateTime.parse("2991-09-06T05:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getEndTime(), DateTime.parse("2991-09-06T06:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getElapsedTime(), new Duration(18, MINUTES));
        assertEquals(basicInfo.getQueryStats().getExecutionTime(), new Duration(144, MINUTES));

        assertEquals(basicInfo.getQueryStats().getTotalDrivers(), 116);
        assertEquals(basicInfo.getQueryStats().getQueuedDrivers(), 117);
        assertEquals(basicInfo.getQueryStats().getRunningDrivers(), 118);
        assertEquals(basicInfo.getQueryStats().getCompletedDrivers(), 119);

        assertEquals(basicInfo.getQueryStats().getCumulativeUserMemory(), 120.0);
        assertEquals(basicInfo.getQueryStats().getUserMemoryReservation(), DataSize.valueOf("121GB"));
        assertEquals(basicInfo.getQueryStats().getTotalMemoryReservation(), DataSize.valueOf("123GB"));
        assertEquals(basicInfo.getQueryStats().getPeakUserMemoryReservation(), DataSize.valueOf("124GB"));
        assertEquals(basicInfo.getQueryStats().getTotalCpuTime(), new Duration(124, MINUTES));

        assertEquals(basicInfo.getQueryStats().isFullyBlocked(), true);
        assertEquals(basicInfo.getQueryStats().getBlockedReasons(), ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY));

        assertEquals(basicInfo.getQueryStats().getProgressPercentage(), OptionalDouble.of(100));

        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getCreateTime(), DateTime.parse("1991-09-06T05:00-05:30"));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getEndTime(), DateTime.parse("1991-09-06T06:00-05:30"));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getElapsedTime(), new Duration(8, MINUTES));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getExecutionTime(), new Duration(44, MINUTES));

        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getTotalDrivers(), 16);
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getQueuedDrivers(), 17);
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getRunningDrivers(), 18);
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getCompletedDrivers(), 19);

        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getCumulativeUserMemory(), 20.0);
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getUserMemoryReservation(), DataSize.valueOf("21GB"));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getTotalMemoryReservation(), DataSize.valueOf("23GB"));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getPeakUserMemoryReservation(), DataSize.valueOf("24GB"));
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getTotalCpuTime(), new Duration(24, MINUTES));

        assertEquals(basicInfo.getQueryStatsNoFailedTasks().isFullyBlocked(), true);
        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getBlockedReasons(), ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY));

        assertEquals(basicInfo.getQueryStatsNoFailedTasks().getProgressPercentage(), OptionalDouble.of(100));

        assertEquals(basicInfo.getErrorCode(), StandardErrorCode.ABANDONED_QUERY.toErrorCode());
        assertEquals(basicInfo.getErrorType(), StandardErrorCode.ABANDONED_QUERY.toErrorCode().getType());
    }
}
