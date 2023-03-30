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
import io.trino.client.NodeVersion;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.operator.BlockedReason;
import io.trino.operator.RetryPolicy;
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
import static org.testng.Assert.assertTrue;

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
                        URI.create("1"),
                        ImmutableList.of("2", "3"),
                        "SELECT 4",
                        Optional.empty(),
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
                                19,
                                34,
                                20,
                                21.0,
                                22.0,
                                DataSize.valueOf("23GB"),
                                DataSize.valueOf("24GB"),
                                DataSize.valueOf("25GB"),
                                DataSize.valueOf("26GB"),
                                DataSize.valueOf("27GB"),
                                DataSize.valueOf("28GB"),
                                DataSize.valueOf("29GB"),
                                DataSize.valueOf("30GB"),
                                DataSize.valueOf("31GB"),
                                true,
                                new Duration(32, MINUTES),
                                new Duration(33, MINUTES),
                                new Duration(34, MINUTES),
                                new Duration(35, MINUTES),
                                new Duration(36, MINUTES),
                                true,
                                ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                                DataSize.valueOf("271GB"),
                                DataSize.valueOf("271GB"),
                                281,
                                281,
                                new Duration(37, MINUTES),
                                new Duration(38, MINUTES),
                                DataSize.valueOf("272GB"),
                                DataSize.valueOf("272GB"),
                                282,
                                282,
                                DataSize.valueOf("39GB"),
                                DataSize.valueOf("40GB"),
                                41,
                                42,
                                DataSize.valueOf("43GB"),
                                DataSize.valueOf("44GB"),
                                45,
                                46,
                                new Duration(101, SECONDS),
                                new Duration(102, SECONDS),
                                DataSize.valueOf("47GB"),
                                DataSize.valueOf("48GB"),
                                49,
                                50,
                                new Duration(103, SECONDS),
                                new Duration(104, SECONDS),
                                DataSize.valueOf("51GB"),
                                DataSize.valueOf("52GB"),
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
                        Optional.of(QueryType.SELECT),
                        RetryPolicy.NONE,
                        false,
                        new NodeVersion("test")));

        assertEquals(basicInfo.getQueryId().getId(), "0");
        assertEquals(basicInfo.getState(), RUNNING);
        assertTrue(basicInfo.isScheduled()); // from query stats
        assertEquals(basicInfo.getQuery(), "SELECT 4");
        assertEquals(basicInfo.getQueryType().get(), QueryType.SELECT);

        assertEquals(basicInfo.getQueryStats().getCreateTime(), DateTime.parse("1991-09-06T05:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getEndTime(), DateTime.parse("1991-09-06T06:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getElapsedTime(), new Duration(8, MINUTES));
        assertEquals(basicInfo.getQueryStats().getExecutionTime(), new Duration(44, MINUTES));

        assertEquals(basicInfo.getQueryStats().getTotalDrivers(), 17);
        assertEquals(basicInfo.getQueryStats().getQueuedDrivers(), 18);
        assertEquals(basicInfo.getQueryStats().getRunningDrivers(), 19);
        assertEquals(basicInfo.getQueryStats().getCompletedDrivers(), 20);

        assertEquals(basicInfo.getQueryStats().getCumulativeUserMemory(), 21.0);
        assertEquals(basicInfo.getQueryStats().getFailedCumulativeUserMemory(), 22.0);
        assertEquals(basicInfo.getQueryStats().getUserMemoryReservation(), DataSize.valueOf("23GB"));
        assertEquals(basicInfo.getQueryStats().getTotalMemoryReservation(), DataSize.valueOf("25GB"));
        assertEquals(basicInfo.getQueryStats().getPeakUserMemoryReservation(), DataSize.valueOf("26GB"));
        assertEquals(basicInfo.getQueryStats().getTotalScheduledTime(), new Duration(32, MINUTES));
        assertEquals(basicInfo.getQueryStats().getFailedScheduledTime(), new Duration(33, MINUTES));
        assertEquals(basicInfo.getQueryStats().getTotalCpuTime(), new Duration(34, MINUTES));
        assertEquals(basicInfo.getQueryStats().getFailedCpuTime(), new Duration(35, MINUTES));

        assertEquals(basicInfo.getQueryStats().isFullyBlocked(), true);
        assertEquals(basicInfo.getQueryStats().getBlockedReasons(), ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY));

        assertEquals(basicInfo.getQueryStats().getProgressPercentage(), OptionalDouble.of(100));

        assertEquals(basicInfo.getErrorCode(), StandardErrorCode.ABANDONED_QUERY.toErrorCode());
        assertEquals(basicInfo.getErrorType(), StandardErrorCode.ABANDONED_QUERY.toErrorCode().getType());
    }
}
