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
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

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
                                new Duration(1, SECONDS),
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
                                OptionalDouble.of(100),
                                OptionalDouble.of(0),
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
                                ImmutableList.of(),
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
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

        assertThat(basicInfo.getQueryId().getId()).isEqualTo("0");
        assertThat(basicInfo.getState()).isEqualTo(RUNNING);
        assertThat(basicInfo.isScheduled()).isTrue(); // from query stats
        assertThat(basicInfo.getQuery()).isEqualTo("SELECT 4");
        assertThat(basicInfo.getQueryType().get()).isEqualTo(QueryType.SELECT);

        assertThat(basicInfo.getQueryStats().getCreateTime()).isEqualTo(DateTime.parse("1991-09-06T05:00-05:30"));
        assertThat(basicInfo.getQueryStats().getEndTime()).isEqualTo(DateTime.parse("1991-09-06T06:00-05:30"));
        assertThat(basicInfo.getQueryStats().getElapsedTime()).isEqualTo(new Duration(8, MINUTES));
        assertThat(basicInfo.getQueryStats().getExecutionTime()).isEqualTo(new Duration(44, MINUTES));

        assertThat(basicInfo.getQueryStats().getTotalDrivers()).isEqualTo(17);
        assertThat(basicInfo.getQueryStats().getQueuedDrivers()).isEqualTo(18);
        assertThat(basicInfo.getQueryStats().getRunningDrivers()).isEqualTo(19);
        assertThat(basicInfo.getQueryStats().getCompletedDrivers()).isEqualTo(20);

        assertThat(basicInfo.getQueryStats().getCumulativeUserMemory()).isEqualTo(21.0);
        assertThat(basicInfo.getQueryStats().getFailedCumulativeUserMemory()).isEqualTo(22.0);
        assertThat(basicInfo.getQueryStats().getUserMemoryReservation()).isEqualTo(DataSize.valueOf("23GB"));
        assertThat(basicInfo.getQueryStats().getTotalMemoryReservation()).isEqualTo(DataSize.valueOf("25GB"));
        assertThat(basicInfo.getQueryStats().getPeakUserMemoryReservation()).isEqualTo(DataSize.valueOf("26GB"));
        assertThat(basicInfo.getQueryStats().getTotalScheduledTime()).isEqualTo(new Duration(32, MINUTES));
        assertThat(basicInfo.getQueryStats().getFailedScheduledTime()).isEqualTo(new Duration(33, MINUTES));
        assertThat(basicInfo.getQueryStats().getTotalCpuTime()).isEqualTo(new Duration(34, MINUTES));
        assertThat(basicInfo.getQueryStats().getFailedCpuTime()).isEqualTo(new Duration(35, MINUTES));

        assertThat(basicInfo.getQueryStats().isFullyBlocked()).isEqualTo(true);
        assertThat(basicInfo.getQueryStats().getBlockedReasons()).isEqualTo(ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY));

        assertThat(basicInfo.getQueryStats().getProgressPercentage()).isEqualTo(OptionalDouble.of(100));
        assertThat(basicInfo.getQueryStats().getRunningPercentage()).isEqualTo(OptionalDouble.of(0));

        assertThat(basicInfo.getErrorCode()).isEqualTo(StandardErrorCode.ABANDONED_QUERY.toErrorCode());
        assertThat(basicInfo.getErrorType()).isEqualTo(StandardErrorCode.ABANDONED_QUERY.toErrorCode().getType());
    }
}
