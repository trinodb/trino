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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.operator.TestPipelineStats.assertExpectedPipelineStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestTaskStats
{
    public static final TaskStats EXPECTED = new TaskStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(100),
            new DateTime(102),
            new DateTime(101),
            new DateTime(3),
            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            6,
            7,
            5,
            28L,
            8,
            6,
            29L,
            24,
            10,

            11.0,
            DataSize.ofBytes(12),
            DataSize.ofBytes(120),
            DataSize.ofBytes(13),
            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            false,
            ImmutableSet.of(),

            DataSize.ofBytes(191),
            201,
            new Duration(15, NANOSECONDS),

            DataSize.ofBytes(192),
            202,

            DataSize.ofBytes(19),
            20,

            DataSize.ofBytes(21),
            22,

            new Duration(271, NANOSECONDS),

            DataSize.ofBytes(23),
            24,

            new Duration(272, NANOSECONDS),

            DataSize.ofBytes(25),
            Optional.of(2),

            26,
            new Duration(27, NANOSECONDS),

            ImmutableList.of(TestPipelineStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<TaskStats> codec = JsonCodec.jsonCodec(TaskStats.class);

        String json = codec.toJson(EXPECTED);
        TaskStats actual = codec.fromJson(json);

        assertExpectedTaskStats(actual);
    }

    public static void assertExpectedTaskStats(TaskStats actual)
    {
        assertThat(actual.getCreateTime()).isEqualTo(new DateTime(1, UTC));
        assertThat(actual.getFirstStartTime()).isEqualTo(new DateTime(2, UTC));
        assertThat(actual.getLastStartTime()).isEqualTo(new DateTime(100, UTC));
        assertThat(actual.getTerminatingStartTime()).isEqualTo(new DateTime(102, UTC));
        assertThat(actual.getLastEndTime()).isEqualTo(new DateTime(101, UTC));
        assertThat(actual.getEndTime()).isEqualTo(new DateTime(3, UTC));
        assertThat(actual.getElapsedTime()).isEqualTo(new Duration(4, NANOSECONDS));
        assertThat(actual.getQueuedTime()).isEqualTo(new Duration(5, NANOSECONDS));

        assertThat(actual.getTotalDrivers()).isEqualTo(6);
        assertThat(actual.getQueuedDrivers()).isEqualTo(7);
        assertThat(actual.getQueuedPartitionedDrivers()).isEqualTo(5);
        assertThat(actual.getQueuedPartitionedSplitsWeight()).isEqualTo(28L);
        assertThat(actual.getRunningDrivers()).isEqualTo(8);
        assertThat(actual.getRunningPartitionedDrivers()).isEqualTo(6);
        assertThat(actual.getRunningPartitionedSplitsWeight()).isEqualTo(29L);
        assertThat(actual.getBlockedDrivers()).isEqualTo(24);
        assertThat(actual.getCompletedDrivers()).isEqualTo(10);

        assertThat(actual.getCumulativeUserMemory()).isEqualTo(11.0);
        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(12));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(120));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(13));

        assertThat(actual.getTotalScheduledTime()).isEqualTo(new Duration(15, NANOSECONDS));
        assertThat(actual.getTotalCpuTime()).isEqualTo(new Duration(16, NANOSECONDS));
        assertThat(actual.getTotalBlockedTime()).isEqualTo(new Duration(18, NANOSECONDS));

        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(191));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(201);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(15, NANOSECONDS));
        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(192));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(202);

        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(19));
        assertThat(actual.getRawInputPositions()).isEqualTo(20);

        assertThat(actual.getProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(21));
        assertThat(actual.getProcessedInputPositions()).isEqualTo(22);

        assertThat(actual.getInputBlockedTime()).isEqualTo(new Duration(271, NANOSECONDS));

        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(23));
        assertThat(actual.getOutputPositions()).isEqualTo(24);

        assertThat(actual.getOutputBlockedTime()).isEqualTo(new Duration(272, NANOSECONDS));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(25));

        assertThat(actual.getPipelines()).hasSize(1);
        assertExpectedPipelineStats(actual.getPipelines().get(0));
    }
}
