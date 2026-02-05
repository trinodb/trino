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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.OptionalInt;

import static io.trino.operator.TestPipelineStats.assertExpectedPipelineStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskStats
{
    public static final TaskStats EXPECTED = new TaskStats(
            Instant.ofEpochMilli(1),
            Instant.ofEpochMilli(2),
            Instant.ofEpochMilli(100),
            Instant.ofEpochMilli(102),
            Instant.ofEpochMilli(101),
            Instant.ofEpochMilli(3),
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
            DataSize.ofBytes(14),
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

            DataSize.ofBytes(21),
            22,

            new Duration(271, NANOSECONDS),

            DataSize.ofBytes(23),
            24,

            new Duration(272, NANOSECONDS),

            DataSize.ofBytes(25),
            DataSize.ofBytes(25),
            OptionalInt.of(2),

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
        assertThat(actual.createTime()).isEqualTo(Instant.ofEpochMilli(1));
        assertThat(actual.firstStartTime()).isEqualTo(Instant.ofEpochMilli(2));
        assertThat(actual.lastStartTime()).isEqualTo(Instant.ofEpochMilli(100));
        assertThat(actual.terminatingStartTime()).isEqualTo(Instant.ofEpochMilli(102));
        assertThat(actual.lastEndTime()).isEqualTo(Instant.ofEpochMilli(101));
        assertThat(actual.endTime()).isEqualTo(Instant.ofEpochMilli(3));
        assertThat(actual.elapsedTime()).isEqualTo(new Duration(4, NANOSECONDS));
        assertThat(actual.queuedTime()).isEqualTo(new Duration(5, NANOSECONDS));

        assertThat(actual.totalDrivers()).isEqualTo(6);
        assertThat(actual.queuedDrivers()).isEqualTo(7);
        assertThat(actual.queuedPartitionedDrivers()).isEqualTo(5);
        assertThat(actual.queuedPartitionedSplitsWeight()).isEqualTo(28L);
        assertThat(actual.runningDrivers()).isEqualTo(8);
        assertThat(actual.runningPartitionedDrivers()).isEqualTo(6);
        assertThat(actual.runningPartitionedSplitsWeight()).isEqualTo(29L);
        assertThat(actual.blockedDrivers()).isEqualTo(24);
        assertThat(actual.completedDrivers()).isEqualTo(10);

        assertThat(actual.cumulativeUserMemory()).isEqualTo(11.0);
        assertThat(actual.userMemoryReservation()).isEqualTo(DataSize.ofBytes(12));
        assertThat(actual.peakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(120));
        assertThat(actual.revocableMemoryReservation()).isEqualTo(DataSize.ofBytes(13));

        assertThat(actual.spilledDataSize()).isEqualTo(DataSize.ofBytes(14));

        assertThat(actual.totalScheduledTime()).isEqualTo(new Duration(15, NANOSECONDS));
        assertThat(actual.totalCpuTime()).isEqualTo(new Duration(16, NANOSECONDS));
        assertThat(actual.totalBlockedTime()).isEqualTo(new Duration(18, NANOSECONDS));

        assertThat(actual.physicalInputDataSize()).isEqualTo(DataSize.ofBytes(191));
        assertThat(actual.physicalInputPositions()).isEqualTo(201);
        assertThat(actual.physicalInputReadTime()).isEqualTo(new Duration(15, NANOSECONDS));
        assertThat(actual.internalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(192));
        assertThat(actual.internalNetworkInputPositions()).isEqualTo(202);

        assertThat(actual.processedInputDataSize()).isEqualTo(DataSize.ofBytes(21));
        assertThat(actual.processedInputPositions()).isEqualTo(22);

        assertThat(actual.inputBlockedTime()).isEqualTo(new Duration(271, NANOSECONDS));

        assertThat(actual.outputDataSize()).isEqualTo(DataSize.ofBytes(23));
        assertThat(actual.outputPositions()).isEqualTo(24);

        assertThat(actual.outputBlockedTime()).isEqualTo(new Duration(272, NANOSECONDS));

        assertThat(actual.physicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(25));

        assertThat(actual.pipelines()).hasSize(1);
        assertExpectedPipelineStats(actual.pipelines().get(0));
    }
}
