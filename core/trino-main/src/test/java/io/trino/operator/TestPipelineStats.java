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
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static io.trino.operator.TestDriverStats.assertExpectedDriverStats;
import static io.trino.operator.TestOperatorStats.assertExpectedOperatorStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestPipelineStats
{
    public static final PipelineStats EXPECTED = new PipelineStats(
            0,

            new DateTime(100),
            new DateTime(101),
            new DateTime(102),

            true,
            false,

            1,
            2,
            1,
            21L,
            3,
            2,
            22L,
            19,
            4,

            DataSize.ofBytes(5),
            DataSize.ofBytes(6),

            getTestDistribution(8),
            getTestDistribution(9),

            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            new Duration(13, NANOSECONDS),
            false,
            ImmutableSet.of(),

            DataSize.ofBytes(141),
            151,
            new Duration(14, NANOSECONDS),

            DataSize.ofBytes(142),
            152,

            DataSize.ofBytes(14),
            15,

            DataSize.ofBytes(16),
            17,

            new Duration(101, NANOSECONDS),

            DataSize.ofBytes(18),
            19,

            new Duration(102, NANOSECONDS),

            DataSize.ofBytes(20),

            ImmutableList.of(TestOperatorStats.EXPECTED),
            ImmutableList.of(TestDriverStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<PipelineStats> codec = JsonCodec.jsonCodec(PipelineStats.class);

        String json = codec.toJson(EXPECTED);
        PipelineStats actual = codec.fromJson(json);

        assertExpectedPipelineStats(actual);
    }

    public static void assertExpectedPipelineStats(PipelineStats actual)
    {
        assertThat(actual.getFirstStartTime()).isEqualTo(new DateTime(100, UTC));
        assertThat(actual.getLastStartTime()).isEqualTo(new DateTime(101, UTC));
        assertThat(actual.getLastEndTime()).isEqualTo(new DateTime(102, UTC));
        assertThat(actual.isInputPipeline()).isTrue();
        assertThat(actual.isOutputPipeline()).isFalse();

        assertThat(actual.getTotalDrivers()).isEqualTo(1);
        assertThat(actual.getQueuedDrivers()).isEqualTo(2);
        assertThat(actual.getQueuedPartitionedDrivers()).isEqualTo(1);
        assertThat(actual.getQueuedPartitionedSplitsWeight()).isEqualTo(21L);
        assertThat(actual.getRunningDrivers()).isEqualTo(3);
        assertThat(actual.getRunningPartitionedDrivers()).isEqualTo(2);
        assertThat(actual.getRunningPartitionedSplitsWeight()).isEqualTo(22L);
        assertThat(actual.getBlockedDrivers()).isEqualTo(19);
        assertThat(actual.getCompletedDrivers()).isEqualTo(4);

        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(5));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(6));

        assertThat(actual.getQueuedTime().getCount()).isEqualTo(8.0);
        assertThat(actual.getElapsedTime().getCount()).isEqualTo(9.0);

        assertThat(actual.getTotalScheduledTime()).isEqualTo(new Duration(10, NANOSECONDS));
        assertThat(actual.getTotalCpuTime()).isEqualTo(new Duration(11, NANOSECONDS));
        assertThat(actual.getTotalBlockedTime()).isEqualTo(new Duration(13, NANOSECONDS));

        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(141));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(151);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(14, NANOSECONDS));

        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(142));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(152);

        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(14));
        assertThat(actual.getRawInputPositions()).isEqualTo(15);

        assertThat(actual.getProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(16));
        assertThat(actual.getProcessedInputPositions()).isEqualTo(17);

        assertThat(actual.getInputBlockedTime()).isEqualTo(new Duration(101, NANOSECONDS));

        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(18));
        assertThat(actual.getOutputPositions()).isEqualTo(19);

        assertThat(actual.getOutputBlockedTime()).isEqualTo(new Duration(102, NANOSECONDS));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(20));

        assertThat(actual.getOperatorSummaries()).hasSize(1);
        assertExpectedOperatorStats(actual.getOperatorSummaries().get(0));

        assertThat(actual.getDrivers()).hasSize(1);
        assertExpectedDriverStats(actual.getDrivers().get(0));
    }

    private static DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }
}
