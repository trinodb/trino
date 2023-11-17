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
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStageStats
{
    private static final StageStats EXPECTED = new StageStats(
            new DateTime(0),

            getTestDistribution(1),

            4,
            5,
            6,
            1,

            7,
            8,
            10,
            26,
            11,

            12.0,
            13.0,
            DataSize.ofBytes(14),
            DataSize.ofBytes(15),
            DataSize.ofBytes(16),
            DataSize.ofBytes(17),
            DataSize.ofBytes(18),

            new Duration(19, NANOSECONDS),
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            new Duration(22, NANOSECONDS),
            new Duration(23, NANOSECONDS),
            false,
            ImmutableSet.of(),

            DataSize.ofBytes(191),
            DataSize.ofBytes(192),
            201,
            202,
            new Duration(24, NANOSECONDS),
            new Duration(25, NANOSECONDS),

            DataSize.ofBytes(193),
            DataSize.ofBytes(194),
            203,
            204,

            DataSize.ofBytes(26),
            DataSize.ofBytes(27),
            28,
            29,

            DataSize.ofBytes(30),
            DataSize.ofBytes(31),
            32,
            33,

            new Duration(201, NANOSECONDS),
            new Duration(202, NANOSECONDS),

            DataSize.ofBytes(34),
            Optional.of(getTDigestHistogram(10)),
            DataSize.ofBytes(35),
            DataSize.ofBytes(36),
            37,
            38,

            new Duration(203, NANOSECONDS),
            new Duration(204, NANOSECONDS),

            DataSize.ofBytes(39),
            DataSize.ofBytes(40),

            new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),

            ImmutableList.of());

    @Test
    public void testJson()
    {
        JsonCodec<StageStats> codec = JsonCodec.jsonCodec(StageStats.class);

        String json = codec.toJson(EXPECTED);
        StageStats actual = codec.fromJson(json);

        assertExpectedStageStats(actual);
    }

    private static void assertExpectedStageStats(StageStats actual)
    {
        assertThat(actual.getSchedulingComplete().getMillis()).isEqualTo(0);

        assertThat(actual.getGetSplitDistribution().getCount()).isEqualTo(1.0);

        assertThat(actual.getTotalTasks()).isEqualTo(4);
        assertThat(actual.getRunningTasks()).isEqualTo(5);
        assertThat(actual.getCompletedTasks()).isEqualTo(6);
        assertThat(actual.getFailedTasks()).isEqualTo(1);

        assertThat(actual.getTotalDrivers()).isEqualTo(7);
        assertThat(actual.getQueuedDrivers()).isEqualTo(8);
        assertThat(actual.getRunningDrivers()).isEqualTo(10);
        assertThat(actual.getBlockedDrivers()).isEqualTo(26);
        assertThat(actual.getCompletedDrivers()).isEqualTo(11);

        assertThat(actual.getCumulativeUserMemory()).isEqualTo(12.0);
        assertThat(actual.getFailedCumulativeUserMemory()).isEqualTo(13.0);
        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(14));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(15));
        assertThat(actual.getTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(16));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(17));
        assertThat(actual.getPeakRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(18));

        assertThat(actual.getTotalScheduledTime()).isEqualTo(new Duration(19, NANOSECONDS));
        assertThat(actual.getFailedScheduledTime()).isEqualTo(new Duration(20, NANOSECONDS));
        assertThat(actual.getTotalCpuTime()).isEqualTo(new Duration(21, NANOSECONDS));
        assertThat(actual.getFailedCpuTime()).isEqualTo(new Duration(22, NANOSECONDS));
        assertThat(actual.getTotalBlockedTime()).isEqualTo(new Duration(23, NANOSECONDS));

        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(191));
        assertThat(actual.getFailedPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(192));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(201);
        assertThat(actual.getFailedPhysicalInputPositions()).isEqualTo(202);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(24, NANOSECONDS));
        assertThat(actual.getFailedPhysicalInputReadTime()).isEqualTo(new Duration(25, NANOSECONDS));

        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(193));
        assertThat(actual.getFailedInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(194));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(203);
        assertThat(actual.getFailedInternalNetworkInputPositions()).isEqualTo(204);

        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(26));
        assertThat(actual.getFailedRawInputDataSize()).isEqualTo(DataSize.ofBytes(27));
        assertThat(actual.getRawInputPositions()).isEqualTo(28);
        assertThat(actual.getFailedRawInputPositions()).isEqualTo(29);

        assertThat(actual.getProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(30));
        assertThat(actual.getFailedProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(31));
        assertThat(actual.getProcessedInputPositions()).isEqualTo(32);
        assertThat(actual.getFailedProcessedInputPositions()).isEqualTo(33);

        assertThat(actual.getInputBlockedTime()).isEqualTo(new Duration(201, NANOSECONDS));
        assertThat(actual.getFailedInputBlockedTime()).isEqualTo(new Duration(202, NANOSECONDS));

        assertThat(actual.getBufferedDataSize()).isEqualTo(DataSize.ofBytes(34));
        assertThat(actual.getOutputBufferUtilization().get().getMax()).isEqualTo(9.0);
        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(35));
        assertThat(actual.getFailedOutputDataSize()).isEqualTo(DataSize.ofBytes(36));
        assertThat(actual.getOutputPositions()).isEqualTo(37);
        assertThat(actual.getFailedOutputPositions()).isEqualTo(38);

        assertThat(actual.getOutputBlockedTime()).isEqualTo(new Duration(203, NANOSECONDS));
        assertThat(actual.getFailedOutputBlockedTime()).isEqualTo(new Duration(204, NANOSECONDS));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(39));
        assertThat(actual.getFailedPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(40));

        assertThat(actual.getGcInfo().getStageId()).isEqualTo(101);
        assertThat(actual.getGcInfo().getTasks()).isEqualTo(102);
        assertThat(actual.getGcInfo().getFullGcTasks()).isEqualTo(103);
        assertThat(actual.getGcInfo().getMinFullGcSec()).isEqualTo(104);
        assertThat(actual.getGcInfo().getMaxFullGcSec()).isEqualTo(105);
        assertThat(actual.getGcInfo().getTotalFullGcSec()).isEqualTo(106);
        assertThat(actual.getGcInfo().getAverageFullGcSec()).isEqualTo(107);
    }

    private static DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }

    private static TDigestHistogram getTDigestHistogram(int count)
    {
        TDigest digest = new TDigest();
        for (int i = 0; i < count; i++) {
            digest.add(i);
        }
        return new TDigestHistogram(digest);
    }
}
