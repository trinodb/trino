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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

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
        assertEquals(actual.getSchedulingComplete().getMillis(), 0);

        assertEquals(actual.getGetSplitDistribution().getCount(), 1.0);

        assertEquals(actual.getTotalTasks(), 4);
        assertEquals(actual.getRunningTasks(), 5);
        assertEquals(actual.getCompletedTasks(), 6);
        assertEquals(actual.getFailedTasks(), 1);

        assertEquals(actual.getTotalDrivers(), 7);
        assertEquals(actual.getQueuedDrivers(), 8);
        assertEquals(actual.getRunningDrivers(), 10);
        assertEquals(actual.getBlockedDrivers(), 26);
        assertEquals(actual.getCompletedDrivers(), 11);

        assertEquals(actual.getCumulativeUserMemory(), 12.0);
        assertEquals(actual.getFailedCumulativeUserMemory(), 13.0);
        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(14));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(15));
        assertEquals(actual.getTotalMemoryReservation(), DataSize.ofBytes(16));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(17));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(18));

        assertEquals(actual.getTotalScheduledTime(), new Duration(19, NANOSECONDS));
        assertEquals(actual.getFailedScheduledTime(), new Duration(20, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(21, NANOSECONDS));
        assertEquals(actual.getFailedCpuTime(), new Duration(22, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(23, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(191));
        assertEquals(actual.getFailedPhysicalInputDataSize(), DataSize.ofBytes(192));
        assertEquals(actual.getPhysicalInputPositions(), 201);
        assertEquals(actual.getFailedPhysicalInputPositions(), 202);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(24, NANOSECONDS));
        assertEquals(actual.getFailedPhysicalInputReadTime(), new Duration(25, NANOSECONDS));

        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(193));
        assertEquals(actual.getFailedInternalNetworkInputDataSize(), DataSize.ofBytes(194));
        assertEquals(actual.getInternalNetworkInputPositions(), 203);
        assertEquals(actual.getFailedInternalNetworkInputPositions(), 204);

        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(26));
        assertEquals(actual.getFailedRawInputDataSize(), DataSize.ofBytes(27));
        assertEquals(actual.getRawInputPositions(), 28);
        assertEquals(actual.getFailedRawInputPositions(), 29);

        assertEquals(actual.getProcessedInputDataSize(), DataSize.ofBytes(30));
        assertEquals(actual.getFailedProcessedInputDataSize(), DataSize.ofBytes(31));
        assertEquals(actual.getProcessedInputPositions(), 32);
        assertEquals(actual.getFailedProcessedInputPositions(), 33);

        assertEquals(actual.getInputBlockedTime(), new Duration(201, NANOSECONDS));
        assertEquals(actual.getFailedInputBlockedTime(), new Duration(202, NANOSECONDS));

        assertEquals(actual.getBufferedDataSize(), DataSize.ofBytes(34));
        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(35));
        assertEquals(actual.getFailedOutputDataSize(), DataSize.ofBytes(36));
        assertEquals(actual.getOutputPositions(), 37);
        assertEquals(actual.getFailedOutputPositions(), 38);

        assertEquals(actual.getOutputBlockedTime(), new Duration(203, NANOSECONDS));
        assertEquals(actual.getFailedOutputBlockedTime(), new Duration(204, NANOSECONDS));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(39));
        assertEquals(actual.getFailedPhysicalWrittenDataSize(), DataSize.ofBytes(40));

        assertEquals(actual.getGcInfo().getStageId(), 101);
        assertEquals(actual.getGcInfo().getTasks(), 102);
        assertEquals(actual.getGcInfo().getFullGcTasks(), 103);
        assertEquals(actual.getGcInfo().getMinFullGcSec(), 104);
        assertEquals(actual.getGcInfo().getMaxFullGcSec(), 105);
        assertEquals(actual.getGcInfo().getTotalFullGcSec(), 106);
        assertEquals(actual.getGcInfo().getAverageFullGcSec(), 107);
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
