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

            7,
            8,
            10,
            26,
            11,

            12.0,
            10.0,
            DataSize.ofBytes(13),
            DataSize.ofBytes(14),
            DataSize.ofBytes(15),
            DataSize.ofBytes(16),
            DataSize.ofBytes(17),

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

            DataSize.ofBytes(23),
            DataSize.ofBytes(24),
            25,

            DataSize.ofBytes(26),

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

        assertEquals(actual.getTotalDrivers(), 7);
        assertEquals(actual.getQueuedDrivers(), 8);
        assertEquals(actual.getRunningDrivers(), 10);
        assertEquals(actual.getBlockedDrivers(), 26);
        assertEquals(actual.getCompletedDrivers(), 11);

        assertEquals(actual.getCumulativeUserMemory(), 12.0);
        assertEquals(actual.getCumulativeSystemMemory(), 10.0);
        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(13));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(14));
        assertEquals(actual.getTotalMemoryReservation(), DataSize.ofBytes(15));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(16));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(17));

        assertEquals(actual.getTotalScheduledTime(), new Duration(15, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(16, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(18, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(191));
        assertEquals(actual.getPhysicalInputPositions(), 201);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(192));
        assertEquals(actual.getInternalNetworkInputPositions(), 202);

        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(19));
        assertEquals(actual.getRawInputPositions(), 20);

        assertEquals(actual.getProcessedInputDataSize(), DataSize.ofBytes(21));
        assertEquals(actual.getProcessedInputPositions(), 22);

        assertEquals(actual.getBufferedDataSize(), DataSize.ofBytes(23));
        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(24));
        assertEquals(actual.getOutputPositions(), 25);

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(26));

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
