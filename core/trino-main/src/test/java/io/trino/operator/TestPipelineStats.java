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
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestPipelineStats
{
    public static final PipelineStats EXPECTED = new PipelineStats(
            0,

            new DateTime(100),
            new DateTime(101),
            new DateTime(102),

            true,
            false,
            true,

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
            DataSize.ofBytes(7),

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

            DataSize.ofBytes(18),
            19,

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
        assertEquals(actual.getFirstStartTime(), new DateTime(100, UTC));
        assertEquals(actual.getLastStartTime(), new DateTime(101, UTC));
        assertEquals(actual.getLastEndTime(), new DateTime(102, UTC));
        assertEquals(actual.isInputPipeline(), true);
        assertEquals(actual.isOutputPipeline(), false);
        assertEquals(actual.isPartitioned(), true);

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getQueuedDrivers(), 2);
        assertEquals(actual.getQueuedPartitionedDrivers(), 1);
        assertEquals(actual.getQueuedPartitionedSplitsWeight(), 21L);
        assertEquals(actual.getRunningDrivers(), 3);
        assertEquals(actual.getRunningPartitionedDrivers(), 2);
        assertEquals(actual.getRunningPartitionedSplitsWeight(), 22L);
        assertEquals(actual.getBlockedDrivers(), 19);
        assertEquals(actual.getCompletedDrivers(), 4);

        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(5));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(6));
        assertEquals(actual.getSystemMemoryReservation(), DataSize.ofBytes(7));

        assertEquals(actual.getQueuedTime().getCount(), 8.0);
        assertEquals(actual.getElapsedTime().getCount(), 9.0);

        assertEquals(actual.getTotalScheduledTime(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(13, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(141));
        assertEquals(actual.getPhysicalInputPositions(), 151);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(14, NANOSECONDS));

        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(142));
        assertEquals(actual.getInternalNetworkInputPositions(), 152);

        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(14));
        assertEquals(actual.getRawInputPositions(), 15);

        assertEquals(actual.getProcessedInputDataSize(), DataSize.ofBytes(16));
        assertEquals(actual.getProcessedInputPositions(), 17);

        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(18));
        assertEquals(actual.getOutputPositions(), 19);

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(20));

        assertEquals(actual.getOperatorSummaries().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorSummaries().get(0));

        assertEquals(actual.getDrivers().size(), 1);
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
