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

import static io.trino.operator.TestOperatorStats.assertExpectedOperatorStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestDriverStats
{
    public static final DriverStats EXPECTED = new DriverStats(
            new DateTime(1),
            new DateTime(2),
            new DateTime(3),

            new Duration(4, NANOSECONDS),
            new Duration(5, NANOSECONDS),

            DataSize.ofBytes(6),
            DataSize.ofBytes(7),

            new Duration(9, NANOSECONDS),
            new Duration(10, NANOSECONDS),
            new Duration(12, NANOSECONDS),
            false,
            ImmutableSet.of(),

            DataSize.ofBytes(131),
            141,
            new Duration(151, NANOSECONDS),

            DataSize.ofBytes(132),
            142,

            DataSize.ofBytes(13),
            14,
            new Duration(15, NANOSECONDS),

            DataSize.ofBytes(16),
            17,

            new Duration(101, NANOSECONDS),

            DataSize.ofBytes(18),
            19,

            new Duration(102, NANOSECONDS),

            DataSize.ofBytes(20),

            ImmutableList.of(TestOperatorStats.EXPECTED));

    @Test
    public void testJson()
    {
        JsonCodec<DriverStats> codec = JsonCodec.jsonCodec(DriverStats.class);

        String json = codec.toJson(EXPECTED);
        DriverStats actual = codec.fromJson(json);

        assertExpectedDriverStats(actual);
    }

    public static void assertExpectedDriverStats(DriverStats actual)
    {
        assertEquals(actual.getCreateTime(), new DateTime(1, UTC));
        assertEquals(actual.getStartTime(), new DateTime(2, UTC));
        assertEquals(actual.getEndTime(), new DateTime(3, UTC));
        assertEquals(actual.getQueuedTime(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getElapsedTime(), new Duration(5, NANOSECONDS));

        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(6));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(7));

        assertEquals(actual.getTotalScheduledTime(), new Duration(9, NANOSECONDS));
        assertEquals(actual.getTotalCpuTime(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getTotalBlockedTime(), new Duration(12, NANOSECONDS));

        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(131));
        assertEquals(actual.getPhysicalInputPositions(), 141);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(151, NANOSECONDS));

        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(132));
        assertEquals(actual.getInternalNetworkInputPositions(), 142);

        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(13));
        assertEquals(actual.getRawInputPositions(), 14);
        assertEquals(actual.getRawInputReadTime(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getProcessedInputDataSize(), DataSize.ofBytes(16));
        assertEquals(actual.getProcessedInputPositions(), 17);

        assertEquals(actual.getInputBlockedTime(), new Duration(101, NANOSECONDS));

        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(18));
        assertEquals(actual.getOutputPositions(), 19);

        assertEquals(actual.getOutputBlockedTime(), new Duration(102, NANOSECONDS));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(20));

        assertEquals(actual.getOperatorStats().size(), 1);
        assertExpectedOperatorStats(actual.getOperatorStats().get(0));
    }
}
