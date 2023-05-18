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
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

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
        assertThat(actual.getCreateTime()).isEqualTo(new DateTime(1, UTC));
        assertThat(actual.getStartTime()).isEqualTo(new DateTime(2, UTC));
        assertThat(actual.getEndTime()).isEqualTo(new DateTime(3, UTC));
        assertThat(actual.getQueuedTime()).isEqualTo(new Duration(4, NANOSECONDS));
        assertThat(actual.getElapsedTime()).isEqualTo(new Duration(5, NANOSECONDS));

        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(6));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(7));

        assertThat(actual.getTotalScheduledTime()).isEqualTo(new Duration(9, NANOSECONDS));
        assertThat(actual.getTotalCpuTime()).isEqualTo(new Duration(10, NANOSECONDS));
        assertThat(actual.getTotalBlockedTime()).isEqualTo(new Duration(12, NANOSECONDS));

        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(131));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(141);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(151, NANOSECONDS));

        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(132));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(142);

        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(13));
        assertThat(actual.getRawInputPositions()).isEqualTo(14);
        assertThat(actual.getRawInputReadTime()).isEqualTo(new Duration(15, NANOSECONDS));

        assertThat(actual.getProcessedInputDataSize()).isEqualTo(DataSize.ofBytes(16));
        assertThat(actual.getProcessedInputPositions()).isEqualTo(17);

        assertThat(actual.getInputBlockedTime()).isEqualTo(new Duration(101, NANOSECONDS));

        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(18));
        assertThat(actual.getOutputPositions()).isEqualTo(19);

        assertThat(actual.getOutputBlockedTime()).isEqualTo(new Duration(102, NANOSECONDS));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(20));

        assertThat(actual.getOperatorStats()).hasSize(1);
        assertExpectedOperatorStats(actual.getOperatorStats().get(0));
    }
}
