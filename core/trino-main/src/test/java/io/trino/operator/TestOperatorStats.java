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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputInfo;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOperatorStats
{
    private static final SplitOperatorInfo NON_MERGEABLE_INFO = new SplitOperatorInfo(TEST_CATALOG_HANDLE, "some_info");
    private static final PartitionedOutputInfo MERGEABLE_INFO = new PartitionedOutputInfo(1024);

    public static final OperatorStats EXPECTED = new OperatorStats(
            0,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            DataSize.ofBytes(51),
            511,
            new Duration(5, NANOSECONDS),
            DataSize.ofBytes(52),
            522,
            DataSize.ofBytes(5),
            DataSize.ofBytes(6),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            DataSize.ofBytes(12),
            13,
            533,
            new Metrics(ImmutableMap.of("metrics", new LongCount(42))),
            new Metrics(ImmutableMap.of("connectorMetrics", new LongCount(43))),

            DataSize.ofBytes(14),

            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),

            DataSize.ofBytes(19),
            DataSize.ofBytes(20),
            DataSize.ofBytes(22),
            DataSize.ofBytes(24),
            DataSize.ofBytes(25),
            DataSize.ofBytes(26),
            Optional.empty(),
            NON_MERGEABLE_INFO);

    public static final OperatorStats MERGEABLE = new OperatorStats(
            0,
            1,
            41,
            new PlanNodeId("test"),
            "test",

            1,

            2,
            new Duration(3, NANOSECONDS),
            new Duration(4, NANOSECONDS),
            DataSize.ofBytes(51),
            511,
            new Duration(5, NANOSECONDS),
            DataSize.ofBytes(52),
            522,
            DataSize.ofBytes(5),
            DataSize.ofBytes(6),
            7,
            8d,

            9,
            new Duration(10, NANOSECONDS),
            new Duration(11, NANOSECONDS),
            DataSize.ofBytes(12),
            13,
            533,
            new Metrics(ImmutableMap.of("metrics", new LongCount(42))),
            new Metrics(ImmutableMap.of("connectorMetrics", new LongCount(43))),

            DataSize.ofBytes(14),

            new Duration(15, NANOSECONDS),

            16,
            new Duration(17, NANOSECONDS),
            new Duration(18, NANOSECONDS),

            DataSize.ofBytes(19),
            DataSize.ofBytes(20),
            DataSize.ofBytes(22),
            DataSize.ofBytes(24),
            DataSize.ofBytes(25),
            DataSize.ofBytes(26),
            Optional.empty(),
            MERGEABLE_INFO);

    @Test
    public void testJson()
    {
        JsonCodec<OperatorStats> codec = JsonCodec.jsonCodec(OperatorStats.class);

        String json = codec.toJson(EXPECTED);
        OperatorStats actual = codec.fromJson(json);

        assertExpectedOperatorStats(actual);
    }

    public static void assertExpectedOperatorStats(OperatorStats actual)
    {
        assertThat(actual.getStageId()).isEqualTo(0);
        assertThat(actual.getOperatorId()).isEqualTo(41);
        assertThat(actual.getOperatorType()).isEqualTo("test");

        assertThat(actual.getTotalDrivers()).isEqualTo(1);
        assertThat(actual.getAddInputCalls()).isEqualTo(2);
        assertThat(actual.getAddInputWall()).isEqualTo(new Duration(3, NANOSECONDS));
        assertThat(actual.getAddInputCpu()).isEqualTo(new Duration(4, NANOSECONDS));
        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(51));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(511);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(5, NANOSECONDS));
        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(52));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(522);
        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(5));
        assertThat(actual.getInputDataSize()).isEqualTo(DataSize.ofBytes(6));
        assertThat(actual.getInputPositions()).isEqualTo(7);
        assertThat(actual.getSumSquaredInputPositions()).isEqualTo(8.0);

        assertThat(actual.getGetOutputCalls()).isEqualTo(9);
        assertThat(actual.getGetOutputWall()).isEqualTo(new Duration(10, NANOSECONDS));
        assertThat(actual.getGetOutputCpu()).isEqualTo(new Duration(11, NANOSECONDS));
        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(12));
        assertThat(actual.getOutputPositions()).isEqualTo(13);

        assertThat(actual.getDynamicFilterSplitsProcessed()).isEqualTo(533);
        assertThat(actual.getMetrics().getMetrics()).isEqualTo(ImmutableMap.of("metrics", new LongCount(42)));
        assertThat(actual.getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of("connectorMetrics", new LongCount(43)));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(14));

        assertThat(actual.getBlockedWall()).isEqualTo(new Duration(15, NANOSECONDS));

        assertThat(actual.getFinishCalls()).isEqualTo(16);
        assertThat(actual.getFinishWall()).isEqualTo(new Duration(17, NANOSECONDS));
        assertThat(actual.getFinishCpu()).isEqualTo(new Duration(18, NANOSECONDS));

        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(19));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(20));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(22));
        assertThat(actual.getPeakRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(24));
        assertThat(actual.getPeakTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(25));
        assertThat(actual.getSpilledDataSize()).isEqualTo(DataSize.ofBytes(26));
        assertThat(actual.getInfo().getClass()).isEqualTo(SplitOperatorInfo.class);
        assertThat(((SplitOperatorInfo) actual.getInfo()).getSplitInfo()).isEqualTo(NON_MERGEABLE_INFO.getSplitInfo());
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(ImmutableList.of(EXPECTED, EXPECTED));

        assertThat(actual.getStageId()).isEqualTo(0);
        assertThat(actual.getOperatorId()).isEqualTo(41);
        assertThat(actual.getOperatorType()).isEqualTo("test");

        assertThat(actual.getTotalDrivers()).isEqualTo(3 * 1);
        assertThat(actual.getAddInputCalls()).isEqualTo(3 * 2);
        assertThat(actual.getAddInputWall()).isEqualTo(new Duration(3 * 3, NANOSECONDS));
        assertThat(actual.getAddInputCpu()).isEqualTo(new Duration(3 * 4, NANOSECONDS));
        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 51));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(3 * 511);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(3 * 5, NANOSECONDS));
        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 52));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(3 * 522);
        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 5));
        assertThat(actual.getInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 6));
        assertThat(actual.getInputPositions()).isEqualTo(3 * 7);
        assertThat(actual.getSumSquaredInputPositions()).isEqualTo(3 * 8.0);

        assertThat(actual.getGetOutputCalls()).isEqualTo(3 * 9);
        assertThat(actual.getGetOutputWall()).isEqualTo(new Duration(3 * 10, NANOSECONDS));
        assertThat(actual.getGetOutputCpu()).isEqualTo(new Duration(3 * 11, NANOSECONDS));
        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(3 * 12));
        assertThat(actual.getOutputPositions()).isEqualTo(3 * 13);

        assertThat(actual.getDynamicFilterSplitsProcessed()).isEqualTo(3 * 533);
        assertThat(actual.getMetrics().getMetrics()).isEqualTo(ImmutableMap.of("metrics", new LongCount(3 * 42)));
        assertThat(actual.getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of("connectorMetrics", new LongCount(3 * 43)));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(3 * 14));

        assertThat(actual.getBlockedWall()).isEqualTo(new Duration(3 * 15, NANOSECONDS));

        assertThat(actual.getFinishCalls()).isEqualTo(3 * 16);
        assertThat(actual.getFinishWall()).isEqualTo(new Duration(3 * 17, NANOSECONDS));
        assertThat(actual.getFinishCpu()).isEqualTo(new Duration(3 * 18, NANOSECONDS));
        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(3 * 19));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(3 * 20));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(22));
        assertThat(actual.getPeakRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(24));
        assertThat(actual.getPeakTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(25));
        assertThat(actual.getSpilledDataSize()).isEqualTo(DataSize.ofBytes(3 * 26));
        assertThat(actual.getInfo()).isNull();
    }

    @Test
    public void testAddMergeable()
    {
        OperatorStats actual = MERGEABLE.add(ImmutableList.of(MERGEABLE, MERGEABLE));

        assertThat(actual.getStageId()).isEqualTo(0);
        assertThat(actual.getOperatorId()).isEqualTo(41);
        assertThat(actual.getOperatorType()).isEqualTo("test");

        assertThat(actual.getTotalDrivers()).isEqualTo(3 * 1);
        assertThat(actual.getAddInputCalls()).isEqualTo(3 * 2);
        assertThat(actual.getAddInputWall()).isEqualTo(new Duration(3 * 3, NANOSECONDS));
        assertThat(actual.getAddInputCpu()).isEqualTo(new Duration(3 * 4, NANOSECONDS));
        assertThat(actual.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 51));
        assertThat(actual.getPhysicalInputPositions()).isEqualTo(3 * 511);
        assertThat(actual.getPhysicalInputReadTime()).isEqualTo(new Duration(3 * 5, NANOSECONDS));
        assertThat(actual.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 52));
        assertThat(actual.getInternalNetworkInputPositions()).isEqualTo(3 * 522);
        assertThat(actual.getRawInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 5));
        assertThat(actual.getInputDataSize()).isEqualTo(DataSize.ofBytes(3 * 6));
        assertThat(actual.getInputPositions()).isEqualTo(3 * 7);
        assertThat(actual.getSumSquaredInputPositions()).isEqualTo(3 * 8.0);

        assertThat(actual.getGetOutputCalls()).isEqualTo(3 * 9);
        assertThat(actual.getGetOutputWall()).isEqualTo(new Duration(3 * 10, NANOSECONDS));
        assertThat(actual.getGetOutputCpu()).isEqualTo(new Duration(3 * 11, NANOSECONDS));
        assertThat(actual.getOutputDataSize()).isEqualTo(DataSize.ofBytes(3 * 12));
        assertThat(actual.getOutputPositions()).isEqualTo(3 * 13);

        assertThat(actual.getDynamicFilterSplitsProcessed()).isEqualTo(3 * 533);
        assertThat(actual.getMetrics().getMetrics()).isEqualTo(ImmutableMap.of("metrics", new LongCount(3 * 42)));
        assertThat(actual.getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of("connectorMetrics", new LongCount(3 * 43)));

        assertThat(actual.getPhysicalWrittenDataSize()).isEqualTo(DataSize.ofBytes(3 * 14));

        assertThat(actual.getBlockedWall()).isEqualTo(new Duration(3 * 15, NANOSECONDS));

        assertThat(actual.getFinishCalls()).isEqualTo(3 * 16);
        assertThat(actual.getFinishWall()).isEqualTo(new Duration(3 * 17, NANOSECONDS));
        assertThat(actual.getFinishCpu()).isEqualTo(new Duration(3 * 18, NANOSECONDS));
        assertThat(actual.getUserMemoryReservation()).isEqualTo(DataSize.ofBytes(3 * 19));
        assertThat(actual.getRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(3 * 20));
        assertThat(actual.getPeakUserMemoryReservation()).isEqualTo(DataSize.ofBytes(22));
        assertThat(actual.getPeakRevocableMemoryReservation()).isEqualTo(DataSize.ofBytes(24));
        assertThat(actual.getPeakTotalMemoryReservation()).isEqualTo(DataSize.ofBytes(25));
        assertThat(actual.getSpilledDataSize()).isEqualTo(DataSize.ofBytes(3 * 26));
        assertThat(actual.getInfo().getClass()).isEqualTo(PartitionedOutputInfo.class);
        assertThat(((PartitionedOutputInfo) actual.getInfo()).getOutputBufferPeakMemoryUsage()).isEqualTo(MERGEABLE_INFO.getOutputBufferPeakMemoryUsage());
    }
}
