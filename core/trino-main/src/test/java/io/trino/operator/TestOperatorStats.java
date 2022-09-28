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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOperatorStats
{
    private static final SplitOperatorInfo NON_MERGEABLE_INFO = new SplitOperatorInfo(TEST_CATALOG_HANDLE, "some_info");
    private static final PartitionedOutputInfo MERGEABLE_INFO = new PartitionedOutputInfo(1, 2, 1024);

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
        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 1);
        assertEquals(actual.getAddInputCalls(), 2);
        assertEquals(actual.getAddInputWall(), new Duration(3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(51));
        assertEquals(actual.getPhysicalInputPositions(), 511);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(5, NANOSECONDS));
        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(52));
        assertEquals(actual.getInternalNetworkInputPositions(), 522);
        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(5));
        assertEquals(actual.getInputDataSize(), DataSize.ofBytes(6));
        assertEquals(actual.getInputPositions(), 7);
        assertEquals(actual.getSumSquaredInputPositions(), 8.0);

        assertEquals(actual.getGetOutputCalls(), 9);
        assertEquals(actual.getGetOutputWall(), new Duration(10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(12));
        assertEquals(actual.getOutputPositions(), 13);

        assertEquals(actual.getDynamicFilterSplitsProcessed(), 533);
        assertEquals(actual.getMetrics().getMetrics(), ImmutableMap.of("metrics", new LongCount(42)));
        assertEquals(actual.getConnectorMetrics().getMetrics(), ImmutableMap.of("connectorMetrics", new LongCount(43)));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(14));

        assertEquals(actual.getBlockedWall(), new Duration(15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 16);
        assertEquals(actual.getFinishWall(), new Duration(17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(18, NANOSECONDS));

        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(19));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(20));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(22));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(24));
        assertEquals(actual.getPeakTotalMemoryReservation(), DataSize.ofBytes(25));
        assertEquals(actual.getSpilledDataSize(), DataSize.ofBytes(26));
        assertEquals(actual.getInfo().getClass(), SplitOperatorInfo.class);
        assertEquals(((SplitOperatorInfo) actual.getInfo()).getSplitInfo(), NON_MERGEABLE_INFO.getSplitInfo());
    }

    @Test
    public void testAdd()
    {
        OperatorStats actual = EXPECTED.add(ImmutableList.of(EXPECTED, EXPECTED));

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(3 * 51));
        assertEquals(actual.getPhysicalInputPositions(), 3 * 511);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(3 * 5, NANOSECONDS));
        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(3 * 52));
        assertEquals(actual.getInternalNetworkInputPositions(), 3 * 522);
        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(3 * 5));
        assertEquals(actual.getInputDataSize(), DataSize.ofBytes(3 * 6));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(3 * 12));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getDynamicFilterSplitsProcessed(), 3 * 533);
        assertEquals(actual.getMetrics().getMetrics(), ImmutableMap.of("metrics", new LongCount(3 * 42)));
        assertEquals(actual.getConnectorMetrics().getMetrics(), ImmutableMap.of("connectorMetrics", new LongCount(3 * 43)));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(3 * 14));

        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(3 * 19));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(3 * 20));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(22));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(24));
        assertEquals(actual.getPeakTotalMemoryReservation(), DataSize.ofBytes(25));
        assertEquals(actual.getSpilledDataSize(), DataSize.ofBytes(3 * 26));
        assertNull(actual.getInfo());
    }

    @Test
    public void testAddMergeable()
    {
        OperatorStats actual = MERGEABLE.add(ImmutableList.of(MERGEABLE, MERGEABLE));

        assertEquals(actual.getStageId(), 0);
        assertEquals(actual.getOperatorId(), 41);
        assertEquals(actual.getOperatorType(), "test");

        assertEquals(actual.getTotalDrivers(), 3 * 1);
        assertEquals(actual.getAddInputCalls(), 3 * 2);
        assertEquals(actual.getAddInputWall(), new Duration(3 * 3, NANOSECONDS));
        assertEquals(actual.getAddInputCpu(), new Duration(3 * 4, NANOSECONDS));
        assertEquals(actual.getPhysicalInputDataSize(), DataSize.ofBytes(3 * 51));
        assertEquals(actual.getPhysicalInputPositions(), 3 * 511);
        assertEquals(actual.getPhysicalInputReadTime(), new Duration(3 * 5, NANOSECONDS));
        assertEquals(actual.getInternalNetworkInputDataSize(), DataSize.ofBytes(3 * 52));
        assertEquals(actual.getInternalNetworkInputPositions(), 3 * 522);
        assertEquals(actual.getRawInputDataSize(), DataSize.ofBytes(3 * 5));
        assertEquals(actual.getInputDataSize(), DataSize.ofBytes(3 * 6));
        assertEquals(actual.getInputPositions(), 3 * 7);
        assertEquals(actual.getSumSquaredInputPositions(), 3 * 8.0);

        assertEquals(actual.getGetOutputCalls(), 3 * 9);
        assertEquals(actual.getGetOutputWall(), new Duration(3 * 10, NANOSECONDS));
        assertEquals(actual.getGetOutputCpu(), new Duration(3 * 11, NANOSECONDS));
        assertEquals(actual.getOutputDataSize(), DataSize.ofBytes(3 * 12));
        assertEquals(actual.getOutputPositions(), 3 * 13);

        assertEquals(actual.getDynamicFilterSplitsProcessed(), 3 * 533);
        assertEquals(actual.getMetrics().getMetrics(), ImmutableMap.of("metrics", new LongCount(3 * 42)));
        assertEquals(actual.getConnectorMetrics().getMetrics(), ImmutableMap.of("connectorMetrics", new LongCount(3 * 43)));

        assertEquals(actual.getPhysicalWrittenDataSize(), DataSize.ofBytes(3 * 14));

        assertEquals(actual.getBlockedWall(), new Duration(3 * 15, NANOSECONDS));

        assertEquals(actual.getFinishCalls(), 3 * 16);
        assertEquals(actual.getFinishWall(), new Duration(3 * 17, NANOSECONDS));
        assertEquals(actual.getFinishCpu(), new Duration(3 * 18, NANOSECONDS));
        assertEquals(actual.getUserMemoryReservation(), DataSize.ofBytes(3 * 19));
        assertEquals(actual.getRevocableMemoryReservation(), DataSize.ofBytes(3 * 20));
        assertEquals(actual.getPeakUserMemoryReservation(), DataSize.ofBytes(22));
        assertEquals(actual.getPeakRevocableMemoryReservation(), DataSize.ofBytes(24));
        assertEquals(actual.getPeakTotalMemoryReservation(), DataSize.ofBytes(25));
        assertEquals(actual.getSpilledDataSize(), DataSize.ofBytes(3 * 26));
        assertEquals(actual.getInfo().getClass(), PartitionedOutputInfo.class);
        assertEquals(((PartitionedOutputInfo) actual.getInfo()).getPagesAdded(), 3 * MERGEABLE_INFO.getPagesAdded());
    }
}
