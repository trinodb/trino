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
package io.trino.operator.aggregation;

import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAggregationMask
{
    @Test
    public void testUnsetNulls()
    {
        AggregationMask aggregationMask = AggregationMask.createSelectAll(0);
        assertAggregationMaskAll(aggregationMask, 0);

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.unselectNullPositions(new IntArrayBlock(positionCount, Optional.empty(), new int[positionCount]));
            assertAggregationMaskAll(aggregationMask, positionCount);

            boolean[] nullFlags = new boolean[positionCount];
            aggregationMask.unselectNullPositions(new IntArrayBlock(positionCount, Optional.of(nullFlags), new int[positionCount]));
            assertAggregationMaskAll(aggregationMask, positionCount);

            Arrays.fill(nullFlags, true);
            nullFlags[1] = false;
            nullFlags[3] = false;
            nullFlags[5] = false;
            aggregationMask.unselectNullPositions(new IntArrayBlock(positionCount, Optional.of(nullFlags), new int[positionCount]));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 3, 5);

            nullFlags[3] = true;
            aggregationMask.unselectNullPositions(new IntArrayBlock(positionCount, Optional.of(nullFlags), new int[positionCount]));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 5);

            nullFlags[1] = true;
            nullFlags[5] = true;
            aggregationMask.unselectNullPositions(new IntArrayBlock(positionCount, Optional.of(nullFlags), new int[positionCount]));
            assertAggregationMaskPositions(aggregationMask, positionCount);

            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.unselectNullPositions(RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[1]), positionCount));
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.unselectNullPositions(RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.of(new boolean[] {false}), new int[1]), positionCount));
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.unselectNullPositions(RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.of(new boolean[] {true}), new int[1]), positionCount));
            assertAggregationMaskPositions(aggregationMask, positionCount);
        }
    }

    @Test
    public void testApplyMask()
    {
        AggregationMask aggregationMask = AggregationMask.createSelectAll(0);
        assertAggregationMaskAll(aggregationMask, 0);

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            byte[] mask = new byte[positionCount];
            Arrays.fill(mask, (byte) 1);

            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.empty(), mask));
            assertAggregationMaskAll(aggregationMask, positionCount);

            Arrays.fill(mask, (byte) 0);
            mask[1] = 1;
            mask[3] = 1;
            mask[5] = 1;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.empty(), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 3, 5);

            mask[3] = 0;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.empty(), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 5);

            mask[1] = 0;
            mask[5] = 0;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.empty(), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount);

            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.applyMaskBlock(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.empty(), new byte[] {1}), positionCount));
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.applyMaskBlock(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.empty(), new byte[] {0}), positionCount));
            assertAggregationMaskPositions(aggregationMask, positionCount);
        }
    }

    @Test
    public void testApplyMaskNulls()
    {
        AggregationMask aggregationMask = AggregationMask.createSelectAll(0);
        assertAggregationMaskAll(aggregationMask, 0);

        for (int positionCount = 7; positionCount < 10; positionCount++) {
            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            byte[] mask = new byte[positionCount];
            Arrays.fill(mask, (byte) 1);

            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.empty(), mask));
            assertAggregationMaskAll(aggregationMask, positionCount);

            boolean[] nullFlags = new boolean[positionCount];
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.of(nullFlags), mask));
            assertAggregationMaskAll(aggregationMask, positionCount);

            Arrays.fill(nullFlags, true);
            nullFlags[1] = false;
            nullFlags[3] = false;
            nullFlags[5] = false;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.of(nullFlags), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 3, 5);

            nullFlags[3] = true;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.of(nullFlags), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount, 1, 5);

            nullFlags[1] = true;
            nullFlags[5] = true;
            aggregationMask.applyMaskBlock(new ByteArrayBlock(positionCount, Optional.of(nullFlags), mask));
            assertAggregationMaskPositions(aggregationMask, positionCount);

            aggregationMask.reset(positionCount);
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.applyMaskBlock(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.empty(), new byte[] {1}), positionCount));
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.applyMaskBlock(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.of(new boolean[] {false}), new byte[] {1}), positionCount));
            assertAggregationMaskAll(aggregationMask, positionCount);

            aggregationMask.applyMaskBlock(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[] {1}), positionCount));
            assertAggregationMaskPositions(aggregationMask, positionCount);
        }
    }

    private static void assertAggregationMaskAll(AggregationMask aggregationMask, int expectedPositionCount)
    {
        assertThat(aggregationMask.isSelectAll()).isTrue();
        assertThat(aggregationMask.isSelectNone()).isEqualTo(expectedPositionCount == 0);
        assertThat(aggregationMask.getPositionCount()).isEqualTo(expectedPositionCount);
        assertThat(aggregationMask.getSelectedPositionCount()).isEqualTo(expectedPositionCount);
        assertThatThrownBy(aggregationMask::getSelectedPositions).isInstanceOf(IllegalStateException.class);
    }

    private static void assertAggregationMaskPositions(AggregationMask aggregationMask, int expectedPositionCount, int... expectedPositions)
    {
        assertThat(aggregationMask.isSelectAll()).isFalse();
        assertThat(aggregationMask.isSelectNone()).isEqualTo(expectedPositions.length == 0);
        assertThat(aggregationMask.getPositionCount()).isEqualTo(expectedPositionCount);
        assertThat(aggregationMask.getSelectedPositionCount()).isEqualTo(expectedPositions.length);
        // AssertJ is buggy and does not allow starts with to contain an empty array
        if (expectedPositions.length > 0) {
            assertThat(aggregationMask.getSelectedPositions()).startsWith(expectedPositions);
        }
    }
}
