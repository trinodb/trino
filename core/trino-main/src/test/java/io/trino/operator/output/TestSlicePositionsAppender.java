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
package io.trino.operator.output;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.output.SlicePositionsAppender.duplicateBytes;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestSlicePositionsAppender
{
    @Test
    public void testAppendEmptySliceRle()
    {
        // test SlicePositionAppender.appendRle with empty value (Slice with length 0)
        PositionsAppender positionsAppender = new SlicePositionsAppender(1, 100);
        Block value = createStringsBlock("");
        positionsAppender.appendRle(value, 10);

        Block actualBlock = positionsAppender.build();

        assertBlockEquals(VARCHAR, actualBlock, RunLengthEncodedBlock.create(value, 10));
    }

    // test append with VariableWidthBlock using Slice not backed by byte array
    // to test special handling in SlicePositionsAppender.copyBytes
    @Test
    public void testAppendSliceNotBackedByByteArray()
    {
        PositionsAppender positionsAppender = new SlicePositionsAppender(1, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        Block block = new VariableWidthBlock(3, Slices.wrappedLongArray(257, 2), new int[] {0, 1, Long.BYTES, 2 * Long.BYTES}, Optional.empty());
        positionsAppender.append(IntArrayList.wrap(new int[] {0, 2}), block);

        Block actual = positionsAppender.build();

        Block expected = new VariableWidthBlock(
                2,
                Slices.wrappedBuffer(new byte[] {1, 2, 0, 0, 0, 0, 0, 0, 0}),
                new int[] {0, 1, Long.BYTES + 1},
                Optional.empty());
        assertBlockEquals(VARCHAR, actual, expected);
    }

    @Test
    public void testDuplicateZeroLength()
    {
        Slice slice = Slices.wrappedBuffer();
        byte[] target = new byte[] {-1};
        duplicateBytes(slice, target, 0, 100);
        assertArrayEquals(new byte[] {-1}, target);
    }

    @Test
    public void testDuplicate1Byte()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {2});
        byte[] target = new byte[5];
        Arrays.fill(target, (byte) -1);
        duplicateBytes(slice, target, 3, 2);
        assertArrayEquals(new byte[] {-1, -1, -1, 2, 2}, target);
    }

    @Test
    public void testDuplicate2Bytes()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {1, 2});
        byte[] target = new byte[8];
        Arrays.fill(target, (byte) -1);
        duplicateBytes(slice, target, 1, 3);
        assertArrayEquals(new byte[] {-1, 1, 2, 1, 2, 1, 2, -1}, target);
    }

    @Test
    public void testDuplicate1Time()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {1, 2});
        byte[] target = new byte[8];
        Arrays.fill(target, (byte) -1);

        duplicateBytes(slice, target, 1, 1);

        assertArrayEquals(new byte[] {-1, 1, 2, -1, -1, -1, -1, -1}, target);
    }

    @Test
    public void testDuplicateMultipleBytesOffNumberOfTimes()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {5, 3, 1});
        byte[] target = new byte[17];
        Arrays.fill(target, (byte) -1);

        duplicateBytes(slice, target, 1, 5);

        assertArrayEquals(new byte[] {-1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, -1}, target);
    }

    @Test
    public void testDuplicateMultipleBytesEvenNumberOfTimes()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {5, 3, 1});
        byte[] target = new byte[20];
        Arrays.fill(target, (byte) -1);

        duplicateBytes(slice, target, 1, 6);

        assertArrayEquals(new byte[] {-1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, -1}, target);
    }

    @Test
    public void testDuplicateMultipleBytesPowerOfTwoNumberOfTimes()
    {
        Slice slice = Slices.wrappedBuffer(new byte[] {5, 3, 1});
        byte[] target = new byte[14];
        Arrays.fill(target, (byte) -1);

        duplicateBytes(slice, target, 1, 4);

        assertArrayEquals(new byte[] {-1, 5, 3, 1, 5, 3, 1, 5, 3, 1, 5, 3, 1, -1}, target);
    }
}
