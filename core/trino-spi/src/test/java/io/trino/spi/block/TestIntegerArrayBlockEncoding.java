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
package io.trino.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.IntArrayBlockEncoding.compactIntsWithNulls;
import static io.trino.spi.block.IntArrayBlockEncoding.compactIntsWithNullsVectorized;
import static io.trino.spi.block.IntArrayBlockEncoding.expandIntsWithNulls;
import static io.trino.spi.block.IntArrayBlockEncoding.expandIntsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.type.IntegerType.INTEGER;

final class TestIntegerArrayBlockEncoding
        extends BaseBlockEncodingTest<Integer>
{
    @Override
    protected Type getType()
    {
        return INTEGER;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Integer value)
    {
        ((IntArrayBlockBuilder) blockBuilder).writeInt(value);
    }

    @Override
    protected Integer randomValue(Random random)
    {
        return random.nextInt();
    }

    @Test
    void testCompressAndExpandIntsWithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                int[] values = randomInts(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactInts(values, validity, offset, length);
                    IntArrayBlock actualBlock = expandIntsWithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    IntArrayBlock expectedBlock = new IntArrayBlock(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(INTEGER, actualBlock, expectedBlock);

                    byte[] vectorizedCompactedValues = compactIntsVectorized(values, validity, offset, length);
                    IntArrayBlock vectorizedActualBlock = expandIntsWithNullsVectorized(Slices.wrappedBuffer(vectorizedCompactedValues).getInput(), length, compactedValidity);
                    assertBlockEquals(INTEGER, vectorizedActualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactInts(int[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Integer.BYTES + 4));
        compactIntsWithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compactIntsVectorized(int[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Integer.BYTES + 4));
        compactIntsWithNullsVectorized(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static int[] copyValues(int[] values, int offset, int length)
    {
        int[] copy = new int[length];
        System.arraycopy(values, offset, copy, 0, length);
        return copy;
    }

    private static int[] randomInts(int length)
    {
        int[] values = new int[length];
        Random random = new Random(42);
        for (int i = 0; i < length; i++) {
            values[i] = random.nextInt();
        }
        return values;
    }
}
