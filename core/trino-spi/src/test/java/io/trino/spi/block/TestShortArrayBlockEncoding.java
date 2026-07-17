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
import static io.trino.spi.block.ShortArrayBlockEncoding.compactShortsWithNulls;
import static io.trino.spi.block.ShortArrayBlockEncoding.compactShortsWithNullsVectorized;
import static io.trino.spi.block.ShortArrayBlockEncoding.expandShortsWithNulls;
import static io.trino.spi.block.ShortArrayBlockEncoding.expandShortsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.type.SmallintType.SMALLINT;

final class TestShortArrayBlockEncoding
        extends BaseBlockEncodingTest<Short>
{
    @Override
    protected Type getType()
    {
        return SMALLINT;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Short value)
    {
        ((ShortArrayBlockBuilder) blockBuilder).writeShort(value);
    }

    @Override
    protected Short randomValue(Random random)
    {
        return (short) random.nextInt(0xFFFF);
    }

    @Test
    void testCompressAndExpandShortsWithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                short[] values = randomShorts(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactShorts(values, validity, offset, length);
                    ShortArrayBlock actualBlock = expandShortsWithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    ShortArrayBlock expectedBlock = new ShortArrayBlock(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(SMALLINT, actualBlock, expectedBlock);

                    byte[] vectorizedCompactedValues = compactShortsVectorized(values, validity, offset, length);
                    ShortArrayBlock vectorizedActualBlock = expandShortsWithNullsVectorized(Slices.wrappedBuffer(vectorizedCompactedValues).getInput(), length, compactedValidity);
                    assertBlockEquals(SMALLINT, vectorizedActualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactShorts(short[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Short.BYTES + 4));
        compactShortsWithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compactShortsVectorized(short[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Short.BYTES + 4));
        compactShortsWithNullsVectorized(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static short[] copyValues(short[] values, int offset, int length)
    {
        short[] copy = new short[length];
        System.arraycopy(values, offset, copy, 0, length);
        return copy;
    }

    private static short[] randomShorts(int length)
    {
        short[] values = new short[length];
        Random random = new Random(42);
        for (int i = 0; i < length; i++) {
            values[i] = (short) random.nextInt(0xFFFF);
        }
        return values;
    }
}
