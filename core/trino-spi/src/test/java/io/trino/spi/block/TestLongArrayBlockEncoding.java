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
import static io.trino.spi.block.LongArrayBlockEncoding.compactLongsWithNulls;
import static io.trino.spi.block.LongArrayBlockEncoding.compactLongsWithNullsVectorized;
import static io.trino.spi.block.LongArrayBlockEncoding.expandLongsWithNulls;
import static io.trino.spi.block.LongArrayBlockEncoding.expandLongsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.type.BigintType.BIGINT;

final class TestLongArrayBlockEncoding
        extends BaseBlockEncodingTest<Long>
{
    @Override
    protected Type getType()
    {
        return BIGINT;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Long value)
    {
        BIGINT.writeLong(blockBuilder, value);
    }

    @Override
    protected Long randomValue(Random random)
    {
        return random.nextLong();
    }

    @Test
    void testCompressAndExpandLongsWithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                long[] values = randomLongs(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactLongs(values, validity, offset, length);
                    LongArrayBlock actualBlock = expandLongsWithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    LongArrayBlock expectedBlock = new LongArrayBlock(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(BIGINT, actualBlock, expectedBlock);

                    byte[] vectorizedCompactedValues = compactLongsVectorized(values, validity, offset, length);
                    LongArrayBlock vectorizedActualBlock = expandLongsWithNullsVectorized(Slices.wrappedBuffer(vectorizedCompactedValues).getInput(), length, compactedValidity);
                    assertBlockEquals(BIGINT, vectorizedActualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactLongs(long[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Long.BYTES + 4));
        compactLongsWithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compactLongsVectorized(long[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Long.BYTES + 4));
        compactLongsWithNullsVectorized(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static long[] copyValues(long[] values, int offset, int length)
    {
        long[] copy = new long[length];
        System.arraycopy(values, offset, copy, 0, length);
        return copy;
    }

    private static long[] randomLongs(int length)
    {
        long[] values = new long[length];
        Random random = new Random(42);
        for (int i = 0; i < length; i++) {
            values[i] = random.nextLong();
        }
        return values;
    }
}
