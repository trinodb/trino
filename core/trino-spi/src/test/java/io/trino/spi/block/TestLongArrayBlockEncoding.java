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

import java.util.Arrays;
import java.util.Random;

import static io.trino.spi.block.EncoderUtil.decodeNullBitsVectorized;
import static io.trino.spi.block.LongArrayBlockEncoding.compactLongsWithNullsScalar;
import static io.trino.spi.block.LongArrayBlockEncoding.compactLongsWithNullsVectorized;
import static io.trino.spi.block.LongArrayBlockEncoding.expandLongsWithNullsScalar;
import static io.trino.spi.block.LongArrayBlockEncoding.expandLongsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.block.TestEncoderUtil.getEncodedNullsAsBits;
import static io.trino.spi.block.TestEncoderUtil.getIsNullArray;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testCompressAndExpandLongsScalarEqualsVector()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                long[] values = randomLongs(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressLongsScalar(values, isNull, offset, length);
                    byte[] vector = compressLongsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("longs: scalar and vector outputs differ").isEqualTo(scalar);
                    byte[] packedIsNullBits = getEncodedNullsAsBits(isNull, offset, length);
                    boolean[] decodedIsNull = decodeNullBitsVectorized(packedIsNullBits, length);
                    assertThat(decodedIsNull).as("decodedIsNull must match input isNull").isEqualTo(Arrays.copyOfRange(isNull, offset, offset + length));
                    LongArrayBlock scalarBlock = expandLongsWithNullsScalar(Slices.wrappedBuffer(scalar).getInput(), length, packedIsNullBits, decodedIsNull);
                    LongArrayBlock vectorBlock = expandLongsWithNullsVectorized(Slices.wrappedBuffer(scalar).getInput(), length, decodedIsNull);
                    assertBlockEquals(BIGINT, scalarBlock, vectorBlock);
                }
            }
        }
    }

    private static byte[] compressLongsScalar(long[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Long.BYTES + 4));
        compactLongsWithNullsScalar(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compressLongsVectorized(long[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Long.BYTES + 4));
        compactLongsWithNullsVectorized(out, values, isNull, offset, length);
        return out.slice().getBytes();
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
