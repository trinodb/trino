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
import static io.trino.spi.block.ShortArrayBlockEncoding.compactShortsWithNullsScalar;
import static io.trino.spi.block.ShortArrayBlockEncoding.compactShortsWithNullsVectorized;
import static io.trino.spi.block.ShortArrayBlockEncoding.expandShortsWithNullsScalar;
import static io.trino.spi.block.ShortArrayBlockEncoding.expandShortsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.block.TestEncoderUtil.getEncodedNullsAsBits;
import static io.trino.spi.block.TestEncoderUtil.getIsNullArray;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testCompressAndExpandShortsScalarEqualsVector()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                short[] values = randomShorts(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressShortsScalar(values, isNull, offset, length);
                    byte[] vector = compressShortsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("shorts: scalar and vector outputs differ").isEqualTo(scalar);
                    byte[] packedIsNullBits = getEncodedNullsAsBits(isNull, offset, length);
                    boolean[] decodedIsNull = decodeNullBitsVectorized(packedIsNullBits, length);
                    assertThat(decodedIsNull).as("decodedIsNull must match input isNull").isEqualTo(Arrays.copyOfRange(isNull, offset, offset + length));
                    ShortArrayBlock scalarBlock = expandShortsWithNullsScalar(Slices.wrappedBuffer(scalar).getInput(), length, packedIsNullBits, decodedIsNull);
                    ShortArrayBlock vectorBlock = expandShortsWithNullsVectorized(Slices.wrappedBuffer(scalar).getInput(), length, decodedIsNull);
                    assertBlockEquals(SMALLINT, scalarBlock, vectorBlock);
                }
            }
        }
    }

    private static byte[] compressShortsScalar(short[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Short.BYTES + 4));
        compactShortsWithNullsScalar(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compressShortsVectorized(short[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Short.BYTES + 4));
        compactShortsWithNullsVectorized(out, values, isNull, offset, length);
        return out.slice().getBytes();
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
