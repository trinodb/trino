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
import static io.trino.spi.block.IntArrayBlockEncoding.compactIntsWithNullsScalar;
import static io.trino.spi.block.IntArrayBlockEncoding.compactIntsWithNullsVectorized;
import static io.trino.spi.block.IntArrayBlockEncoding.expandIntsWithNullsScalar;
import static io.trino.spi.block.IntArrayBlockEncoding.expandIntsWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.block.TestEncoderUtil.getEncodedNullsAsBits;
import static io.trino.spi.block.TestEncoderUtil.getIsNullArray;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testCompressAndExpandIntsScalarEqualsVector()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                int[] values = randomInts(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressIntsScalar(values, isNull, offset, length);
                    byte[] vector = compressIntsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("ints: scalar and vector outputs differ").isEqualTo(scalar);
                    byte[] packedIsNullBits = getEncodedNullsAsBits(isNull, offset, length);
                    boolean[] decodedIsNull = decodeNullBitsVectorized(packedIsNullBits, length);
                    assertThat(decodedIsNull).as("decodedIsNull must match input isNull").isEqualTo(Arrays.copyOfRange(isNull, offset, offset + length));
                    IntArrayBlock scalarBlock = expandIntsWithNullsScalar(Slices.wrappedBuffer(scalar).getInput(), length, packedIsNullBits, decodedIsNull);
                    IntArrayBlock vectorBlock = expandIntsWithNullsVectorized(Slices.wrappedBuffer(vector).getInput(), length, decodedIsNull);
                    assertBlockEquals(INTEGER, scalarBlock, vectorBlock);
                }
            }
        }
    }

    private static byte[] compressIntsScalar(int[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Integer.BYTES + 4));
        compactIntsWithNullsScalar(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compressIntsVectorized(int[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Integer.BYTES + 4));
        compactIntsWithNullsVectorized(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private int[] randomInts(int length)
    {
        int[] values = new int[length];
        Random random = new Random(42);
        for (int i = 0; i < length; i++) {
            values[i] = random.nextInt();
        }
        return values;
    }
}
