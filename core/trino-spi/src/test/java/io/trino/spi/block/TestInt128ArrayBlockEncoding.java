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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Random;

import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Int128ArrayBlockEncoding.compactInt128WithNulls;
import static io.trino.spi.block.Int128ArrayBlockEncoding.expandInt128WithNulls;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;

final class TestInt128ArrayBlockEncoding
        extends BaseBlockEncodingTest<Int128>
{
    private static final DecimalType TYPE = DecimalType.createDecimalType(30);

    @Override
    protected Type getType()
    {
        return TYPE;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Int128 value)
    {
        TYPE.writeObject(blockBuilder, value);
    }

    @Override
    protected Int128 randomValue(Random random)
    {
        BigInteger bound = Decimals.bigIntegerTenToNth(TYPE.getPrecision());
        BigInteger magnitude = new BigInteger(128, random).mod(bound);
        return Int128.valueOf(random.nextBoolean() ? magnitude : magnitude.negate());
    }

    @Test
    void testCompressAndExpandInt128WithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                long[] values = randomInt128Values(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactInt128(values, validity, offset, length);
                    Int128ArrayBlock actualBlock = expandInt128WithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    Int128ArrayBlock expectedBlock = new Int128ArrayBlock(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(TYPE, actualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactInt128(long[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput((length * Int128ArrayBlock.INT128_BYTES) + Integer.BYTES);
        compactInt128WithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static long[] copyValues(long[] values, int offset, int length)
    {
        long[] copy = new long[length * 2];
        System.arraycopy(values, offset * 2, copy, 0, length * 2);
        return copy;
    }

    private static long[] randomInt128Values(int length)
    {
        long[] values = new long[length * 2];
        Random random = new Random(42);
        BigInteger bound = Decimals.bigIntegerTenToNth(TYPE.getPrecision());
        for (int position = 0; position < length; position++) {
            int valueIndex = position * 2;
            BigInteger magnitude = new BigInteger(128, random).mod(bound);
            Int128 value = Int128.valueOf(random.nextBoolean() ? magnitude : magnitude.negate());
            values[valueIndex] = value.getHigh();
            values[valueIndex + 1] = value.getLow();
        }
        return values;
    }
}
