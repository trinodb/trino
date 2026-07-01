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
import static io.trino.spi.block.ByteArrayBlockEncoding.compactBytesWithNulls;
import static io.trino.spi.block.ByteArrayBlockEncoding.compactBytesWithNullsVectorized;
import static io.trino.spi.block.ByteArrayBlockEncoding.expandBytesWithNulls;
import static io.trino.spi.block.ByteArrayBlockEncoding.expandBytesWithNullsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.type.TinyintType.TINYINT;

final class TestByteArrayBlockEncoding
        extends BaseBlockEncodingTest<Byte>
{
    @Override
    protected Type getType()
    {
        return TINYINT;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Byte value)
    {
        TINYINT.writeByte(blockBuilder, value);
    }

    @Override
    protected Byte randomValue(Random random)
    {
        return (byte) random.nextInt(0xFF);
    }

    @Test
    void testCompressAndExpandBytesWithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                byte[] values = randomBytes(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactBytes(values, validity, offset, length);
                    ByteArrayBlock actualBlock = expandBytesWithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    ByteArrayBlock expectedBlock = new ByteArrayBlock(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(TINYINT, actualBlock, expectedBlock);

                    byte[] vectorizedCompactedValues = compactBytesVectorized(values, validity, offset, length);
                    ByteArrayBlock vectorizedActualBlock = expandBytesWithNullsVectorized(Slices.wrappedBuffer(vectorizedCompactedValues).getInput(), length, compactedValidity);
                    assertBlockEquals(TINYINT, vectorizedActualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactBytes(byte[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Byte.BYTES + 4));
        compactBytesWithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compactBytesVectorized(byte[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Byte.BYTES + 4));
        compactBytesWithNullsVectorized(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] copyValues(byte[] values, int offset, int length)
    {
        byte[] copy = new byte[length];
        System.arraycopy(values, offset, copy, 0, length);
        return copy;
    }

    private static byte[] randomBytes(int size)
    {
        byte[] data = new byte[size];
        Random random = new Random(42);
        random.nextBytes(data);
        return data;
    }
}
