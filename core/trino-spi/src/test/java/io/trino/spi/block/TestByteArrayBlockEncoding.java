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

import static io.trino.spi.block.ByteArrayBlockEncoding.compactBytesWithNullsScalar;
import static io.trino.spi.block.ByteArrayBlockEncoding.compactBytesWithNullsVectorized;
import static io.trino.spi.block.ByteArrayBlockEncoding.expandBytesWithNullsScalar;
import static io.trino.spi.block.ByteArrayBlockEncoding.expandBytesWithNullsVectorized;
import static io.trino.spi.block.EncoderUtil.decodeNullBitsVectorized;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.block.TestEncoderUtil.getEncodedNullsAsBits;
import static io.trino.spi.block.TestEncoderUtil.getIsNullArray;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testCompressAndExpandBytesScalarEqualsVector()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                byte[] values = randomBytes(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] compressedScalar = compressBytesScalar(values, isNull, offset, length);
                    byte[] compressedVectorized = compressBytesVectorized(values, isNull, offset, length);
                    assertThat(compressedVectorized).as("bytes: compressedScalar and vector outputs differ").isEqualTo(compressedScalar);
                    byte[] packedIsNullBits = getEncodedNullsAsBits(isNull, offset, length);
                    boolean[] decodedIsNull = decodeNullBitsVectorized(packedIsNullBits, length);
                    assertThat(decodedIsNull).as("decodedIsNull must match input isNull").isEqualTo(Arrays.copyOfRange(isNull, offset, offset + length));
                    ByteArrayBlock scalarBlock = expandBytesWithNullsScalar(Slices.wrappedBuffer(compressedScalar).getInput(), length, packedIsNullBits, decodedIsNull);
                    ByteArrayBlock vectorBlock = expandBytesWithNullsVectorized(Slices.wrappedBuffer(compressedScalar).getInput(), length, decodedIsNull);
                    assertBlockEquals(BOOLEAN, scalarBlock, vectorBlock);
                }
            }
        }
    }

    static byte[] compressBytesScalar(byte[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Byte.BYTES + 4));
        compactBytesWithNullsScalar(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] compressBytesVectorized(byte[] values, boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput(length * (Byte.BYTES + 4));
        compactBytesWithNullsVectorized(out, values, isNull, offset, length);
        return out.slice().getBytes();
    }

    private static byte[] randomBytes(int size)
    {
        byte[] data = new byte[size];
        Random r = new Random(42);
        r.nextBytes(data);
        return data;
    }
}
