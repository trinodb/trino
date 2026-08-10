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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static io.trino.spi.block.Fixed12BlockEncoding.compactFixed12WithNulls;
import static io.trino.spi.block.Fixed12BlockEncoding.expandFixed12WithNulls;
import static io.trino.spi.block.TestEncoderUtil.assertBlockEquals;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

final class TestFixed12BlockEncoding
        extends BaseBlockEncodingTest<LongTimestamp>
{
    @Override
    protected Type getType()
    {
        return TIMESTAMP_PICOS;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, LongTimestamp value)
    {
        TIMESTAMP_PICOS.writeObject(blockBuilder, value);
    }

    @Override
    protected LongTimestamp randomValue(Random random)
    {
        return new LongTimestamp(random.nextLong(), random.nextInt(PICOSECONDS_PER_MICROSECOND));
    }

    @Test
    void testCompressAndExpandFixed12WithValidity()
    {
        for (int length : TestEncoderUtil.getTestLengths()) {
            for (int offset : TestEncoderUtil.getTestOffsets()) {
                int[] values = randomFixed12(offset + length);
                for (long[] validity : TestEncoderUtil.getValidityArrays(offset + length)) {
                    if (!hasUnsetBit(validity, offset, length)) {
                        continue;
                    }
                    long[] compactedValidity = compactBitmap(validity, offset, length);
                    byte[] compactedValues = compactFixed12(values, validity, offset, length);
                    Fixed12Block actualBlock = expandFixed12WithNulls(Slices.wrappedBuffer(compactedValues).getInput(), length, compactedValidity);
                    Fixed12Block expectedBlock = new Fixed12Block(0, length, compactedValidity, copyValues(values, offset, length));
                    assertBlockEquals(TIMESTAMP_PICOS, actualBlock, expectedBlock);
                }
            }
        }
    }

    private static byte[] compactFixed12(int[] values, long[] validity, int offset, int length)
    {
        DynamicSliceOutput out = new DynamicSliceOutput((length * Fixed12Block.FIXED12_BYTES) + Integer.BYTES);
        compactFixed12WithNulls(out, values, validity, offset, length);
        return out.slice().getBytes();
    }

    private static int[] copyValues(int[] values, int offset, int length)
    {
        int[] copy = new int[length * 3];
        System.arraycopy(values, offset * 3, copy, 0, length * 3);
        return copy;
    }

    private static int[] randomFixed12(int length)
    {
        int[] values = new int[length * 3];
        Random random = new Random(42);
        for (int position = 0; position < length; position++) {
            encodeFixed12(random.nextLong(), random.nextInt(PICOSECONDS_PER_MICROSECOND), values, position);
        }
        return values;
    }
}
