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
package io.trino.parquet.reader.flat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.parquet.reader.flat.BitPackingUtils.bitCount;
import static io.trino.parquet.reader.flat.BitPackingUtils.unpack;
import static io.trino.parquet.reader.flat.VectorBitPackingUtils.vectorUnpack8FromByte;
import static io.trino.parquet.reader.flat.VectorBitPackingUtils.vectorUnpackAndInvert8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBitPackingUtils
{
    @Test
    public void testBitCount()
    {
        for (int i = 0; i < 256; i++) {
            int count = bitCount((byte) i);
            assertThat(count).isEqualTo(Integer.bitCount(i));
        }
    }

    @Test
    public void testUnpack()
    {
        boolean[] values = new boolean[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            for (int start = 0; start < 8; start++) {
                for (int end = start + 1; end <= 8; end++) {
                    for (int offset : ImmutableList.of(0, 12, 23, 99)) {
                        int nonNullCount = unpack(values, offset, (byte) packedByte, start, end);
                        assertThat(nonNullCount).isEqualTo((end - start) - Integer.bitCount(selectBits(packedByte, start, end)));

                        for (int bit = start; bit < end; bit++) {
                            assertThat(values[offset + bit - start]).isEqualTo(((packedByte >>> bit) & 1) == 1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testUnpackByte()
    {
        Random random = new Random(0);
        byte[] values = new byte[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            for (int start = 0; start < 8; start++) {
                for (int end = start + 1; end <= 8; end++) {
                    int offset = random.nextInt(100);
                    unpack(values, offset, (byte) packedByte, start, end);

                    for (int bit = start; bit < end; bit++) {
                        assertThat(values[offset + bit - start]).isEqualTo((byte) ((packedByte >>> bit) & 1));
                    }
                }
            }
        }
    }

    @Test
    public void testUnpack8()
    {
        Random random = new Random(0);
        boolean[] values = new boolean[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            int offset = random.nextInt(100);
            int nonNullCount = unpack(values, offset, (byte) packedByte);
            assertThat(nonNullCount).isEqualTo(8 - Integer.bitCount(packedByte));

            for (int bit = 0; bit < 8; bit++) {
                assertThat(values[offset + bit]).isEqualTo(((packedByte >>> bit) & 1) == 1);
            }
        }
    }

    @Test
    public void testVectorUnpackAndInvert8()
    {
        Random random = new Random(0);
        boolean[] values = new boolean[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            int offset = random.nextInt(100);
            int nonNullCount = vectorUnpackAndInvert8(values, offset, (byte) packedByte);
            assertThat(nonNullCount).isEqualTo(Integer.bitCount(packedByte));

            for (int bit = 0; bit < 8; bit++) {
                assertThat(values[offset + bit]).isEqualTo(((packedByte >>> bit) & 1) == 0);
            }
        }
    }

    @Test
    public void testUnpack8FromByte()
    {
        Random random = new Random(0);
        byte[] values = new byte[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            int offset = random.nextInt(100);
            BitPackingUtils.unpack8FromByte(values, offset, (byte) packedByte);

            for (int bit = 0; bit < 8; bit++) {
                assertThat(values[offset + bit]).isEqualTo((byte) ((packedByte >>> bit) & 1));
            }
        }
    }

    @Test
    public void testVectorUnpack8FromByte()
    {
        Random random = new Random(0);
        byte[] values = new byte[100 + 8];
        for (int packedByte = 0; packedByte < 256; packedByte++) {
            int offset = random.nextInt(100);
            vectorUnpack8FromByte(values, offset, (byte) packedByte);

            for (int bit = 0; bit < 8; bit++) {
                assertThat(values[offset + bit]).isEqualTo((byte) ((packedByte >>> bit) & 1));
            }
        }
    }

    private static int selectBits(int packedByte, int start, int end)
    {
        return (packedByte >>> start) & ((1 << (end - start)) - 1);
    }
}
