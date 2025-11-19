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
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.spi.block.EncoderUtil.compactBytesWithNullsScalar;
import static io.trino.spi.block.EncoderUtil.compactBytesWithNullsVectorized;
import static io.trino.spi.block.EncoderUtil.compactIntsWithNullsScalar;
import static io.trino.spi.block.EncoderUtil.compactIntsWithNullsVectorized;
import static io.trino.spi.block.EncoderUtil.compactLongsWithNullsScalar;
import static io.trino.spi.block.EncoderUtil.compactLongsWithNullsVectorized;
import static io.trino.spi.block.EncoderUtil.compactShortsWithNullsScalar;
import static io.trino.spi.block.EncoderUtil.compactShortsWithNullsVectorized;
import static org.assertj.core.api.Assertions.assertThat;

final class TestEncoderUtil
{
    private static final int[] TEST_LENGTHS = {0, 3, 255, 257, 512, 530, 1024, 2048, 8192};
    private static final int[] TEST_OFFSETS = {0, 2, 256};
    private static final long RANDOM_SEED = 42;

    @Test
    void testBytesScalarEqualsVector()
    {
        for (int length : TEST_LENGTHS) {
            for (int offset : TEST_OFFSETS) {
                byte[] values = randomBytes(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressBytesScalar(values, isNull, offset, length);
                    byte[] vector = compressBytesVectorized(values, isNull, offset, length);
                    assertThat(vector).as("bytes: scalar and vector outputs differ").isEqualTo(scalar);
                }
            }
        }
    }

    @Test
    void testShortsScalarEqualsVector()
    {
        for (int length : TEST_LENGTHS) {
            for (int offset : TEST_OFFSETS) {
                short[] values = randomShorts(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressShortsScalar(values, isNull, offset, length);
                    byte[] vector = compressShortsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("shorts: scalar and vector outputs differ").isEqualTo(scalar);
                }
            }
        }
    }

    @Test
    void testIntsScalarEqualsVector()
    {
        for (int length : TEST_LENGTHS) {
            for (int offset : TEST_OFFSETS) {
                int[] values = randomInts(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressIntsScalar(values, isNull, offset, length);
                    byte[] vector = compressIntsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("ints: scalar and vector outputs differ").isEqualTo(scalar);
                }
            }
        }
    }

    @Test
    void testLongsScalarEqualsVector()
    {
        for (int length : TEST_LENGTHS) {
            for (int offset : TEST_OFFSETS) {
                long[] values = randomLongs(offset + length);
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    byte[] scalar = compressLongsScalar(values, isNull, offset, length);
                    byte[] vector = compressLongsVectorized(values, isNull, offset, length);
                    assertThat(vector).as("longs: scalar and vector outputs differ").isEqualTo(scalar);
                }
            }
        }
    }

    static boolean[][] getIsNullArray(int length)
    {
        return new boolean[][] {
                all(false, length),
                all(true, length),
                alternating(length),
                randomBooleans(length)};
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

    static byte[] compressLongsScalar(long[] values, boolean[] isNull, int offset, int length)
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

    private static byte[] randomBytes(int size)
    {
        byte[] data = new byte[size];
        Random r = new Random(RANDOM_SEED);
        r.nextBytes(data);
        return data;
    }

    private static short[] randomShorts(int size)
    {
        short[] data = new short[size];
        Random r = new Random(RANDOM_SEED);
        for (int i = 0; i < size; i++) {
            data[i] = (short) r.nextInt();
        }
        return data;
    }

    private static int[] randomInts(int size)
    {
        int[] data = new int[size];
        Random r = new Random(RANDOM_SEED);
        for (int i = 0; i < size; i++) {
            data[i] = r.nextInt();
        }
        return data;
    }

    private static long[] randomLongs(int size)
    {
        long[] data = new long[size];
        Random r = new Random(RANDOM_SEED);
        for (int i = 0; i < size; i++) {
            data[i] = r.nextLong();
        }
        return data;
    }

    private static boolean[] all(boolean value, int size)
    {
        boolean[] out = new boolean[size];
        if (value) {
            for (int i = 0; i < size; i++) {
                out[i] = true;
            }
        }
        return out;
    }

    private static boolean[] alternating(int size)
    {
        boolean[] out = new boolean[size];
        for (int i = 0; i < size; i++) {
            out[i] = (i % 2) == 0;
        }
        return out;
    }

    private static boolean[] randomBooleans(int size)
    {
        boolean[] out = new boolean[size];
        Random r = new Random(RANDOM_SEED);
        for (int i = 0; i < size; i++) {
            out[i] = r.nextDouble() < 0.3;
        }
        return out;
    }
}
