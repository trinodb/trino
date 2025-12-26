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
import io.airlift.slice.Slice;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

final class TestEncoderUtil
{
    private static final int[] TEST_LENGTHS = {0, 3, 255, 257, 512, 530, 1024, 2048, 8192};
    private static final int[] TEST_OFFSETS = {0, 2, 256};
    private static final long RANDOM_SEED = 42;

    private TestEncoderUtil()
    {
    }

    static int[] getTestLengths()
    {
        return TEST_LENGTHS.clone();
    }

    static int[] getTestOffsets()
    {
        return TEST_OFFSETS.clone();
    }

    @Test
    void testEncodeNullBitsScalarEqualsVectorized()
    {
        for (int offset : getTestOffsets()) {
            for (int length : getTestLengths()) {
                for (boolean[] isNull : getIsNullArray(offset + length)) {
                    DynamicSliceOutput scalarOutput = new DynamicSliceOutput((length / 8) + 2);
                    EncoderUtil.encodeNullsAsBitsScalar(scalarOutput, isNull, offset, length);
                    DynamicSliceOutput vectorOutput = new DynamicSliceOutput((length / 8) + 2);
                    EncoderUtil.encodeNullsAsBitsVectorized(vectorOutput, isNull, offset, length);
                    Slice scalarSlice = scalarOutput.slice();
                    Slice vectorSlice = vectorOutput.slice();
                    assertThat(scalarSlice).as("scalar and vector encode results differ").isEqualTo(vectorSlice);
                    boolean[] scalarDecode = EncoderUtil.decodeNullBitsScalar(scalarSlice.getInput(), length).orElseThrow();
                    boolean[] vectorDecode = EncoderUtil.decodeNullBitsVectorized(scalarSlice.getInput(), length).orElseThrow();
                    assertThat(scalarDecode).as("scalar and vector decode results differ").isEqualTo(vectorDecode);
                    assertThat(scalarDecode).as("decode boolean[] differs from input value").isEqualTo(Arrays.copyOfRange(isNull, offset, offset + length));
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

    static byte[] getEncodedNullsAsBits(boolean[] isNull, int offset, int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput((length / 8) + 2);
        EncoderUtil.encodeNullsAsBitsScalar(dynamicSliceOutput, isNull, offset, length);
        Slice encodedSlice = dynamicSliceOutput.slice();
        return EncoderUtil.retrieveNullBits(encodedSlice.getInput(), length);
    }

    public static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertThat(type.getObjectValue(actual, position))
                    .describedAs("position " + position)
                    .isEqualTo(type.getObjectValue(expected, position));
        }
    }

    private static boolean[] all(boolean value, int size)
    {
        boolean[] out = new boolean[size];
        Arrays.fill(out, value);
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
