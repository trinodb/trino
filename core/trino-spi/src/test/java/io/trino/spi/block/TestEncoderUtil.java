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

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

final class TestEncoderUtil
{
    private static final int[] TEST_LENGTHS = {0, 3, 255, 257, 512, 530, 1024, 2048, 8192};
    private static final int[] TEST_OFFSETS = {0, 2, 256};

    private TestEncoderUtil() {}

    static int[] getTestLengths()
    {
        return TEST_LENGTHS.clone();
    }

    static int[] getTestOffsets()
    {
        return TEST_OFFSETS.clone();
    }

    static long[][] getValidityArrays(int length)
    {
        return new long[][] {
                allValid(length),
                allNull(length),
                alternatingValidity(length),
                randomValidity(length),
        };
    }

    private static long[] allValid(int length)
    {
        long[] validity = new long[Bitmap.wordsForBits(length)];
        Bitmap.setBits(validity, 0, 0, length);
        return validity;
    }

    private static long[] allNull(int length)
    {
        return new long[Bitmap.wordsForBits(length)];
    }

    private static long[] alternatingValidity(int length)
    {
        long[] validity = new long[Bitmap.wordsForBits(length)];
        for (int position = 1; position < length; position += 2) {
            Bitmap.set(validity, 0, position);
        }
        return validity;
    }

    private static long[] randomValidity(int length)
    {
        long[] validity = new long[Bitmap.wordsForBits(length)];
        Random random = new Random(42);
        for (int position = 0; position < length; position++) {
            if (random.nextDouble() >= 0.3) {
                Bitmap.set(validity, 0, position);
            }
        }
        return validity;
    }

    @Test
    void testEncodeValidityAsLongsRoundTrip()
    {
        for (int offset : getTestOffsets()) {
            for (int length : getTestLengths()) {
                long[] valueIsValid = new long[Bitmap.wordsForBits(offset + length)];
                for (int position = 0; position < offset + length; position++) {
                    if (position % 3 != 0) {
                        Bitmap.set(valueIsValid, 0, position);
                    }
                }

                DynamicSliceOutput output = new DynamicSliceOutput(Long.BYTES * Bitmap.wordsForBits(length) + 1);
                EncoderUtil.encodeValidityAsLongs(output, valueIsValid, offset, length);
                Slice slice = output.slice();
                long[] decoded = EncoderUtil.decodeValidityAsLongs(slice.getInput(), length);

                assertThat(decoded).isNotNull();
                for (int position = 0; position < length; position += Long.SIZE) {
                    int remaining = Math.min(Long.SIZE, length - position);
                    long mask = Bitmap.lowBitsMask(remaining);
                    assertThat(Bitmap.getAlignedWord(decoded, 0, position) & mask)
                            .isEqualTo(Bitmap.getAlignedWord(valueIsValid, offset, position) & mask);
                }
            }
        }

        DynamicSliceOutput output = new DynamicSliceOutput(1);
        EncoderUtil.encodeValidityAsLongs(output, null, 0, 10);
        assertThat(EncoderUtil.decodeValidityAsLongs(output.slice().getInput(), 10)).isNull();
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
}
