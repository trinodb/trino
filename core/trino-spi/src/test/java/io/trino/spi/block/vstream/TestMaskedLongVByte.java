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
package io.trino.spi.block.vstream;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static io.trino.spi.block.vstream.MaskedLongVByte.maxEncodedSize;
import static org.assertj.core.api.Assertions.assertThat;

class TestMaskedLongVByte
{
    @Test
    public void testRoundTrip()
    {
        TestData[] testCases = new TestData[] {
                new TestData(new long[] {0, 1, 2, 3, 4, 5, 6}, 7),
                new TestData(new long[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}, 11),
                new TestData(new long[] {0, 100, 200, 300, 400, 500, 600, 700}, 15),
                new TestData(new long[] {1024, 12, 10, 1_073_741_824}, 9),
                new TestData(new long[] {1, 2, 3, 1024}, 5),
                new TestData(new long[] {1024, 12, 10, 1_073_741_824, 1, 2, 3, 1024}, 14),
                new TestData(new long[] {100, 10, 200, 20, 32, 64, 128, 0, -1}, 13),
                new TestData(new long[] {Byte.MAX_VALUE}, 2),
                new TestData(new long[] {Short.MAX_VALUE}, 3),
                new TestData(new long[] {Short.MAX_VALUE, Short.MAX_VALUE}, 6),
                new TestData(new long[] {Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE}, 9),
                new TestData(new long[] {Short.MIN_VALUE}, 3),
                new TestData(new long[] {Short.MIN_VALUE, Short.MAX_VALUE}, 6),
                new TestData(new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}, 10),
                new TestData(new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, 20),
                new TestData(new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE}, 21),
                new TestData(new long[] {1912210}, 4),
                new TestData(new long[] {8212879628873029L}, 8),
                new TestData(powerOfTwos(), 1586),
        };

        for (TestData test : testCases) {
            Slice slice = Slices.allocate(maxEncodedSize(test.length()));
            int encodedLength = MaskedLongVByte.writeLongs(slice.getOutput(), test.data());
            Slice encoded = slice.slice(0, encodedLength);
            long[] decoded = MaskedLongVByte.readLongs(encoded.getInput(), test.length());

            assertThat(decoded)
                    .as("decoded array for " + Arrays.toString(test.data()))
                    .isEqualTo(test.data());
            assertThat(encoded.length())
                    .as("encoded length for " + Arrays.toString(test.data()))
                    .isEqualTo(test.expectedByteLength());
        }
    }

    @Test
    public void testRandomData()
    {
        Random random = new Random();
        int[] lengths = new int[] {10, 100, 1_000, 1_000_000};

        for (int length : lengths) {
            long[] data = random.longs(length).toArray();

            Slice slice = Slices.allocate(MaskedLongVByte.maxEncodedSize(length));
            long encodedLength = MaskedLongVByte.writeLongs(slice.getOutput(), data);
            Slice encoded = slice.slice(0, (int) encodedLength);
            long[] decoded = MaskedLongVByte.readLongs(encoded.getInput(), length);

            assertThat(data)
                    .isEqualTo(decoded);
        }
    }

    private record TestData(long[] data, int expectedByteLength)
    {
        public int length()
        {
            return data.length;
        }
    }

    private static long[] powerOfTwos()
    {
        long[] data = new long[64 * 6];
        for (int i = 0; i < 64; i++) {
            data[i] = 1L << i;
            data[i * 2] = -(1L << i);
            data[i * 3] = (1L << i) - 1;
            data[i * 4] = (1L << i) + 1;
            data[i * 5] = -(1L << i) - 1;
            data[i * 6] = -(1L << i) + 1;
        }
        return data;
    }
}
