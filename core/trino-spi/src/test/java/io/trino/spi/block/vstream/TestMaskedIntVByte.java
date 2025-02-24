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

import static io.trino.spi.block.vstream.MaskedIntVByte.maxEncodedSize;
import static org.assertj.core.api.Assertions.assertThat;

class TestMaskedIntVByte
{
    @Test
    public void testRoundTrip()
    {
        TestData[] testCases = new TestData[] {
                new TestData(new int[] {0, 1, 2, 3, 4, 5, 6}, 7),
                new TestData(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}, 11),
                new TestData(new int[] {0, 100, 200, 300, 400, 500, 600, 700}, 14),
                new TestData(new int[] {1024, 12, 10, 1_073_741_824}, 9),
                new TestData(new int[] {1, 2, 3, 1024}, 5),
                new TestData(new int[] {1024, 12, 10, 1_073_741_824, 1, 2, 3, 1024}, 14),
                new TestData(new int[] {100, 10, 200, 20, 32, 64, 128, 0, -1}, 15),
                new TestData(new int[] {Byte.MAX_VALUE}, 1),
                new TestData(new int[] {Short.MAX_VALUE}, 3),
                new TestData(new int[] {Short.MAX_VALUE, Short.MAX_VALUE}, 6),
                new TestData(new int[] {Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE}, 9),
                new TestData(new int[] {Short.MIN_VALUE}, 5),
                new TestData(new int[] {Short.MIN_VALUE, Short.MAX_VALUE}, 8),
                new TestData(powerOfTwos(), 511),
                new TestData(new int[] {1912210}, 3),
        };

        for (TestData test : testCases) {
            Slice slice = Slices.allocate(maxEncodedSize(test.length()));
            long encodedLength = MaskedIntVByte.writeInts(slice.getOutput(), test.data());
            Slice encoded = slice.slice(0, (int) encodedLength);
            int[] decoded = MaskedIntVByte.readInts(encoded.getInput(), test.length());

            assertThat(test.data())
                    .isEqualTo(decoded);
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
            int[] data = random.ints(length).toArray();

            Slice slice = Slices.allocate(maxEncodedSize(data.length));
            long encodedLength = MaskedIntVByte.writeInts(slice.getOutput(), data);
            Slice encoded = slice.slice(0, (int) encodedLength);
            int[] decoded = MaskedIntVByte.readInts(encoded.getInput(), data.length);

            assertThat(data)
                    .isEqualTo(decoded);
        }
    }

    private static int[] powerOfTwos()
    {
        int[] data = new int[32 * 6];
        for (int i = 0; i < 32; i++) {
            data[i] = 1 << i;
            data[i * 2] = -(1 << i);
            data[i * 3] = (1 << i) - 1;
            data[i * 4] = (1 << i) + 1;
            data[i * 5] = -(1 << i) - 1;
            data[i * 6] = -(1 << i) + 1;
        }
        return data;
    }

    private record TestData(int[] data, int expectedByteLength)
    {
        public int length()
        {
            return data.length;
        }
    }
}
