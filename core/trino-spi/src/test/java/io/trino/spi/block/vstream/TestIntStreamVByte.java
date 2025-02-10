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

import static io.trino.spi.block.vstream.IntStreamVByte.byteWidth;
import static io.trino.spi.block.vstream.IntStreamVByte.controlBytesTableSize;
import static io.trino.spi.block.vstream.IntStreamVByte.maxEncodedSize;
import static org.assertj.core.api.Assertions.assertThat;

class TestIntStreamVByte
{
    @Test
    public void testRoundTrip()
    {
        // Test cases from from https://arxiv.org/abs/1709.08990
        TestData[] testCases = new TestData[] {
                new TestData(new int[] {0, 1, 2, 3, 4, 5, 6}, 9),
                new TestData(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}, 10),
                new TestData(new int[] {0, 100, 200, 300, 400, 500, 600, 700}, 16),
                new TestData(new int[] {1024, 12, 10, 1_073_741_824}, 9),
                new TestData(new int[] {1, 2, 3, 1024}, 6),
                new TestData(new int[] {1024, 12, 10, 1_073_741_824, 1, 2, 3, 1024}, 15),
                new TestData(new int[] {100, 10, 200, 20, 32, 64, 128, 0, -1}, 17),
                new TestData(new int[] {Byte.MAX_VALUE}, 2),
                new TestData(new int[] {Short.MAX_VALUE}, 3),
                new TestData(new int[] {Short.MAX_VALUE, Short.MAX_VALUE}, 5),
                new TestData(new int[] {Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE}, 7),
                new TestData(new int[] {Short.MIN_VALUE}, 5),
                new TestData(new int[] {Short.MIN_VALUE, Short.MAX_VALUE}, 7),
                new TestData(powerOfTwos(), 497),
                new TestData(new int[] {1912210}, 4),
        };

        for (TestData test : testCases) {
            Slice slice = Slices.allocate(maxEncodedSize(test.length()));
            long encodedLength = IntStreamVByte.writeInts(slice.getOutput(), test.data());
            Slice encoded = slice.slice(0, (int) encodedLength);
            int[] decoded = IntStreamVByte.readInts(encoded.getInput(), test.length());

            assertThat(test.data())
                    .isEqualTo(decoded);
            assertThat(encoded.length())
                    .as("encoded length for " + Arrays.toString(test.data()))
                    .isEqualTo(test.expectedByteLength())
                    .as("calculated encoded length for " + Arrays.toString(test.data()))
                    .isEqualTo(expectedByteCount(test.data));
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
            long encodedLength = IntStreamVByte.writeInts(slice.getOutput(), data);
            Slice encoded = slice.slice(0, (int) encodedLength);
            int[] decoded = IntStreamVByte.readInts(encoded.getInput(), data.length);

            assertThat(data)
                    .isEqualTo(decoded);
        }
    }

    private static long expectedByteCount(int[] data)
    {
        long byteCount = 0;
        for (int value : data) {
            byteCount += byteWidth(value);
        }
        return byteCount + controlBytesTableSize(data.length);
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
