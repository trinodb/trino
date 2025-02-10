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

import static io.trino.spi.block.vstream.ShortStreamVByte.byteWidth;
import static io.trino.spi.block.vstream.ShortStreamVByte.controlBytesTableSize;
import static io.trino.spi.block.vstream.ShortStreamVByte.maxEncodedSize;
import static org.assertj.core.api.Assertions.assertThat;

public class TestShortStreamVByte
{
    @Test
    public void testRoundTrip()
    {
        // Test cases from from https://arxiv.org/abs/1709.08990
        TestData[] testCases = new TestData[] {
                new TestData(new short[] {0, 1, 2, 3, 4, 5, 6}, 8),
                new TestData(new short[] {0, 100, 200, 300, 400, 500, 600, 700}, 15),
                new TestData(new short[] {1024, 12, 10, Short.MAX_VALUE}, 7),
                new TestData(new short[] {1, 2, 3, 1024}, 6),
                new TestData(new short[] {1024, 12, 10, Short.MAX_VALUE, 1, 2, 3, 1024}, 12),
                new TestData(new short[] {100, 10, 200, 20, 32, 64, 128, 0, -1}, 14),
                new TestData(new short[] {Byte.MAX_VALUE}, 2),
                new TestData(new short[] {Short.MAX_VALUE}, 3),
                new TestData(new short[] {Short.MAX_VALUE, Short.MAX_VALUE}, 5),
                new TestData(new short[] {Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE}, 7),
                new TestData(new short[] {Short.MIN_VALUE}, 3),
                new TestData(new short[] {Short.MIN_VALUE, Short.MAX_VALUE}, 5),
        };

        for (TestData test : testCases) {
            Slice slice = Slices.allocate(maxEncodedSize(test.length()));
            int encodedLength = ShortStreamVByte.writeShorts(slice.getOutput(), test.data());
            Slice encoded = slice.slice(0, encodedLength);
            short[] decoded = ShortStreamVByte.readShorts(encoded.getInput(), test.length());

            assertThat(test.data())
                    .isEqualTo(decoded);
            assertThat(encoded.length())
                    .as("encoded length for " + Arrays.toString(test.data()))
                    .isEqualTo(test.expectedByteLength())
                    .as("calculated encoded length for " + Arrays.toString(test.data()))
                    .isEqualTo(expectedByteCount(test.data));
        }
    }

    private static long expectedByteCount(short[] data)
    {
        long byteCount = 0;
        for (short value : data) {
            byteCount += byteWidth(value);
        }
        return byteCount + controlBytesTableSize(data.length);
    }

    private record TestData(short[] data, int expectedByteLength)
    {
        public int length()
        {
            return data.length;
        }
    }
}
