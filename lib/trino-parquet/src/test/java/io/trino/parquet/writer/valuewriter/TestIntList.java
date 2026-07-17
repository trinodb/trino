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
package io.trino.parquet.writer.valuewriter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestIntList
{
    @Test
    void testEmpty()
    {
        int[] emitted = {0};
        new IntList().forEachSegment((_, _, length) -> emitted[0] += length);
        assertThat(emitted[0]).isZero();
    }

    @Test
    void testSingleSlab()
    {
        // stays within the first slab
        assertRoundTrip(500);
    }

    @Test
    void testSpansInitialSlab()
    {
        // crosses out of the initial slab into a larger one
        assertRoundTrip(2000);
    }

    @Test
    void testSpansManyMaxSlabs()
    {
        // exceeds the slab-size cap so several equally sized slabs are allocated
        assertRoundTrip(16 * 1024 * 4 + 100);
    }

    private static void assertRoundTrip(int size)
    {
        IntList list = new IntList();
        for (int value = 0; value < size; value++) {
            list.add(value);
        }

        int[] collected = new int[size];
        int[] position = {0};
        list.forEachSegment((segment, offset, length) -> {
            assertThat(offset).isZero();
            assertThat(length).isPositive();
            System.arraycopy(segment, offset, collected, position[0], length);
            position[0] += length;
        });

        assertThat(position[0]).isEqualTo(size);
        int[] expected = new int[size];
        for (int value = 0; value < size; value++) {
            expected[value] = value;
        }
        assertThat(collected).isEqualTo(expected);
    }
}
