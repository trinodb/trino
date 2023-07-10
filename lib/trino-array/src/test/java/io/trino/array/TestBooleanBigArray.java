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
package io.trino.array;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBooleanBigArray
{
    @Test
    public void testFill()
    {
        BooleanBigArray array = new BooleanBigArray();
        assertFillCapacity(array, 0, true);
        assertFillCapacity(array, 1, false);
        assertFillCapacity(array, 1000, true);
        assertFillCapacity(array, BigArrays.SEGMENT_SIZE, false);
        assertFillCapacity(array, BigArrays.SEGMENT_SIZE + 1, true);
    }

    private static void assertFillCapacity(BooleanBigArray array, long capacity, boolean value)
    {
        array.ensureCapacity(capacity);
        array.fill(value);

        for (int i = 0; i < capacity; i++) {
            assertThat(array.get(i)).isEqualTo(value);
        }
    }

    @Test
    public void testCopyTo()
    {
        BooleanBigArray source = new BooleanBigArray();
        BooleanBigArray destination = new BooleanBigArray();

        for (long sourceIndex : Arrays.asList(0, 1, BigArrays.SEGMENT_SIZE, BigArrays.SEGMENT_SIZE + 1)) {
            for (long destinationIndex : Arrays.asList(0, 1, BigArrays.SEGMENT_SIZE, BigArrays.SEGMENT_SIZE + 1)) {
                for (long length : Arrays.asList(0, 1, BigArrays.SEGMENT_SIZE, BigArrays.SEGMENT_SIZE + 1)) {
                    assertCopyTo(source, sourceIndex, destination, destinationIndex, length);
                }
            }
        }
    }

    private static void assertCopyTo(BooleanBigArray source, long sourceIndex, BooleanBigArray destination, long destinationIndex, long length)
    {
        long sourceCapacity = sourceIndex + length;
        source.ensureCapacity(sourceCapacity);
        long destinationCapacity = destinationIndex + length + 1; // Add +1 to let us verify that copy does not go out of bounds
        destination.ensureCapacity(destinationCapacity);

        boolean destinationFillValue = true;
        destination.fill(destinationFillValue);

        for (long i = 0; i < sourceCapacity; i++) {
            source.set(i, i % 2 == 0);
        }

        source.copyTo(sourceIndex, destination, destinationIndex, length);

        for (long i = 0; i < destinationIndex; i++) {
            assertThat(destination.get(i)).isEqualTo(destinationFillValue);
        }
        for (long i = 0; i < length; i++) {
            assertThat(source.get(sourceIndex + i)).isEqualTo(destination.get(destinationIndex + i));
        }
        for (long i = destinationIndex + length; i < destinationCapacity; i++) {
            assertThat(destination.get(i)).isEqualTo(destinationFillValue);
        }
    }
}
