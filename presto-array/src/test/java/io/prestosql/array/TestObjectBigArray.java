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
package io.prestosql.array;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestObjectBigArray
{
    @Test
    public void testFill()
    {
        ObjectBigArray<String> array = new ObjectBigArray<>();
        assertFillCapacity(array, 0, "a");
        assertFillCapacity(array, 1, "b");
        assertFillCapacity(array, 1000, "c");
        assertFillCapacity(array, BigArrays.SEGMENT_SIZE, "d");
        assertFillCapacity(array, BigArrays.SEGMENT_SIZE + 1, "e");
    }

    private static <T> void assertFillCapacity(ObjectBigArray<T> array, long capacity, T value)
    {
        array.ensureCapacity(capacity);
        array.fill(value);

        for (int i = 0; i < capacity; i++) {
            assertEquals(array.get(i), value);
        }
    }
}
