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
package io.trino.util;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLong2LongOpenBigHashMap
{
    @Test
    public void testBasicOps()
    {
        int expected = 100_000;
        Long2LongOpenBigHashMap map = new Long2LongOpenBigHashMap(expected);
        map.defaultReturnValue(-1);

        assertTrue(map.isEmpty());
        assertEquals(map.size(), 0);
        assertEquals(map.get(0), -1);
        assertEquals(map.get(1), -1);

        List<Long> values = Arrays.asList(Long.MIN_VALUE, -10L, 0L, 10L, Long.MAX_VALUE);

        // Put
        int count = 0;
        for (long key : values) {
            count++;
            assertEquals(map.put(key, count - 1), -1);
            assertFalse(map.isEmpty());
            assertEquals(map.size(), count);
        }

        // Replace
        count = 0;
        for (long key : values) {
            count++;
            assertTrue(map.replace(key, count - 1, count));
            assertFalse(map.isEmpty());
            assertEquals(map.size(), values.size());
        }

        // Get
        count = 0;
        for (long key : values) {
            count++;
            assertTrue(map.containsKey(key));
            assertTrue(map.containsValue(count));
            assertEquals(map.get(key), count);
        }

        // Remove
        count = 0;
        for (long key : values) {
            count++;
            assertEquals(map.remove(key), count);
        }
    }

    @Test
    public void testRehash()
    {
        int initialCapacity = 1;
        Long2LongOpenBigHashMap map = new Long2LongOpenBigHashMap(initialCapacity);
        map.defaultReturnValue(-1);

        // Inserting 1M elements should be enough to trigger some rehashes given an initial capacity of 1.

        for (long key = 0; key < 1_000_000; key++) {
            assertEquals(map.put(key, key + 1), -1);
        }

        for (long key = 0; key < 1_000_000; key++) {
            assertEquals(map.get(key), key + 1);
        }

        // Remove most of the elements and force a trim()
        for (long key = 1; key < 1_000_000; key++) {
            map.remove(key);
        }
        map.trim();

        // Make sure we can still fetch the remaining key
        assertEquals(map.get(0), 1);
    }
}
