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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLongBigArrayFIFOQueue
{
    @Test
    public void testEnqueueDequeue()
    {
        LongBigArrayFIFOQueue queue = new LongBigArrayFIFOQueue();
        assertTrue(queue.isEmpty());
        assertEquals(queue.size(), 0);
        assertEquals(queue.longSize(), 0L);

        // Enqueue elements
        for (long i = 1; i <= 100; i++) {
            queue.enqueue(i);
            assertFalse(queue.isEmpty());
            assertEquals(queue.lastLong(), i);
            assertEquals(queue.size(), i);
            assertEquals(queue.longSize(), i);
        }

        assertEquals(queue.firstLong(), 1);
        assertEquals(queue.lastLong(), 100);

        // Dequeue elements
        for (long i = 1; i <= 100; i++) {
            assertEquals(queue.size(), 100 - i + 1);
            assertEquals(queue.longSize(), 100 - i + 1);
            assertEquals(queue.dequeueLong(), i);
        }

        assertTrue(queue.isEmpty());
        assertEquals(queue.size(), 0);
        assertEquals(queue.longSize(), 0L);
    }

    @Test
    public void testReverseEnqueueDequeue()
    {
        LongBigArrayFIFOQueue queue = new LongBigArrayFIFOQueue();
        assertTrue(queue.isEmpty());
        assertEquals(queue.size(), 0);
        assertEquals(queue.longSize(), 0L);

        // Enqueue elements
        for (long i = 1; i <= 100; i++) {
            queue.enqueueFirst(i);
            assertFalse(queue.isEmpty());
            assertEquals(queue.firstLong(), i);
            assertEquals(queue.size(), i);
            assertEquals(queue.longSize(), i);
        }

        assertEquals(queue.firstLong(), 100);
        assertEquals(queue.lastLong(), 1);

        // Dequeue elements
        for (long i = 1; i <= 100; i++) {
            assertEquals(queue.size(), 100 - i + 1);
            assertEquals(queue.longSize(), 100 - i + 1);
            assertEquals(queue.dequeueLastLong(), i);
        }

        assertTrue(queue.isEmpty());
        assertEquals(queue.size(), 0);
        assertEquals(queue.longSize(), 0L);
    }

    @Test
    public void testResize()
    {
        int initialCapacity = 1;
        LongBigArrayFIFOQueue queue = new LongBigArrayFIFOQueue(initialCapacity);

        // Inserting 1M elements should be enough to trigger some resizes given an initial capacity of 1.

        for (long i = 0; i < 1_000_000; i++) {
            queue.enqueue(i);
        }
        for (long i = 0; i < 1_000_000; i++) {
            assertEquals(queue.dequeueLong(), i);
        }
        queue.trim();
        for (long i = 0; i < 1_000_000; i++) {
            queue.enqueue(i);
        }
        for (long i = 0; i < 1_000_000; i++) {
            assertEquals(queue.dequeueLong(), i);
        }
    }
}
