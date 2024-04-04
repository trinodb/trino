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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongBigArrayFIFOQueue
{
    @Test
    public void testEnqueueDequeue()
    {
        LongBigArrayFIFOQueue queue = new LongBigArrayFIFOQueue();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.longSize()).isEqualTo(0L);

        // Enqueue elements
        for (long i = 1; i <= 100; i++) {
            queue.enqueue(i);
            assertThat(queue.isEmpty()).isFalse();
            assertThat(queue.lastLong()).isEqualTo(i);
            assertThat(queue.size()).isEqualTo(i);
            assertThat(queue.longSize()).isEqualTo(i);
        }

        assertThat(queue.firstLong()).isEqualTo(1);
        assertThat(queue.lastLong()).isEqualTo(100);

        // Dequeue elements
        for (long i = 1; i <= 100; i++) {
            assertThat(queue.size()).isEqualTo(100 - i + 1);
            assertThat(queue.longSize()).isEqualTo(100 - i + 1);
            assertThat(queue.dequeueLong()).isEqualTo(i);
        }

        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.longSize()).isEqualTo(0L);
    }

    @Test
    public void testReverseEnqueueDequeue()
    {
        LongBigArrayFIFOQueue queue = new LongBigArrayFIFOQueue();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.longSize()).isEqualTo(0L);

        // Enqueue elements
        for (long i = 1; i <= 100; i++) {
            queue.enqueueFirst(i);
            assertThat(queue.isEmpty()).isFalse();
            assertThat(queue.firstLong()).isEqualTo(i);
            assertThat(queue.size()).isEqualTo(i);
            assertThat(queue.longSize()).isEqualTo(i);
        }

        assertThat(queue.firstLong()).isEqualTo(100);
        assertThat(queue.lastLong()).isEqualTo(1);

        // Dequeue elements
        for (long i = 1; i <= 100; i++) {
            assertThat(queue.size()).isEqualTo(100 - i + 1);
            assertThat(queue.longSize()).isEqualTo(100 - i + 1);
            assertThat(queue.dequeueLastLong()).isEqualTo(i);
        }

        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.longSize()).isEqualTo(0L);
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
            assertThat(queue.dequeueLong()).isEqualTo(i);
        }
        queue.trim();
        for (long i = 0; i < 1_000_000; i++) {
            queue.enqueue(i);
        }
        for (long i = 0; i < 1_000_000; i++) {
            assertThat(queue.dequeueLong()).isEqualTo(i);
        }
    }
}
