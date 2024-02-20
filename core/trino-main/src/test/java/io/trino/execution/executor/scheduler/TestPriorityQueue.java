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
package io.trino.execution.executor.scheduler;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPriorityQueue
{
    @Test
    public void testEmpty()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        assertThat(queue.poll()).isNull();
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testNotEmpty()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("hello", 1);
        assertThat(queue.isEmpty()).isFalse();
    }

    @Test
    public void testDuplicate()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("hello", 1);
        assertThatThrownBy(() -> queue.add("hello", 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testOrder()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("jumps", 5);
        queue.add("fox", 4);
        queue.add("over", 6);
        queue.add("brown", 3);
        queue.add("dog", 8);
        queue.add("the", 1);
        queue.add("lazy", 7);
        queue.add("quick", 2);

        assertThat(queue.poll()).isEqualTo("the");
        assertThat(queue.poll()).isEqualTo("quick");
        assertThat(queue.poll()).isEqualTo("brown");
        assertThat(queue.poll()).isEqualTo("fox");
        assertThat(queue.poll()).isEqualTo("jumps");
        assertThat(queue.poll()).isEqualTo("over");
        assertThat(queue.poll()).isEqualTo("lazy");
        assertThat(queue.poll()).isEqualTo("dog");
        assertThat(queue.poll()).isNull();
    }

    @Test
    public void testInterleaved()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("jumps", 5);
        queue.add("over", 6);
        queue.add("fox", 4);

        assertThat(queue.poll()).isEqualTo("fox");
        assertThat(queue.poll()).isEqualTo("jumps");

        queue.add("brown", 3);
        queue.add("dog", 8);
        queue.add("the", 1);

        assertThat(queue.poll()).isEqualTo("the");
        assertThat(queue.poll()).isEqualTo("brown");
        assertThat(queue.poll()).isEqualTo("over");

        queue.add("lazy", 7);
        queue.add("quick", 2);

        assertThat(queue.poll()).isEqualTo("quick");
        assertThat(queue.poll()).isEqualTo("lazy");
        assertThat(queue.poll()).isEqualTo("dog");
        assertThat(queue.poll()).isNull();
    }

    @Test
    public void testRemove()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("fox", 4);
        queue.add("brown", 3);
        queue.add("the", 1);
        queue.add("quick", 2);

        queue.remove("brown");

        assertThat(queue.poll()).isEqualTo("the");
        assertThat(queue.poll()).isEqualTo("quick");
        assertThat(queue.poll()).isEqualTo("fox");
        assertThat(queue.poll()).isNull();
    }

    @Test
    public void testRemoveMissing()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("the", 1);
        queue.add("quick", 2);
        queue.add("brown", 3);

        assertThatThrownBy(() -> queue.remove("fox"))
                .isInstanceOf(IllegalArgumentException.class);

        queue.removeIfPresent("fox");
    }

    @Test
    public void testContains()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("the", 1);
        queue.add("quick", 2);
        queue.add("brown", 3);

        assertThat(queue.contains("quick")).isTrue();
        assertThat(queue.contains("fox")).isFalse();
    }

    @Test
    public void testRecycle()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("hello", 1);
        assertThat(queue.poll()).isEqualTo("hello");

        queue.add("hello", 2);
        assertThat(queue.poll()).isEqualTo("hello");
    }

    @Test
    public void testValues()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        assertThat(queue.values()).isEmpty();

        queue.add("hello", 1);
        queue.add("world", 2);

        assertThat(queue.values())
                .isEqualTo(ImmutableSet.of("hello", "world"));
    }

    @Test
    public void testNextPriority()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        assertThatThrownBy(queue::nextPriority)
                .isInstanceOf(IllegalStateException.class);

        queue.add("hello", 10);
        queue.add("world", 20);

        assertThat(queue.nextPriority()).isEqualTo(10);

        queue.poll();
        assertThat(queue.nextPriority()).isEqualTo(20);

        queue.poll();
        assertThatThrownBy(queue::nextPriority)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSamePriority()
    {
        PriorityQueue<String> queue = new PriorityQueue<>();

        queue.add("hello", 1);
        queue.add("world", 1);

        assertThat(queue.poll()).isEqualTo("hello");
        assertThat(queue.poll()).isEqualTo("world");
    }
}
