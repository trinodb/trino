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
package io.trino.execution.resourcegroups;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.HIGH_TO_LOW;
import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.LOW_TO_HIGH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUpdateablePriorityQueue
{
    @Test
    public void testFifoQueue()
    {
        assertEquals(populateAndExtract(new FifoQueue<>()), ImmutableList.of(1, 2, 3));
    }

    @Test
    public void testIndexedPriorityQueue()
    {
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>()), ImmutableList.of(3, 2, 1));
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>(HIGH_TO_LOW)), ImmutableList.of(3, 2, 1));
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>(LOW_TO_HIGH)), ImmutableList.of(1, 2, 3));
    }

    @Test
    public void testPrioritizedPeekPollIndexedPriorityQueue()
    {
        IndexedPriorityQueue<Object> queue = new IndexedPriorityQueue<>();
        queue.addOrUpdate("a", 1);
        queue.addOrUpdate("b", 3);
        queue.addOrUpdate("c", 2);

        IndexedPriorityQueue.Prioritized<Object> peek1 = queue.peekPrioritized();
        assertThat(peek1.getValue()).isEqualTo("b");
        assertThat(peek1.getPriority()).isEqualTo(3);
        IndexedPriorityQueue.Prioritized<Object> poll1 = queue.pollPrioritized();
        assertThat(poll1.getValue()).isEqualTo("b");
        assertThat(poll1.getPriority()).isEqualTo(3);

        IndexedPriorityQueue.Prioritized<Object> peek2 = queue.peekPrioritized();
        assertThat(peek2.getValue()).isEqualTo("c");
        assertThat(peek2.getPriority()).isEqualTo(2);
        IndexedPriorityQueue.Prioritized<Object> poll2 = queue.pollPrioritized();
        assertThat(poll2.getValue()).isEqualTo("c");
        assertThat(poll2.getPriority()).isEqualTo(2);

        IndexedPriorityQueue.Prioritized<Object> peek3 = queue.peekPrioritized();
        assertThat(peek3.getValue()).isEqualTo("a");
        assertThat(peek3.getPriority()).isEqualTo(1);
        IndexedPriorityQueue.Prioritized<Object> poll3 = queue.pollPrioritized();
        assertThat(poll3.getValue()).isEqualTo("a");
        assertThat(poll3.getPriority()).isEqualTo(1);

        assertThat(queue.peekPrioritized()).isNull();
        assertThat(queue.pollPrioritized()).isNull();
    }

    @Test
    public void testStochasticPriorityQueue()
    {
        assertTrue(populateAndExtract(new StochasticPriorityQueue<>()).size() == 3);
    }

    private static List<Integer> populateAndExtract(UpdateablePriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        return ImmutableList.copyOf(queue);
    }
}
