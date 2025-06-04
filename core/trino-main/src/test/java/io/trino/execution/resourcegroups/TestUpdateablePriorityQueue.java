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
import io.trino.execution.resourcegroups.IndexedPriorityQueue.Prioritized;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.HIGH_TO_LOW;
import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.LOW_TO_HIGH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUpdateablePriorityQueue
{
    @Test
    public void testFifoQueue()
    {
        assertThat(populateAndExtract(new FifoQueue<>())).isEqualTo(ImmutableList.of(1, 2, 3));
    }

    @Test
    public void testIndexedPriorityQueue()
    {
        assertThat(populateAndExtract(new IndexedPriorityQueue<>())).isEqualTo(ImmutableList.of(3, 2, 1));
        assertThat(populateAndExtract(new IndexedPriorityQueue<>(HIGH_TO_LOW))).isEqualTo(ImmutableList.of(3, 2, 1));
        assertThat(populateAndExtract(new IndexedPriorityQueue<>(LOW_TO_HIGH))).isEqualTo(ImmutableList.of(1, 2, 3));
    }

    @Test
    public void testPrioritizedPeekPollIndexedPriorityQueue()
    {
        IndexedPriorityQueue<Object> queue = new IndexedPriorityQueue<>();
        queue.addOrUpdate("a", 1);
        queue.addOrUpdate("b", 3);
        queue.addOrUpdate("c", 2);

        assertThat(queue.peekPrioritized()).isEqualTo(new Prioritized<>("b", 3));
        assertThat(queue.pollPrioritized()).isEqualTo(new Prioritized<>("b", 3));
        assertThat(queue.peekPrioritized()).isEqualTo(new Prioritized<>("c", 2));
        assertThat(queue.pollPrioritized()).isEqualTo(new Prioritized<>("c", 2));
        assertThat(queue.peekPrioritized()).isEqualTo(new Prioritized<>("a", 1));
        assertThat(queue.pollPrioritized()).isEqualTo(new Prioritized<>("a", 1));
        assertThat(queue.peekPrioritized()).isNull();
        assertThat(queue.pollPrioritized()).isNull();
    }

    @Test
    public void testStochasticPriorityQueue()
    {
        assertThat(populateAndExtract(new StochasticPriorityQueue<>()).size() == 3).isTrue();
    }

    private static List<Integer> populateAndExtract(UpdateablePriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        return ImmutableList.copyOf(queue);
    }
}
