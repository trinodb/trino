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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// The priority-boost primitive donation is built on: a boosted node is pulled ahead of fair order
/// across nesting levels, and re-ranked or restored to fair order when the boost changes or clears.
public class TestPriorityDonation
{
    @Test
    public void testBoostPullsProducerToFrontOfGroup()
    {
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "a"), 1);
        queue.enqueue(List.of("G", "b"), 2);
        queue.enqueue(List.of("G", "p"), 50); // far back in fair order

        assertThat(queue.peek()).isEqualTo("a");

        queue.setBoost(List.of("G", "p"), 0);
        assertThat(queue.dequeue(1)).isEqualTo("p"); // jumps the queue

        // The rest resume fair order.
        assertThat(queue.dequeue(1)).isEqualTo("a");
        assertThat(queue.dequeue(1)).isEqualTo("b");
    }

    @Test
    public void testClearBoostRestoresFairOrder()
    {
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "a"), 1);
        queue.enqueue(List.of("G", "p"), 50);

        queue.setBoost(List.of("G", "p"), 0);
        assertThat(queue.peek()).isEqualTo("p");

        queue.clearBoost(List.of("G", "p"));
        assertThat(queue.peek()).isEqualTo("a"); // back to fair order
    }

    @Test
    public void testBoostCrossesGroupBoundaries()
    {
        // A producer in a heavier group would normally lose to a lighter group; boosting flips that.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("Light"));
        queue.startGroup(List.of("Heavy"));
        queue.enqueue(List.of("Light", "x"), 1);
        queue.enqueue(List.of("Heavy", "p"), 100);

        assertThat(queue.peek()).isEqualTo("x");

        queue.setBoost(List.of("Heavy", "p"), 0);
        assertThat(queue.dequeue(1)).isEqualTo("p");
    }

    @Test
    public void testMoreUrgentDonorWinsAmongBoosted()
    {
        // Two producers, both boosted. Fair order would take p1 (lighter) first, but p2's donor is
        // more urgent (lower rank), so p2 wins among the boosted pair.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "p1"), 10);
        queue.enqueue(List.of("G", "p2"), 20);

        assertThat(queue.peek()).isEqualTo("p1"); // fair order

        queue.setBoost(List.of("G", "p1"), 100); // boosted, but by a less urgent consumer
        queue.setBoost(List.of("G", "p2"), 50); // boosted by a more urgent consumer

        assertThat(queue.dequeue(1)).isEqualTo("p2");
        assertThat(queue.dequeue(1)).isEqualTo("p1");
    }

    @Test
    public void testBoostDoesNotStarveRunnableSibling()
    {
        // Priority inheritance, not an absolute band: a boosted producer starts at the donor's
        // virtual runtime but accrues from there, so a sibling that is already runnable is scheduled
        // within a bounded number of rounds instead of being starved forever. This is what keeps
        // donation from livelocking the work a blocked consumer ultimately depends on.
        SchedulingNode<String> queue = new SchedulingNode<>();
        queue.startGroup(List.of("G"));
        queue.enqueue(List.of("G", "p"), 0); // producer, fresh (low virtual runtime)
        queue.enqueue(List.of("G", "s"), 5); // sibling, already a little ahead in accumulated work

        queue.setBoost(List.of("G", "p"), 0); // donor at rank 0: p sorts first...
        assertThat(queue.peek()).isEqualTo("p");

        // ...but as p runs it accrues runtime and must yield to s rather than monopolizing every round.
        boolean sRan = false;
        for (int i = 0; i < 50 && !sRan; i++) {
            String task = queue.dequeue(10);
            queue.enqueue(List.of("G", task), 10); // busy task: consumes its whole quantum and returns
            sRan = task.equals("s");
        }
        assertThat(sRan).isTrue();
    }
}
