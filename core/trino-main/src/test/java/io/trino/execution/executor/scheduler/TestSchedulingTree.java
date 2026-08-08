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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Exercises [SchedulingNode] at several levels of nesting, verifying that Completely-Fair-Scheduler
/// fairness composes across levels. [TestSchedulingNode] covers the flat two-level behaviour.
public class TestSchedulingTree
{
    private static final long QUANTUM = 100;

    @Test
    public void testSingleDeepChain()
    {
        // A single task under tenant -> query behaves exactly like a flat single group.
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(path("T"));
        tree.startGroup(path("T", "Q"));

        tree.enqueue(path("T", "Q", "a"), 1);
        tree.enqueue(path("T", "Q", "b"), 3);
        tree.enqueue(path("T", "Q", "c"), 5);

        assertThat(tree.dequeue(1)).isEqualTo("a");
        assertThat(tree.dequeue(1)).isEqualTo("b");
        assertThat(tree.dequeue(1)).isEqualTo("c");
    }

    @Test
    public void testStateBubblesUpThroughLevels()
    {
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(path("T"));
        tree.startGroup(path("T", "Q"));
        assertThat(tree.state(path("T"))).isEqualTo(State.BLOCKED);
        assertThat(tree.state(path("T", "Q"))).isEqualTo(State.BLOCKED);

        tree.enqueue(path("T", "Q", "a"), 0);
        assertThat(tree.state(path("T", "Q"))).isEqualTo(State.RUNNABLE);
        assertThat(tree.state(path("T"))).isEqualTo(State.RUNNABLE);

        // Dequeue the only task: the query and the tenant are now fully running.
        assertThat(tree.dequeue(QUANTUM)).isEqualTo("a");
        assertThat(tree.state(path("T", "Q"))).isEqualTo(State.RUNNING);
        assertThat(tree.state(path("T"))).isEqualTo(State.RUNNING);

        // Block the only task: blocked-ness bubbles all the way up.
        tree.block(path("T", "Q", "a"), QUANTUM);
        assertThat(tree.state(path("T", "Q"))).isEqualTo(State.BLOCKED);
        assertThat(tree.state(path("T"))).isEqualTo(State.BLOCKED);

        // Unblock: runnable again at every level.
        tree.enqueue(path("T", "Q", "a"), 0);
        assertThat(tree.state(path("T", "Q"))).isEqualTo(State.RUNNABLE);
        assertThat(tree.state(path("T"))).isEqualTo(State.RUNNABLE);
    }

    @Test
    public void testFinishSubtreeReturnsAllLeaves()
    {
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(path("T"));
        tree.startGroup(path("T", "Q1"));
        tree.startGroup(path("T", "Q2"));
        tree.enqueue(path("T", "Q1", "a"), 0);
        tree.enqueue(path("T", "Q1", "b"), 0);
        tree.enqueue(path("T", "Q2", "c"), 0);

        // Finishing the whole tenant returns every leaf beneath it.
        assertThat(tree.finishGroup(path("T"))).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(tree.containsGroup(path("T"))).isFalse();
        assertThat(tree.dequeue(QUANTUM)).isNull();
    }

    @Test
    public void testFairnessComposesAcrossLevels()
    {
        // Tenant A has two queries (one task each); tenant B has a single query with one task.
        // Hierarchical fairness splits CPU equally between tenants, so B's lone task should get
        // roughly twice the runtime of either of A's tasks.
        SchedulingNode<String> tree = new SchedulingNode<>();
        tree.startGroup(path("A"));
        tree.startGroup(path("A", "Q1"));
        tree.startGroup(path("A", "Q2"));
        tree.startGroup(path("B"));
        tree.startGroup(path("B", "Q1"));

        Map<String, List<Object>> paths = new HashMap<>();
        paths.put("a", path("A", "Q1", "a"));
        paths.put("b", path("A", "Q2", "b"));
        paths.put("c", path("B", "Q1", "c"));
        paths.forEach((_, p) -> tree.enqueue(p, 0));

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 6000; i++) {
            String task = tree.dequeue(QUANTUM);
            counts.merge(task, 1, Integer::sum);
            // Re-enqueue as if it consumed a full quantum (a busy, CPU-bound task).
            tree.enqueue(paths.get(task), QUANTUM);
        }

        int a = counts.get("a");
        int b = counts.get("b");
        int c = counts.get("c");

        // A's two tasks share tenant A's half equally.
        assertThat(a).isCloseTo(b, Percentage.withPercentage(10));
        // B's lone task gets tenant B's whole half — about twice either of A's tasks.
        assertThat(c).isCloseTo(a + b, Percentage.withPercentage(10));
    }

    private static List<Object> path(Object... elements)
    {
        return List.of(elements);
    }
}
