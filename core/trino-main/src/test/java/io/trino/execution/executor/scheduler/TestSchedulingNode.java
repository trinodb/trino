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

/// Exercises the flat two-level `(group, task)` scheduling behaviour of [SchedulingNode] — the
/// shape the thread-per-driver executor uses. [TestSchedulingTree] covers deeper nesting.
public class TestSchedulingNode
{
    @Test
    public void testEmpty()
    {
        SchedulingNode<Integer> node = new SchedulingNode<>();

        assertThat(node.dequeue(1)).isNull();
    }

    @Test
    public void testSingleGroup()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));

        node.enqueue(List.of("G1", "T1"), 1);
        node.enqueue(List.of("G1", "T2"), 3);
        node.enqueue(List.of("G1", "T3"), 5);
        node.enqueue(List.of("G1", "T4"), 7);

        assertThat(node.dequeue(1)).isEqualTo("T1");
        assertThat(node.dequeue(1)).isEqualTo("T2");
        assertThat(node.dequeue(1)).isEqualTo("T3");
        assertThat(node.dequeue(1)).isEqualTo("T4");

        node.enqueue(List.of("G1", "T1"), 10);
        node.enqueue(List.of("G1", "T2"), 10);
        node.enqueue(List.of("G1", "T3"), 10);
        node.enqueue(List.of("G1", "T4"), 10);

        assertThat(node.dequeue(1)).isEqualTo("T1");
        assertThat(node.dequeue(1)).isEqualTo("T2");
        assertThat(node.dequeue(1)).isEqualTo("T3");
        assertThat(node.dequeue(1)).isEqualTo("T4");

        node.enqueue(List.of("G1", "T1"), 16);
        node.enqueue(List.of("G1", "T2"), 12);
        node.enqueue(List.of("G1", "T3"), 8);
        node.enqueue(List.of("G1", "T4"), 4);

        assertThat(node.dequeue(1)).isEqualTo("T4");
        assertThat(node.dequeue(1)).isEqualTo("T3");
        assertThat(node.dequeue(1)).isEqualTo("T2");
        assertThat(node.dequeue(1)).isEqualTo("T1");

        node.finish(List.of("G1", "T1"));
        node.finish(List.of("G1", "T2"));
        node.finish(List.of("G1", "T3"));
        node.finish(List.of("G1", "T4"));

        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testBasic()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.startGroup(List.of("G2"));

        node.enqueue(List.of("G1", "T1.0"), 1);
        node.enqueue(List.of("G1", "T1.1"), 2);
        node.enqueue(List.of("G2", "T2.0"), 3);
        node.enqueue(List.of("G2", "T2.1"), 4);

        assertThat(node.dequeue(1)).isEqualTo("T1.0");
        assertThat(node.dequeue(1)).isEqualTo("T1.1");
        assertThat(node.dequeue(1)).isEqualTo("T2.0");
        assertThat(node.dequeue(1)).isEqualTo("T2.1");

        node.enqueue(List.of("G1", "T1.0"), 10);
        node.enqueue(List.of("G1", "T1.1"), 20);
        node.enqueue(List.of("G2", "T2.0"), 15);
        node.enqueue(List.of("G2", "T2.1"), 5);

        assertThat(node.dequeue(1)).isEqualTo("T2.1");
        assertThat(node.dequeue(1)).isEqualTo("T2.0");
        assertThat(node.dequeue(1)).isEqualTo("T1.0");
        assertThat(node.dequeue(1)).isEqualTo("T1.1");

        node.enqueue(List.of("G1", "T1.0"), 100);
        node.enqueue(List.of("G2", "T2.0"), 90);
        assertThat(node.dequeue(1)).isEqualTo("T2.0");
        assertThat(node.dequeue(1)).isEqualTo("T1.0");
    }

    @Test
    public void testSomeEmptyGroups()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.startGroup(List.of("G2"));

        node.enqueue(List.of("G2", "T1"), 0);

        assertThat(node.dequeue(1)).isEqualTo("T1");
    }

    @Test
    public void testDelayedCreation()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.startGroup(List.of("G2"));

        node.enqueue(List.of("G1", "T1.0"), 100);
        node.enqueue(List.of("G2", "T2.0"), 200);

        node.startGroup(List.of("G3")); // new group gets a priority baseline equal to the minimum current priority
        node.enqueue(List.of("G3", "T3.0"), 50);

        assertThat(node.dequeue(1)).isEqualTo("T1.0");
        assertThat(node.dequeue(1)).isEqualTo("T3.0");
        assertThat(node.dequeue(1)).isEqualTo("T2.0");
    }

    @Test
    public void testDelayedCreationWhileAllRunning()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.startGroup(List.of("G2"));

        node.enqueue(List.of("G1", "T1.0"), 0);

        node.enqueue(List.of("G2", "T2.0"), 100);
        node.dequeue(50);
        node.dequeue(50);

        node.startGroup(List.of("G3")); // new group gets a priority baseline equal to the minimum current priority
        node.enqueue(List.of("G3", "T3.0"), 10);

        node.enqueue(List.of("G1", "T1.0"), 50);
        node.enqueue(List.of("G2", "T2.0"), 50);

        assertThat(node.dequeue(1)).isEqualTo("T1.0");
        assertThat(node.dequeue(1)).isEqualTo("T3.0");
        assertThat(node.dequeue(1)).isEqualTo("T2.0");
    }

    @Test
    public void testGroupState()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        // initial state with no tasks
        node.startGroup(List.of("G1"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);

        // after adding a task, it should be runnable
        node.enqueue(List.of("G1", "T1"), 0);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);
        node.enqueue(List.of("G1", "T2"), 0);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);

        // after dequeueing, still runnable if there's at least one runnable task
        node.dequeue(1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);

        // after all tasks are dequeued, it should be running
        node.dequeue(1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNING);

        // still running while at least one task is running and there are no runnable tasks
        node.block(List.of("G1", "T1"), 1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNING);

        // runnable after blocking when there are still runnable tasks
        node.enqueue(List.of("G1", "T1"), 1);
        node.block(List.of("G1", "T2"), 1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);

        // blocked when all tasks are blocked
        node.dequeue(1);
        node.block(List.of("G1", "T1"), 1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);

        // back to runnable after unblocking
        node.enqueue(List.of("G1", "T1"), 1);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);
    }

    @Test
    public void testNonGreedyDeque()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.startGroup(List.of("G2"));

        node.enqueue(List.of("G1", "T1.0"), 0);
        node.enqueue(List.of("G2", "T2.0"), 1);

        node.enqueue(List.of("G1", "T1.1"), 2);
        node.enqueue(List.of("G1", "T1.2"), 3);

        node.enqueue(List.of("G2", "T2.1"), 2);
        node.enqueue(List.of("G2", "T2.2"), 3);

        assertThat(node.dequeue(2)).isEqualTo("T1.0");
        assertThat(node.dequeue(2)).isEqualTo("T2.0");
        assertThat(node.dequeue(2)).isEqualTo("T1.1");
        assertThat(node.dequeue(2)).isEqualTo("T2.1");
        assertThat(node.dequeue(2)).isEqualTo("T1.2");
        assertThat(node.dequeue(2)).isEqualTo("T2.2");
        assertThat(node.dequeue(2)).isNull();
    }

    @Test
    public void testFinishTask()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.enqueue(List.of("G1", "T1"), 0);
        node.enqueue(List.of("G1", "T2"), 1);
        node.enqueue(List.of("G1", "T3"), 2);

        assertThat(node.peek()).isEqualTo("T1");
        node.finish(List.of("G1", "T1"));
        assertThat(node.peek()).isEqualTo("T2");
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);

        // check that the group becomes not-runnable
        node.finish(List.of("G1", "T2"));
        node.finish(List.of("G1", "T3"));
        assertThat(node.peek()).isNull();
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);

        // check that the group becomes runnable again
        node.enqueue(List.of("G1", "T4"), 0);
        assertThat(node.peek()).isEqualTo("T4");
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);
    }

    @Test
    public void testFinishTaskWhileRunning()
    {
        SchedulingNode<String> node = new SchedulingNode<>();
        node.startGroup(List.of("G1"));

        node.enqueue(List.of("G1", "T1"), 0);
        node.enqueue(List.of("G1", "T2"), 1);
        node.enqueue(List.of("G1", "T3"), 2);
        assertThat(node.dequeue(0)).isEqualTo("T1");
        assertThat(node.dequeue(0)).isEqualTo("T2");
        assertThat(node.peek()).isEqualTo("T3");
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNABLE);

        node.finish(List.of("G1", "T3"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNING);

        node.finish(List.of("G1", "T1"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.RUNNING);

        node.finish(List.of("G1", "T2"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testFinishTaskWhileBlocked()
    {
        SchedulingNode<String> node = new SchedulingNode<>();
        node.startGroup(List.of("G1"));

        node.enqueue(List.of("G1", "T1"), 0);
        node.enqueue(List.of("G1", "T2"), 1);
        assertThat(node.dequeue(0)).isEqualTo("T1");
        assertThat(node.dequeue(0)).isEqualTo("T2");
        node.block(List.of("G1", "T1"), 0);
        node.block(List.of("G1", "T2"), 0);
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);

        node.finish(List.of("G1", "T1"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);

        node.finish(List.of("G1", "T2"));
        assertThat(node.state(List.of("G1"))).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testFinishGroup()
    {
        SchedulingNode<String> node = new SchedulingNode<>();

        node.startGroup(List.of("G1"));
        node.enqueue(List.of("G1", "T1.1"), 0);
        assertThat(node.peek()).isEqualTo("T1.1");

        node.startGroup(List.of("G2"));
        node.enqueue(List.of("G2", "T2.1"), 1);
        assertThat(node.peek()).isEqualTo("T1.1");

        node.finishGroup(List.of("G1"));
        assertThat(node.containsGroup(List.of("G1"))).isFalse();
        assertThat(node.peek()).isEqualTo("T2.1");
    }
}
