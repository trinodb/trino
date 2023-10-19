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

import static org.assertj.core.api.Assertions.assertThat;

public class TestSchedulingQueue
{
    @Test
    public void testEmpty()
    {
        SchedulingQueue<String, Integer> queue = new SchedulingQueue<>();

        assertThat(queue.dequeue(1)).isNull();
    }

    @Test
    public void testSingleGroup()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");

        queue.enqueue("G1", "T1", 1);
        queue.enqueue("G1", "T2", 3);
        queue.enqueue("G1", "T3", 5);
        queue.enqueue("G1", "T4", 7);

        assertThat(queue.dequeue(1)).isEqualTo("T1");
        assertThat(queue.dequeue(1)).isEqualTo("T2");
        assertThat(queue.dequeue(1)).isEqualTo("T3");
        assertThat(queue.dequeue(1)).isEqualTo("T4");

        queue.enqueue("G1", "T1", 10);
        queue.enqueue("G1", "T2", 10);
        queue.enqueue("G1", "T3", 10);
        queue.enqueue("G1", "T4", 10);

        assertThat(queue.dequeue(1)).isEqualTo("T1");
        assertThat(queue.dequeue(1)).isEqualTo("T2");
        assertThat(queue.dequeue(1)).isEqualTo("T3");
        assertThat(queue.dequeue(1)).isEqualTo("T4");

        queue.enqueue("G1", "T1", 16);
        queue.enqueue("G1", "T2", 12);
        queue.enqueue("G1", "T3", 8);
        queue.enqueue("G1", "T4", 4);

        assertThat(queue.dequeue(1)).isEqualTo("T4");
        assertThat(queue.dequeue(1)).isEqualTo("T3");
        assertThat(queue.dequeue(1)).isEqualTo("T2");
        assertThat(queue.dequeue(1)).isEqualTo("T1");

        queue.finish("G1", "T1");
        queue.finish("G1", "T2");
        queue.finish("G1", "T3");
        queue.finish("G1", "T4");

        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testBasic()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.startGroup("G2");

        queue.enqueue("G1", "T1.0", 1);
        queue.enqueue("G1", "T1.1", 2);
        queue.enqueue("G2", "T2.0", 3);
        queue.enqueue("G2", "T2.1", 4);

        assertThat(queue.dequeue(1)).isEqualTo("T1.0");
        assertThat(queue.dequeue(1)).isEqualTo("T1.1");
        assertThat(queue.dequeue(1)).isEqualTo("T2.0");
        assertThat(queue.dequeue(1)).isEqualTo("T2.1");

        queue.enqueue("G1", "T1.0", 10);
        queue.enqueue("G1", "T1.1", 20);
        queue.enqueue("G2", "T2.0", 15);
        queue.enqueue("G2", "T2.1", 5);

        assertThat(queue.dequeue(1)).isEqualTo("T2.1");
        assertThat(queue.dequeue(1)).isEqualTo("T2.0");
        assertThat(queue.dequeue(1)).isEqualTo("T1.0");
        assertThat(queue.dequeue(1)).isEqualTo("T1.1");

        queue.enqueue("G1", "T1.0", 100);
        queue.enqueue("G2", "T2.0", 90);
        assertThat(queue.dequeue(1)).isEqualTo("T2.0");
        assertThat(queue.dequeue(1)).isEqualTo("T1.0");
    }

    @Test
    public void testSomeEmptyGroups()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.startGroup("G2");

        queue.enqueue("G2", "T1", 0);

        assertThat(queue.dequeue(1)).isEqualTo("T1");
    }

    @Test
    public void testDelayedCreation()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.startGroup("G2");

        queue.enqueue("G1", "T1.0", 100);
        queue.enqueue("G2", "T2.0", 200);

        queue.startGroup("G3"); // new group gets a priority baseline equal to the minimum current priority
        queue.enqueue("G3", "T3.0", 50);

        assertThat(queue.dequeue(1)).isEqualTo("T1.0");
        assertThat(queue.dequeue(1)).isEqualTo("T3.0");
        assertThat(queue.dequeue(1)).isEqualTo("T2.0");
    }

    @Test
    public void testDelayedCreationWhileAllRunning()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.startGroup("G2");

        queue.enqueue("G1", "T1.0", 0);

        queue.enqueue("G2", "T2.0", 100);
        queue.dequeue(50);
        queue.dequeue(50);

        queue.startGroup("G3"); // new group gets a priority baseline equal to the minimum current priority
        queue.enqueue("G3", "T3.0", 10);

        queue.enqueue("G1", "T1.0", 50);
        queue.enqueue("G2", "T2.0", 50);

        assertThat(queue.dequeue(1)).isEqualTo("T1.0");
        assertThat(queue.dequeue(1)).isEqualTo("T3.0");
        assertThat(queue.dequeue(1)).isEqualTo("T2.0");
    }

    @Test
    public void testGroupState()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        // initial state with no tasks
        queue.startGroup("G1");
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);

        // after adding a task, it should be runnable
        queue.enqueue("G1", "T1", 0);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);
        queue.enqueue("G1", "T2", 0);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);

        // after dequeueing, still runnable if there's at least one runnable task
        queue.dequeue(1);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);

        // after all tasks are dequeued, it should be running
        queue.dequeue(1);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNING);

        // still running while at least one task is running and there are no runnable tasks
        queue.block("G1", "T1", 1);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNING);

        // runnable after blocking when there are still runnable tasks
        queue.enqueue("G1", "T1", 1);
        queue.block("G1", "T2", 1);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);

        // blocked when all tasks are blocked
        queue.dequeue(1);
        queue.block("G1", "T1", 1);
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);

        // back to runnable after unblocking
        queue.enqueue("G1", "T1", 1);
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);
    }

    @Test
    public void testNonGreedyDeque()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.startGroup("G2");

        queue.enqueue("G1", "T1.0", 0);
        queue.enqueue("G2", "T2.0", 1);

        queue.enqueue("G1", "T1.1", 2);
        queue.enqueue("G1", "T1.2", 3);

        queue.enqueue("G2", "T2.1", 2);
        queue.enqueue("G2", "T2.2", 3);

        assertThat(queue.dequeue(2)).isEqualTo("T1.0");
        assertThat(queue.dequeue(2)).isEqualTo("T2.0");
        assertThat(queue.dequeue(2)).isEqualTo("T1.1");
        assertThat(queue.dequeue(2)).isEqualTo("T2.1");
        assertThat(queue.dequeue(2)).isEqualTo("T1.2");
        assertThat(queue.dequeue(2)).isEqualTo("T2.2");
        assertThat(queue.dequeue(2)).isNull();
    }

    @Test
    public void testFinishTask()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.enqueue("G1", "T1", 0);
        queue.enqueue("G1", "T2", 1);
        queue.enqueue("G1", "T3", 2);

        assertThat(queue.peek()).isEqualTo("T1");
        queue.finish("G1", "T1");
        assertThat(queue.peek()).isEqualTo("T2");
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);

        // check that the group becomes not-runnable
        queue.finish("G1", "T2");
        queue.finish("G1", "T3");
        assertThat(queue.peek()).isNull();
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);

        // check that the group becomes runnable again
        queue.enqueue("G1", "T4", 0);
        assertThat(queue.peek()).isEqualTo("T4");
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);
    }

    @Test
    public void testFinishTaskWhileRunning()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();
        queue.startGroup("G1");

        queue.enqueue("G1", "T1", 0);
        queue.enqueue("G1", "T2", 1);
        queue.enqueue("G1", "T3", 2);
        assertThat(queue.dequeue(0)).isEqualTo("T1");
        assertThat(queue.dequeue(0)).isEqualTo("T2");
        assertThat(queue.peek()).isEqualTo("T3");
        assertThat(queue.state("G1")).isEqualTo(State.RUNNABLE);

        queue.finish("G1", "T3");
        assertThat(queue.state("G1")).isEqualTo(State.RUNNING);

        queue.finish("G1", "T1");
        assertThat(queue.state("G1")).isEqualTo(State.RUNNING);

        queue.finish("G1", "T2");
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testFinishTaskWhileBlocked()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();
        queue.startGroup("G1");

        queue.enqueue("G1", "T1", 0);
        queue.enqueue("G1", "T2", 1);
        assertThat(queue.dequeue(0)).isEqualTo("T1");
        assertThat(queue.dequeue(0)).isEqualTo("T2");
        queue.block("G1", "T1", 0);
        queue.block("G1", "T2", 0);
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);

        queue.finish("G1", "T1");
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);

        queue.finish("G1", "T2");
        assertThat(queue.state("G1")).isEqualTo(State.BLOCKED);
    }

    @Test
    public void testFinishGroup()
    {
        SchedulingQueue<String, String> queue = new SchedulingQueue<>();

        queue.startGroup("G1");
        queue.enqueue("G1", "T1.1", 0);
        assertThat(queue.peek()).isEqualTo("T1.1");

        queue.startGroup("G2");
        queue.enqueue("G2", "T2.1", 1);
        assertThat(queue.peek()).isEqualTo("T1.1");

        queue.finishGroup("G1");
        assertThat(queue.containsGroup("G1")).isFalse();
        assertThat(queue.peek()).isEqualTo("T2.1");
    }
}
