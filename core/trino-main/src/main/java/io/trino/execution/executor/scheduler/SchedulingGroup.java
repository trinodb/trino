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
import io.trino.annotation.NotThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.execution.executor.scheduler.State.BLOCKED;
import static io.trino.execution.executor.scheduler.State.RUNNABLE;
import static io.trino.execution.executor.scheduler.State.RUNNING;

@NotThreadSafe
final class SchedulingGroup<T>
{
    private State state;
    private long weight;
    private final Map<T, Task> tasks = new HashMap<>();
    private final PriorityQueue<T> runnableQueue = new PriorityQueue<>();
    private final Set<T> blocked = new HashSet<>();
    private final PriorityQueue<T> baselineWeights = new PriorityQueue<>();

    public SchedulingGroup()
    {
        this.state = BLOCKED;
    }

    public void enqueue(T handle, long deltaWeight)
    {
        Task task = tasks.get(handle);

        if (task == null) {
            // New tasks get assigned the baseline weight so that they don't monopolize the queue
            // while they catch up
            task = new Task(baselineWeight());
            tasks.put(handle, task);
        }
        else if (task.state() == BLOCKED) {
            blocked.remove(handle);
            task.addWeight(baselineWeight());
        }

        weight -= task.uncommittedWeight();
        weight += deltaWeight;

        task.commitWeight(deltaWeight);
        task.setState(RUNNABLE);
        runnableQueue.add(handle, task.weight());
        baselineWeights.addOrReplace(handle, task.weight());

        updateState();
    }

    public T dequeue(long expectedWeight)
    {
        checkArgument(state == RUNNABLE);

        T task = runnableQueue.takeOrThrow();

        Task info = tasks.get(task);
        info.setUncommittedWeight(expectedWeight);
        info.setState(RUNNING);
        weight += expectedWeight;

        baselineWeights.addOrReplace(task, info.weight());

        updateState();

        return task;
    }

    /**
     * Whether {@link #unblockToRunning(Object, long)} can be applied to the given task.
     */
    public boolean canUnblockToRunning(T handle)
    {
        return runnableQueue.isEmpty() && blocked.contains(handle);
    }

    /**
     * Moves a blocked task directly to the running state without passing through the
     * runnable queue. Only legal when {@link #canUnblockToRunning(Object)} holds, i.e. when
     * the group has no other runnable task that would have been picked ahead of this one.
     * Under that precondition this is exactly {@link #enqueue(Object, long)} followed by
     * {@link #dequeue(long)}, so weight accounting is identical to the regular path.
     */
    public void unblockToRunning(T handle, long expectedWeight)
    {
        checkArgument(canUnblockToRunning(handle), "Task cannot be unblocked directly to running: %s", handle);

        enqueue(handle, 0);
        T dequeued = dequeue(expectedWeight);
        verify(dequeued == handle, "Dequeued unexpected task: %s", dequeued);
    }

    public void finish(T task)
    {
        checkArgument(tasks.containsKey(task), "Unknown task: %s", task);
        tasks.remove(task);
        blocked.remove(task);
        runnableQueue.removeIfPresent(task);
        baselineWeights.removeIfPresent(task);

        updateState();
    }

    public void block(T handle, long deltaWeight)
    {
        checkArgument(tasks.containsKey(handle), "Unknown task: %s", handle);
        checkArgument(!runnableQueue.contains(handle), "Task is already in queue: %s", handle);

        weight += deltaWeight;

        Task task = tasks.get(handle);
        task.commitWeight(deltaWeight);
        task.setState(BLOCKED);
        task.addWeight(-baselineWeight());
        blocked.add(handle);
        baselineWeights.remove(handle);

        updateState();
    }

    public long baselineWeight()
    {
        if (baselineWeights.isEmpty()) {
            return 0;
        }

        return baselineWeights.nextPriority();
    }

    public void addWeight(long delta)
    {
        weight += delta;
    }

    private void updateState()
    {
        if (blocked.size() == tasks.size()) {
            state = BLOCKED;
        }
        else if (runnableQueue.isEmpty()) {
            state = RUNNING;
        }
        else {
            state = RUNNABLE;
        }
    }

    public long weight()
    {
        return weight;
    }

    public Set<T> tasks()
    {
        return ImmutableSet.copyOf(tasks.keySet());
    }

    public State state()
    {
        return state;
    }

    public T peek()
    {
        return runnableQueue.peek();
    }

    public int runnableCount()
    {
        return runnableQueue.size();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for (Entry<T, Task> entry : tasks.entrySet()) {
            T key = entry.getKey();
            Task task = entry.getValue();

            String prefix = "%s %s".formatted(
                    key == peek() ? "=>" : "  ",
                    key);

            String details = switch (task.state()) {
                case BLOCKED -> "[BLOCKED, saved delta = %s]".formatted(task.weight());
                case RUNNABLE -> "[RUNNABLE, weight = %s]".formatted(task.weight());
                case RUNNING -> "[RUNNING, weight = %s, uncommitted = %s]".formatted(task.weight(), task.uncommittedWeight());
            };

            builder.append(prefix)
                    .append(" ")
                    .append(details)
                    .append("\n");
        }

        return builder.toString();
    }
}
