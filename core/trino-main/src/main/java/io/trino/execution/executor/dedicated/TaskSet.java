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
package io.trino.execution.executor.dedicated;

import com.google.common.collect.Iterables;
import io.trino.execution.TaskId;
import io.trino.execution.executor.ExecutionPriority;
import io.trino.execution.executor.ExecutionPriorityManager;

import java.io.Closeable;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

class TaskSet
        implements Closeable
{
    private final Map<TaskId, TaskEntry> normalPriorityTasks = new HashMap<>();
    private final Map<TaskId, TaskEntry> lowPriorityTasks = new HashMap<>();
    private final int normalToLowPriorityResourceRatio;
    private final Deque<ExecutionPriority> recentPriorities;
    private int recentLowPriorityTasks;

    public TaskSet(ExecutionPriorityManager executionPriorityManager)
    {
        normalToLowPriorityResourceRatio = (int) executionPriorityManager.getNormalToLowPriorityResourceRatio();
        recentPriorities = new ArrayDeque<>(normalToLowPriorityResourceRatio + 1);
    }

    @Override
    public void close()
    {
        lowPriorityTasks.values().forEach(TaskEntry::destroy);
        normalPriorityTasks.values().forEach(TaskEntry::destroy);
    }

    public void remove(TaskEntry task)
    {
        if (task.priority().isLow()) {
            lowPriorityTasks.remove(task.taskId());
        }
        else {
            normalPriorityTasks.remove(task.taskId());
        }
    }

    public void add(TaskEntry task)
    {
        (task.priority().isLow() ? lowPriorityTasks : normalPriorityTasks).put(task.taskId(), task);
    }

    public Iterable<TaskEntry> values()
    {
        return Iterables.concat(normalPriorityTasks.values(), lowPriorityTasks.values());
    }

    public Queue<TaskEntry> newQueue()
    {
        return new TaskQueue(new ArrayDeque<>(normalPriorityTasks.values()), new ArrayDeque<>(lowPriorityTasks.values()));
    }

    public int size()
    {
        return normalPriorityTasks.size() + lowPriorityTasks.size();
    }

    private class TaskQueue
            extends AbstractQueue<TaskEntry>
    {
        private final Queue<TaskEntry> normalPriority;
        private final Queue<TaskEntry> lowPriority;

        private TaskQueue(Queue<TaskEntry> normalPriority, Queue<TaskEntry> lowPriority)
        {
            this.normalPriority = requireNonNull(normalPriority, "normalPriority is null");
            this.lowPriority = requireNonNull(lowPriority, "lowPriority is null");
        }

        @Override
        public int size()
        {
            return normalPriority.size() + lowPriority.size();
        }

        @Override
        public boolean offer(TaskEntry taskEntry)
        {
            return (taskEntry.priority().isLow() ? lowPriority : normalPriority).offer(taskEntry);
        }

        @Override
        public TaskEntry poll()
        {
            TaskEntry task = null;
            while (task == null) {
                if (recentLowPriorityTasks > 0) {
                    task = normalPriority.poll();
                }
                if (task == null) {
                    task = lowPriority.poll();
                    if (task != null) {
                        recentLowPriorityTasks++;
                    }
                    else {
                        // try normal priority unconditionally
                        task = normalPriority.poll();
                    }
                }
                if (task != null) {
                    recentPriorities.addFirst(task.priority());
                }
            }
            if (recentPriorities.size() > normalToLowPriorityResourceRatio) {
                ExecutionPriority removed = recentPriorities.removeLast();
                if (removed.isLow()) {
                    recentLowPriorityTasks--;
                }
            }
            return task;
        }

        @Override
        public Iterator<TaskEntry> iterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TaskEntry peek()
        {
            throw new UnsupportedOperationException();
        }
    }
}
