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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/// Thread-safe wrapper around the (not thread-safe) [SchedulingNode] tree. Groups nest to any depth:
/// a group's position in the tree is its [Group#path()], and a task is a leaf below its innermost
/// group.
@ThreadSafe
final class BlockingSchedulingQueue
{
    private final Lock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final SchedulingNode<TaskControl> root = new SchedulingNode<>();

    public void startGroup(Group group)
    {
        lock.lock();
        try {
            root.startGroup(groupPath(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<TaskControl> finishGroup(Group group)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return ImmutableSet.of();
            }
            return root.finishGroup(groupPath(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<TaskControl> getTasks(Group group)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return ImmutableSet.of();
            }
            return root.getTasks(groupPath(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<TaskControl> finishAll()
    {
        lock.lock();
        try {
            return root.finishAll();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean enqueue(Group group, TaskControl task, long deltaWeight)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return false;
            }
            root.enqueue(leafPath(group, task), deltaWeight);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean block(Group group, TaskControl task, long deltaWeight)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return false;
            }
            root.block(leafPath(group, task), deltaWeight);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    /// Dequeue the next runnable task, or return `null` if none is currently runnable. Unlike a
    /// blocking queue, this never waits — admission is driven by the scheduler calling this whenever
    /// a slot frees or work is enqueued.
    public TaskControl tryDequeue(long expectedWeight)
    {
        lock.lock();
        try {
            return root.dequeue(expectedWeight);
        }
        finally {
            lock.unlock();
        }
    }

    public long weightOf(Group group, TaskControl task)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                // The group may have been removed (and the task cancelled) concurrently.
                return 0;
            }
            return root.weightOf(leafPath(group, task));
        }
        finally {
            lock.unlock();
        }
    }

    /// The group's own virtual runtime — how far fair order has deferred it. A missing group (removed
    /// concurrently, or a producer that never scheduled a split) sorts last.
    public long groupWeightOf(Group group)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return Long.MIN_VALUE;
            }
            return root.weightOf(groupPath(group));
        }
        finally {
            lock.unlock();
        }
    }

    public void boost(Group group, long rank)
    {
        lock.lock();
        try {
            if (root.containsGroup(groupPath(group))) {
                root.setBoost(groupPath(group), rank);
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void unboost(Group group)
    {
        lock.lock();
        try {
            if (root.containsGroup(groupPath(group))) {
                root.clearBoost(groupPath(group));
            }
        }
        finally {
            lock.unlock();
        }
    }

    public boolean finish(Group group, TaskControl task)
    {
        lock.lock();
        try {
            if (!root.containsGroup(groupPath(group))) {
                return false;
            }
            root.finish(leafPath(group, task));
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public String toString()
    {
        lock.lock();
        try {
            return root.toString();
        }
        finally {
            lock.unlock();
        }
    }

    public int getRunnableCount()
    {
        lock.lock();
        try {
            return root.getRunnableCount();
        }
        finally {
            lock.unlock();
        }
    }

    private static List<Object> groupPath(Group group)
    {
        return new ArrayList<>(group.path());
    }

    private static List<Object> leafPath(Group group, TaskControl task)
    {
        List<Object> path = groupPath(group);
        path.add(task);
        return path;
    }
}
