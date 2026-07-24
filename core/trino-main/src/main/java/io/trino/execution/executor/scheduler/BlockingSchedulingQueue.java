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

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/// Thread-safe wrapper around the (not thread-safe) [SchedulingNode] tree, exposing the flat
/// `(group, task)` API the scheduler uses: a group is a depth-1 node and a task a leaf beneath it.
@ThreadSafe
final class BlockingSchedulingQueue<G, T>
{
    private final Lock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final SchedulingNode<T> root = new SchedulingNode<>();

    public void startGroup(G group)
    {
        lock.lock();
        try {
            root.startGroup(List.of(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishGroup(G group)
    {
        lock.lock();
        try {
            return root.finishGroup(List.of(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> getTasks(G group)
    {
        lock.lock();
        try {
            if (!root.containsGroup(List.of(group))) {
                return ImmutableSet.of();
            }

            return root.getTasks(List.of(group));
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishAll()
    {
        lock.lock();
        try {
            return root.finishAll();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean enqueue(G group, T task, long deltaWeight)
    {
        lock.lock();
        try {
            if (!root.containsGroup(List.of(group))) {
                return false;
            }

            root.enqueue(List.of(group, task), deltaWeight);

            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean block(G group, T task, long deltaWeight)
    {
        lock.lock();
        try {
            if (!root.containsGroup(List.of(group))) {
                return false;
            }

            root.block(List.of(group, task), deltaWeight);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    /// Dequeue the next runnable task, or return `null` if none is currently runnable. Unlike a
    /// blocking queue, this never waits — admission is driven by the scheduler calling this whenever
    /// a slot frees or work is enqueued.
    public T tryDequeue(long expectedWeight)
    {
        lock.lock();
        try {
            return root.dequeue(expectedWeight);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean finish(G group, T task)
    {
        lock.lock();
        try {
            if (!root.containsGroup(List.of(group))) {
                return false;
            }

            root.finish(List.of(group, task));
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
}
