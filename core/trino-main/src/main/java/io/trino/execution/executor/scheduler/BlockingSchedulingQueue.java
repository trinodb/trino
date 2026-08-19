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

import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
final class BlockingSchedulingQueue<G, T>
{
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    @GuardedBy("lock")
    private final SchedulingQueue<G, T> queue = new SchedulingQueue<>();

    /**
     * Lock-free mirror of {@link SchedulingQueue#getRunnableCount()}, republished by every
     * mutator while it holds the lock. Readers may observe a stale value, which is why
     * {@link #unblockToRunning} re-checks the precondition under the lock.
     */
    private volatile int runnableCount;

    public void startGroup(G group)
    {
        lock.lock();
        try {
            queue.startGroup(group);
            republishRunnableCount();
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishGroup(G group)
    {
        lock.lock();
        try {
            Set<T> tasks = queue.finishGroup(group);
            republishRunnableCount();
            return tasks;
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> getTasks(G group)
    {
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return ImmutableSet.of();
            }

            return queue.getTasks(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishAll()
    {
        lock.lock();
        try {
            Set<T> tasks = queue.finishAll();
            republishRunnableCount();
            return tasks;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean enqueue(G group, T task, long deltaWeight)
    {
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.enqueue(group, task, deltaWeight);
            republishRunnableCount();
            notEmpty.signal();

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
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.block(group, task, deltaWeight);
            republishRunnableCount();
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Moves a blocked task straight back to the running state, bypassing the queue and the
     * scheduler thread. Fails (returning false) if the task no longer qualifies for the
     * bypass, in which case the caller must fall back to {@link #enqueue}.
     *
     * <p>The caller is expected to gate this on {@link #getRunnableCount()} being zero and on
     * having already acquired a concurrency slot.</p>
     */
    public boolean unblockToRunning(G group, T task, long expectedWeight)
    {
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            boolean bypassed = queue.unblockToRunning(group, task, expectedWeight);
            republishRunnableCount();
            return bypassed;
        }
        finally {
            lock.unlock();
        }
    }

    public T dequeue(long expectedWeight)
            throws InterruptedException
    {
        lock.lock();
        try {
            T result;
            do {
                result = queue.dequeue(expectedWeight);
                if (result == null) {
                    notEmpty.await();
                }
            }
            while (result == null);

            republishRunnableCount();
            return result;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean finish(G group, T task)
    {
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.finish(group, task);
            republishRunnableCount();
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
            return queue.toString();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Approximate number of runnable tasks. Safe to read without holding the lock, but the
     * value may be stale by the time the caller acts on it.
     */
    public int getRunnableCount()
    {
        return runnableCount;
    }

    @GuardedBy("lock")
    private void republishRunnableCount()
    {
        runnableCount = queue.getRunnableCount();
    }
}
