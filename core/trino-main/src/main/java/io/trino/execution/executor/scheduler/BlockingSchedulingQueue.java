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

    public void startGroup(G group)
    {
        lock.lock();
        try {
            queue.startGroup(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishGroup(G group)
    {
        lock.lock();
        try {
            return queue.finishGroup(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishAll()
    {
        lock.lock();
        try {
            return queue.finishAll();
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
            return true;
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

            return result;
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

    public int getRunnableCount()
    {
        lock.lock();
        try {
            return queue.getRunnableCount();
        }
        finally {
            lock.unlock();
        }
    }
}
