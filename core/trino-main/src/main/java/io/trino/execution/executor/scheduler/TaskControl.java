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

import com.google.common.base.Ticker;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * Equality is based on group and id for the purpose of adding to the scheduling queue.
 */
@ThreadSafe
final class TaskControl
{
    private final Group group;
    private final int id;
    private final Ticker ticker;

    private final Lock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final Condition wakeup = lock.newCondition();

    @GuardedBy("lock")
    private boolean ready;

    @GuardedBy("lock")
    private boolean blocked;

    @GuardedBy("lock")
    private boolean cancelled;

    @GuardedBy("lock")
    private State state;

    private volatile long periodStart;
    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong scheduledNanos = new AtomicLong();
    private final AtomicLong blockedNanos = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();
    private volatile Thread thread;

    public TaskControl(Group group, int id, Ticker ticker)
    {
        this.group = requireNonNull(group, "group is null");
        this.id = id;
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.state = State.NEW;
        this.ready = false;
        this.periodStart = ticker.read();
    }

    public void setThread(Thread thread)
    {
        this.thread = thread;
    }

    public void cancel()
    {
        lock.lock();
        try {
            cancelled = true;
            wakeup.signal();

            // TODO: it should be possible to interrupt the thread, but
            //       it appears that it's not safe to do so. It can cause the query
            //       to get stuck (e.g., AbstractDistributedEngineOnlyQueries.testSelectiveLimit)
            //
            //       Thread thread = this.thread;
            //       if (thread != null) {
            //           thread.interrupt();
            //       }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Called by the scheduler thread when the task is ready to run. It
     * causes anyone blocking in {@link #awaitReady()} to wake up.
     *
     * @return false if the task was already cancelled
     */
    public boolean markReady()
    {
        lock.lock();
        try {
            if (cancelled) {
                return false;
            }
            ready = true;
            wakeup.signal();
        }
        finally {
            lock.unlock();
        }

        return true;
    }

    public void markNotReady()
    {
        lock.lock();
        try {
            ready = false;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isReady()
    {
        lock.lock();
        try {
            return ready;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return false if the operation was interrupted due to cancellation
     */
    public boolean awaitReady()
    {
        lock.lock();
        try {
            while (!ready && !cancelled) {
                try {
                    wakeup.await();
                }
                catch (InterruptedException e) {
                }
            }

            return !cancelled;
        }
        finally {
            lock.unlock();
        }
    }

    public void markUnblocked()
    {
        lock.lock();
        try {
            blocked = false;
            wakeup.signal();
        }
        finally {
            lock.unlock();
        }
    }

    public void markBlocked()
    {
        lock.lock();
        try {
            blocked = true;
        }
        finally {
            lock.unlock();
        }
    }

    public void awaitUnblock()
    {
        lock.lock();
        try {
            while (blocked && !cancelled) {
                try {
                    wakeup.await();
                }
                catch (InterruptedException e) {
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return false if the transition was unsuccessful due to the task being interrupted
     */
    public boolean transitionToBlocked()
    {
        boolean success = transitionTo(State.BLOCKED);

        if (success) {
            markBlocked();
        }

        return success;
    }

    public void transitionToFinished()
    {
        transitionTo(State.FINISHED);
    }

    /**
     * @return false if the transition was unsuccessful due to the task being interrupted
     */
    public boolean transitionToWaiting()
    {
        boolean success = transitionTo(State.WAITING);

        if (success) {
            markNotReady();
        }

        return success;
    }

    /**
     * @return false if the transition was unsuccessful due to the task being interrupted
     */
    public boolean transitionToRunning()
    {
        return transitionTo(State.RUNNING);
    }

    private boolean transitionTo(State state)
    {
        lock.lock();
        try {
            recordPeriodEnd(this.state);

            if (cancelled) {
                this.state = State.INTERRUPTED;
                return false;
            }
            else {
                this.state = state;
                return true;
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void recordPeriodEnd(State state)
    {
        long now = ticker.read();
        long elapsed = now - periodStart;
        switch (state) {
            case RUNNING -> scheduledNanos.addAndGet(elapsed);
            case BLOCKED -> blockedNanos.addAndGet(elapsed);
            case NEW -> startNanos.addAndGet(elapsed);
            case WAITING -> waitNanos.addAndGet(elapsed);
            case INTERRUPTED, FINISHED -> {}
        }
        periodStart = now;
    }

    public Group group()
    {
        return group;
    }

    public State getState()
    {
        lock.lock();
        try {
            return state;
        }
        finally {
            lock.unlock();
        }
    }

    public long elapsed()
    {
        return ticker.read() - periodStart;
    }

    public long getStartNanos()
    {
        return startNanos.get();
    }

    public long getWaitNanos()
    {
        return waitNanos.get();
    }

    public long getScheduledNanos()
    {
        return scheduledNanos.get();
    }

    public long getBlockedNanos()
    {
        return blockedNanos.get();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskControl that = (TaskControl) o;
        return id == that.id && group.equals(that.group);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(group, id);
    }

    @Override
    public String toString()
    {
        lock.lock();
        try {
            return group.name() + "-" + id + " [" + state + "]";
        }
        finally {
            lock.unlock();
        }
    }

    public Thread getThread()
    {
        return thread;
    }

    public enum State
    {
        NEW,
        WAITING,
        RUNNING,
        BLOCKED,
        INTERRUPTED,
        FINISHED
    }
}
