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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;

import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

/// Implementation notes:
///
///   - The [TaskControl] state machine is only modified by the task executor thread (i.e. from
///     within [#runTask]). Other threads can indirectly affect what the task executor thread does by
///     marking the task as ready or cancelled and unblocking it, which will then act on that
///     information.
///   - Admission is event-driven: whenever a slot frees (a task yields, blocks or finishes) or
///     runnable work appears (a task is submitted or unblocked), the acting thread calls
///     [#schedule()], which fills every free slot by dequeuing the next runnable task and waking its
///     thread directly. [#schedule()] is serialized by [#scheduleLock] so that concurrent callers
///     cannot lose a wakeup.
@ThreadSafe
public final class FairScheduler
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(FairScheduler.class);

    public static final long QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(1000);

    private final ListeningExecutorService taskExecutor;
    private final ThreadPoolExecutor executor; // instance underlying taskExecutor, for diagnostics
    private final ThreadPoolExecutorMBean executorMBean;
    private final BlockingSchedulingQueue<Group, TaskControl> queue = new BlockingSchedulingQueue<>();
    private final Reservation<TaskControl> concurrencyControl;
    private final Ticker ticker;

    // Serializes admission so that a concurrent enqueue cannot race a "reserved a slot but found
    // the queue momentarily empty" caller into dropping a runnable task on the floor.
    private final ReentrantLock scheduleLock = new ReentrantLock();

    private final Gate paused = new Gate(true);

    // Read from schedule() under scheduleLock as well as under the monitor, so it is volatile rather
    // than @GuardedBy("this"). close() is authoritative: it cancels every task, so a task that
    // schedule() marks ready after a stale read simply fails markReady and releases its slot.
    private volatile boolean closed;

    public FairScheduler(int maxConcurrentTasks, String threadNameFormat, Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");

        concurrencyControl = new Reservation<>(maxConcurrentTasks);

        executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), daemonThreadsNamed(threadNameFormat));
        executorMBean = new ThreadPoolExecutorMBean(executor);
        taskExecutor = MoreExecutors.listeningDecorator(executor);
    }

    public static FairScheduler newInstance(int maxConcurrentTasks)
    {
        return newInstance(maxConcurrentTasks, Ticker.systemTicker());
    }

    public static FairScheduler newInstance(int maxConcurrentTasks, Ticker ticker)
    {
        FairScheduler scheduler = new FairScheduler(maxConcurrentTasks, "fair-scheduler-runner-%d", ticker);
        scheduler.start();
        return scheduler;
    }

    public void start()
    {
        // Admission is driven by schedule(); kick it in case work was enqueued before start().
        schedule();
    }

    public void pause()
    {
        paused.close();
    }

    public void resume()
    {
        paused.open();
        schedule();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        Set<TaskControl> tasks = queue.finishAll();

        for (TaskControl task : tasks) {
            task.cancel();
        }

        taskExecutor.shutdownNow();
    }

    public synchronized Group createGroup(String name)
    {
        checkArgument(!closed, "Already closed");

        Group group = new Group(name);
        queue.startGroup(group);

        return group;
    }

    public synchronized void removeGroup(Group group)
    {
        checkArgument(!closed, "Already closed");

        Set<TaskControl> tasks = queue.finishGroup(group);

        for (TaskControl task : tasks) {
            task.cancel();
        }
    }

    public Set<Integer> getTasks(Group group)
    {
        return queue.getTasks(group).stream()
                .map(TaskControl::id)
                .collect(toImmutableSet());
    }

    public synchronized ListenableFuture<Void> submit(Group group, int id, Schedulable runner)
    {
        checkArgument(!closed, "Already closed");

        TaskControl task = new TaskControl(group, id, ticker);

        return taskExecutor.submit(() -> runTask(runner, task), null);
    }

    private void runTask(Schedulable runner, TaskControl task)
    {
        task.setThread(Thread.currentThread());

        if (!makeRunnableAndAwait(task, 0)) {
            return;
        }

        SchedulerContext context = new SchedulerContext(this, task);
        try {
            runner.run(context);
        }
        catch (Exception e) {
            LOG.error(e);
        }
        finally {
            // If the runner exited due to an exception in user code or
            // normally (not in response to an interruption during blocking or yield),
            // it must have had a semaphore permit reserved, so release it.
            if (task.getState() == TaskControl.State.RUNNING) {
                concurrencyControl.release(task);
            }
            queue.finish(task.group(), task);
            task.transitionToFinished();
            // The finished task freed a slot; hand it to the next runnable task.
            schedule();
        }
    }

    private boolean makeRunnableAndAwait(TaskControl task, long deltaWeight)
    {
        if (!task.transitionToWaiting()) {
            return false;
        }

        if (!queue.enqueue(task.group(), task, deltaWeight)) {
            return false;
        }

        // The task is now runnable; try to place it (or another runnable task) on a free slot.
        schedule();

        // wait for the task to be scheduled
        return awaitReadyAndTransitionToRunning(task);
    }

    /**
     * @return false if the transition was unsuccessful due to the task being cancelled
     */
    private boolean awaitReadyAndTransitionToRunning(TaskControl task)
    {
        if (!task.awaitReady()) {
            if (task.isReady()) {
                // If the task was marked as ready (slot acquired) but then cancelled before
                // awaitReady() was notified, we need to release the slot.
                concurrencyControl.release(task);
            }
            return false;
        }

        if (!task.transitionToRunning()) {
            concurrencyControl.release(task);
            return false;
        }

        return true;
    }

    boolean yield(TaskControl task)
    {
        checkState(task.getThread() == Thread.currentThread(), "yield() may only be called from the task thread");

        long delta = task.elapsed();
        if (delta < QUANTUM_NANOS) {
            return true;
        }

        concurrencyControl.release(task);

        return makeRunnableAndAwait(task, delta);
    }

    boolean block(TaskControl task, ListenableFuture<?> future)
    {
        checkState(task.getThread() == Thread.currentThread(), "block() may only be called from the task thread");

        long delta = task.elapsed();

        concurrencyControl.release(task);

        // The blocking task just freed a slot; let another runnable task use it.
        schedule();

        if (!task.transitionToBlocked()) {
            return false;
        }

        if (!queue.block(task.group(), task, delta)) {
            return false;
        }

        future.addListener(task::markUnblocked, MoreExecutors.directExecutor());
        task.awaitUnblock();

        return makeRunnableAndAwait(task, 0);
    }

    /// Fill every free concurrency slot with the next runnable task and wake its thread. Runs on
    /// whichever thread triggered a scheduling opportunity; serialized by [#scheduleLock].
    private void schedule()
    {
        if (closed) {
            return;
        }

        scheduleLock.lock();
        try {
            while (paused.isOpen() && !closed) {
                if (!concurrencyControl.tryReserve()) {
                    // No free slot; a running task will call schedule() again when it releases one.
                    break;
                }

                TaskControl task = queue.tryDequeue(QUANTUM_NANOS);
                if (task == null) {
                    // Slot is free but nothing is runnable; return the slot and stop.
                    concurrencyControl.releaseSlot();
                    break;
                }

                concurrencyControl.register(task);
                if (!task.markReady()) {
                    // Task was cancelled before it could run; free the slot and try the next one.
                    concurrencyControl.release(task);
                }
            }
        }
        finally {
            scheduleLock.unlock();
        }
    }

    long getStartNanos(TaskControl task)
    {
        return task.getStartNanos();
    }

    long getScheduledNanos(TaskControl task)
    {
        return task.getScheduledNanos();
    }

    long getWaitNanos(TaskControl task)
    {
        return task.getWaitNanos();
    }

    long getBlockedNanos(TaskControl task)
    {
        return task.getBlockedNanos();
    }

    public String diagnostics()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(queue);

        builder.append("Task executor: pool=%s, active=%s, queue=%s\n".formatted(
                executor.getPoolSize(),
                executor.getActiveCount(),
                executor.getQueue().size()));

        builder.append("Concurrency control: slots=%s, available=%s\n".formatted(
                concurrencyControl.totalSlots(),
                concurrencyControl.availableSlots()));

        builder.append("Reservations:\n");
        concurrencyControl.reservations().forEach(reservation ->
                builder.append("    ")
                        .append(reservation)
                        .append("\n"));

        return builder.toString();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", FairScheduler.class.getSimpleName() + "[", "]")
                .add("queue=" + queue)
                .add("concurrencyControl=" + concurrencyControl)
                .add("closed=" + closed)
                .toString();
    }

    //
    // STATS, exposed from ThreadPerDriverTaskExecutor
    //
    public ThreadPoolExecutorMBean getTaskExecutor()
    {
        return executorMBean;
    }

    public int getConcurrencyControlTotalSlots()
    {
        return concurrencyControl.totalSlots();
    }

    public int getConcurrencyControlAvailableSlots()
    {
        return concurrencyControl.availableSlots();
    }
}
