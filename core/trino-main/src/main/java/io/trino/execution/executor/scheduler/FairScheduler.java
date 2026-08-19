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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;

/**
 * <h2>Implementation nodes</h2>
 *
 * <ul>
 *     <li>The TaskControl state machine is only modified by the task executor
 * thread (i.e., from within {@link FairScheduler#runTask(Schedulable, TaskControl)} )}). Other threads
 * can indirectly affect what the task executor thread does by marking the task as ready or cancelled
 * and unblocking the task executor thread, which will then act on that information.</li>
 * </ul>
 */
@ThreadSafe
public final class FairScheduler
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(FairScheduler.class);

    public static final long QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * A single shard keeps fair-share ordering exact across all groups. See
     * {@link ShardedSchedulingQueue} for what is given up by raising this.
     */
    public static final int DEFAULT_SHARDS = 1;

    private final ExecutorService schedulerExecutor;
    private final ThreadPoolExecutorMBean schedulerExecutorMBean;
    private final ListeningExecutorService taskExecutor;
    private final ThreadPoolExecutor executor; // instance underlying taskExecutor, for diagnostics
    private final ThreadPoolExecutorMBean executorMBean;
    private final ShardedSchedulingQueue<Group, TaskControl> queue;
    private final Reservation<TaskControl> concurrencyControl;
    private final Ticker ticker;

    private final LongAdder bypassedResumeCount = new LongAdder();
    private final LongAdder scheduledResumeCount = new LongAdder();

    /**
     * Tasks that a scheduler thread has dequeued but not yet handed a concurrency slot. They
     * have already won a scheduling decision, so {@link #tryResumeWithoutScheduler} must not
     * take a slot ahead of them.
     */
    private final AtomicInteger pendingStarts = new AtomicInteger();

    private final Gate paused = new Gate(true);

    // written under this monitor, read without it by runTask()
    private volatile boolean closed;

    public FairScheduler(int maxConcurrentTasks, String threadNameFormat, Ticker ticker)
    {
        this(maxConcurrentTasks, DEFAULT_SHARDS, threadNameFormat, ticker);
    }

    public FairScheduler(int maxConcurrentTasks, int shards, String threadNameFormat, Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");

        concurrencyControl = new Reservation<>(maxConcurrentTasks);
        queue = new ShardedSchedulingQueue<>(shards);

        schedulerExecutor = Executors.newCachedThreadPool(daemonThreadsNamed("fair-scheduler-%d"));
        schedulerExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) schedulerExecutor);

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
        return newInstance(maxConcurrentTasks, DEFAULT_SHARDS, ticker);
    }

    public static FairScheduler newInstance(int maxConcurrentTasks, int shards, Ticker ticker)
    {
        FairScheduler scheduler = new FairScheduler(maxConcurrentTasks, shards, "fair-scheduler-runner-%d", ticker);
        scheduler.start();
        return scheduler;
    }

    public void start()
    {
        for (int i = 0; i < queue.shardCount(); i++) {
            BlockingSchedulingQueue<Group, TaskControl> shard = queue.shard(i);
            schedulerExecutor.submit(() -> runScheduler(shard));
        }
    }

    public void pause()
    {
        paused.close();
    }

    public void resume()
    {
        paused.open();
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
        schedulerExecutor.shutdownNow();
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

        try {
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
            }
        }
        finally {
            // A cancellation interrupt can still be pending on this thread if nothing consumed it.
            // Clear it so it cannot land on whatever split this pooled thread runs next, but keep
            // it while shutting down, where interruption is how the pool is stopped.
            if (Thread.interrupted() && closed) {
                Thread.currentThread().interrupt();
            }
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

        if (!task.transitionToBlocked()) {
            return false;
        }

        if (!queue.block(task.group(), task, delta)) {
            return false;
        }

        future.addListener(task::markUnblocked, MoreExecutors.directExecutor());
        task.awaitUnblock();

        if (tryResumeWithoutScheduler(task)) {
            bypassedResumeCount.increment();
            return true;
        }

        scheduledResumeCount.increment();
        return makeRunnableAndAwait(task, 0);
    }

    /**
     * Attempts to resume a just-unblocked task on its own thread, skipping the round trip
     * through the scheduler thread. Drivers block and unblock far more often than they exhaust
     * their quantum, and the regular path costs two context switches plus three lock
     * acquisitions for each of those transitions even when the worker has spare capacity.
     *
     * <p>The bypass is only taken when there is nothing runnable waiting for a slot, so a task
     * can never barge ahead of a task the scheduler would have picked instead. Both the
     * runnable count and the slot acquisition are checked optimistically and re-validated by
     * {@link BlockingSchedulingQueue#unblockToRunning}, which rejects the bypass if the
     * situation changed; the caller then falls back to the regular path.</p>
     *
     * @return true if the task is now running and the caller may return to the driver
     */
    private boolean tryResumeWithoutScheduler(TaskControl task)
    {
        // The scheduler thread stops handing out slots while paused, so the bypass has to as
        // well, otherwise pausing would no longer stop scheduling
        if (!paused.isOpen()) {
            return false;
        }

        if (queue.getRunnableCount() > 0 || pendingStarts.get() > 0) {
            return false;
        }

        if (!concurrencyControl.tryReserve()) {
            return false;
        }

        if (!queue.unblockToRunning(task.group(), task, QUANTUM_NANOS)) {
            concurrencyControl.releaseUnregistered();
            return false;
        }

        concurrencyControl.register(task);

        if (!task.transitionToRunning()) {
            concurrencyControl.release(task);
            return false;
        }

        return true;
    }

    private void runScheduler(BlockingSchedulingQueue<Group, TaskControl> shard)
    {
        while (true) {
            try {
                paused.awaitOpen();

                // Dequeue before reserving. Reserving first would let a scheduler thread sit on a
                // concurrency slot while its shard has nothing to run, stranding that slot for as
                // long as the shard stays idle.
                TaskControl task = shard.dequeue(QUANTUM_NANOS);

                pendingStarts.incrementAndGet();
                try {
                    concurrencyControl.reserve();
                    concurrencyControl.register(task);
                    if (!task.markReady()) {
                        concurrencyControl.release(task);
                    }
                }
                finally {
                    pendingStarts.decrementAndGet();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (Exception e) {
                LOG.error(e);
            }
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

        // The tasks holding a slot are exactly those the queue reports as RUNNING above
        builder.append("Concurrency control: slots=%s, available=%s, pending starts=%s\n".formatted(
                concurrencyControl.totalSlots(),
                concurrencyControl.availableSlots(),
                pendingStarts.get()));

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
    public ThreadPoolExecutorMBean getSchedulerExecutor()
    {
        return schedulerExecutorMBean;
    }

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

    public int getShardCount()
    {
        return queue.shardCount();
    }

    /**
     * Number of times an unblocked task resumed on its own thread without involving the
     * scheduler thread. Compare against {@link #getScheduledResumeCount()} to see how often the
     * bypass applies.
     */
    public long getBypassedResumeCount()
    {
        return bypassedResumeCount.sum();
    }

    /**
     * Number of times an unblocked task had to go through the scheduler thread to resume.
     */
    public long getScheduledResumeCount()
    {
        return scheduledResumeCount.sum();
    }
}
