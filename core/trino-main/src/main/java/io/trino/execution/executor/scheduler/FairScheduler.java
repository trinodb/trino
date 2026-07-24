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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.TreeMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

/// Implementation notes:
///
///   - The [TaskControl] state machine is only modified by the task executor thread (i.e. from
///     within [#runTask]). Other threads can indirectly affect what the task executor thread does by
///     marking the task as ready or cancelled and unblocking it, which will then act on that
///     information.
///   - There is no scheduler thread. Whichever thread makes a task runnable or frees a slot calls
///     [#dispatch()], which hands free slots to runnable tasks in fair order. A slot is reserved
///     under the queue lock only once a runnable task is present, so it is always held by a task;
///     every release is followed by a dispatch, so no runnable task waits while a slot is free.
@ThreadSafe
public final class FairScheduler
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(FairScheduler.class);

    public static final long QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(1000);

    private final ListeningExecutorService taskExecutor;
    private final ThreadPoolExecutor executor; // instance underlying taskExecutor, for diagnostics
    private final ThreadPoolExecutorMBean executorMBean;
    private final BlockingSchedulingQueue queue = new BlockingSchedulingQueue();
    private final Reservation<TaskControl> concurrencyControl;
    private final Ticker ticker;

    private volatile boolean paused;

    // Guards the producer donations below. Lock order: boostLock, then the queue's lock.
    private final ReentrantLock boostLock = new ReentrantLock();

    // Producer groups (whole pipelines) currently boosted, each mapped to the multiset of donor
    // ranks of the tasks waiting on it: the boost applies to the group node so every driver in the
    // pipeline runs sooner. The producer inherits the most urgent waiter's rank; cleared when empty.
    @GuardedBy("boostLock")
    private final Map<Group, TreeMultiset<Long>> pipelineBoostDonors = new HashMap<>();

    /// Prevents group creation and removal from racing with [#close()]. Submission only needs the
    /// volatile `closed` check: a task accepted while close is in progress either observes that its
    /// group was removed, or is rejected by the executor after shutdown.
    private final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private volatile boolean closed;

    public FairScheduler(int maxConcurrentTasks, String threadNameFormat, Ticker ticker)
    {
        this(maxConcurrentTasks, daemonThreadsNamed(threadNameFormat), ticker);
    }

    @VisibleForTesting
    public FairScheduler(int maxConcurrentTasks, ThreadFactory threadFactory, Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");

        concurrencyControl = new Reservation<>(maxConcurrentTasks);

        executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
        executorMBean = new ThreadPoolExecutorMBean(executor);
        taskExecutor = MoreExecutors.listeningDecorator(executor);
    }

    public static FairScheduler newInstance(int maxConcurrentTasks)
    {
        return newInstance(maxConcurrentTasks, Ticker.systemTicker());
    }

    public static FairScheduler newInstance(int maxConcurrentTasks, Ticker ticker)
    {
        return new FairScheduler(maxConcurrentTasks, "fair-scheduler-runner-%d", ticker);
    }

    public void pause()
    {
        paused = true;
    }

    public void resume()
    {
        paused = false;
        dispatch();
    }

    @Override
    public void close()
    {
        lifecycleLock.writeLock().lock();
        try {
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
        finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    public Group createGroup(String name)
    {
        lifecycleLock.readLock().lock();
        try {
            checkArgument(!closed, "Already closed");

            Group group = new Group(name);
            queue.startGroup(group);

            return group;
        }
        finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /// Create a group nested under `parent`. Fairness is enforced at every level: children of
    /// `parent` share `parent`'s slice, and tasks within this group share this group's.
    public Group createGroup(Group parent, String name)
    {
        lifecycleLock.readLock().lock();
        try {
            checkArgument(!closed, "Already closed");

            Group group = new Group(parent, name);
            queue.startGroup(group);

            return group;
        }
        finally {
            lifecycleLock.readLock().unlock();
        }
    }

    public void removeGroup(Group group)
    {
        lifecycleLock.readLock().lock();
        try {
            checkArgument(!closed, "Already closed");

            Set<TaskControl> tasks = queue.finishGroup(group);

            for (TaskControl task : tasks) {
                task.cancel();
            }
        }
        finally {
            lifecycleLock.readLock().unlock();
        }
    }

    public Set<Integer> getTasks(Group group)
    {
        return queue.getTasks(group).stream()
                .map(TaskControl::id)
                .collect(toImmutableSet());
    }

    public ListenableFuture<Void> submit(Group group, int id, Schedulable runner)
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
            long delta = 0;
            if (task.getState() == TaskControl.State.RUNNING) {
                delta = task.elapsed();
                concurrencyControl.release(task);
            }
            queue.finish(task.group(), task, delta);
            task.transitionToFinished();
            dispatch();
        }
    }

    private boolean makeRunnableAndAwait(TaskControl task, long deltaWeight)
    {
        boolean enqueued = task.transitionToWaiting() && queue.enqueue(task.group(), task, deltaWeight);
        // Dispatch even if the task was cancelled: a yield freed its slot without dispatching it.
        dispatch();
        if (!enqueued) {
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
                release(task);
            }
            return false;
        }

        if (!task.transitionToRunning()) {
            release(task);
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

        // Free the slot without dispatching it: makeRunnableAndAwait re-enqueues this task first, so
        // it competes for the slot in fair order instead of handing it to whichever task was waiting.
        concurrencyControl.release(task);

        return makeRunnableAndAwait(task, delta);
    }

    boolean block(TaskControl task, ListenableFuture<?> future)
    {
        return blockUntil(task, future, () -> () -> {});
    }

    /// Block on `future`, donating priority to the producer groups in `producerPipelines` — e.g. the
    /// build pipeline a probe waits on, or the co-located upstream tasks a consumer reads from over an
    /// exchange — so their drivers run sooner. Donation is capped (see [#producersToBoost]) so a wide
    /// fan-in does not do unbounded boost work under the scheduler lock on every block.
    boolean blockOnProducerPipelines(TaskControl task, ListenableFuture<?> future, Collection<Group> producerPipelines)
    {
        return blockUntil(task, future, () -> {
            List<Runnable> undo = new ArrayList<>();
            // Unwind the boosts already applied if a later one throws, so a partial failure cannot
            // leak a boost — blockUntil only guarantees the returned action runs, not this loop.
            try {
                for (Group producer : producersToBoost(producerPipelines)) {
                    long rank = beginPipelineBoost(task, producer);
                    undo.add(() -> endPipelineBoost(producer, rank));
                }
            }
            catch (RuntimeException e) {
                undo.forEach(Runnable::run);
                throw e;
            }
            return () -> undo.forEach(Runnable::run);
        });
    }

    /// The producer groups to actually boost for one block: all of them when a consumer names at most
    /// a slot's worth, otherwise the slot-count that fair order defers most (highest virtual runtime).
    /// Boosting is priority inheritance, so donating to a wide fan-in is safe, but a boost and an
    /// unboost per producer under the scheduler lock on every block is not free; a producer already
    /// near the front of fair order runs soon anyway, so the ones fair order defers most are worth it.
    private List<Group> producersToBoost(Collection<Group> producerPipelines)
    {
        int limit = concurrencyControl.totalSlots();
        if (producerPipelines.size() <= limit) {
            return List.copyOf(producerPipelines);
        }
        return producerPipelines.stream()
                .map(producer -> Map.entry(producer, queue.groupWeightOf(producer)))
                .sorted(comparingLong((Map.Entry<Group, Long> entry) -> entry.getValue()).reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(toImmutableList());
    }

    /// Release the task's slot and wait until `future` completes (or the task is cancelled or
    /// interrupted). `beginBoost` runs only after the blocking precondition holds and the elapsed
    /// quantum has been measured; it applies any priority donation and returns the action that
    /// withdraws it, which always runs once the wait ends.
    private boolean blockUntil(TaskControl task, ListenableFuture<?> future, Supplier<Runnable> beginBoost)
    {
        checkState(task.getThread() == Thread.currentThread(), "block() may only be called from the task thread");

        long delta = task.elapsed();

        // Free the slot, then apply the donation and charge the time this task used before
        // dispatching it, so the next task, ideally a boosted producer, is chosen against up-to-date
        // weights. Applying the donation only now — after the quantum is measured — means time spent
        // waiting on boostLock is not charged as consumed quantum. The returned action withdraws the
        // donation in the finally.
        concurrencyControl.release(task);
        Runnable onUnblocked;
        try {
            onUnblocked = beginBoost.get();
        }
        catch (RuntimeException e) {
            dispatch();
            throw e;
        }
        try {
            boolean blocked = task.transitionToBlocked() && queue.block(task.group(), task, delta);
            dispatch();
            if (!blocked) {
                return false;
            }

            // Register the unblock listener only after queue.block() has frozen the task out of the
            // scheduling tree: a caller that observes listener registration (some tests do, as proof
            // the task is frozen) must not be able to see the unblock hook before the freeze. Keep
            // these two statements in this order.
            future.addListener(task::markUnblocked, MoreExecutors.directExecutor());
            task.awaitUnblock();
        }
        finally {
            onUnblocked.run();
        }

        return makeRunnableAndAwait(task, 0);
    }

    private long beginPipelineBoost(TaskControl consumer, Group producer)
    {
        boostLock.lock();
        try {
            long rank = queue.weightOf(consumer.group(), consumer);
            TreeMultiset<Long> donors = pipelineBoostDonors.computeIfAbsent(producer, _ -> TreeMultiset.create());
            donors.add(rank);
            queue.boost(producer, donors.firstEntry().getElement());
            return rank;
        }
        finally {
            boostLock.unlock();
        }
    }

    private void endPipelineBoost(Group producer, long rank)
    {
        boostLock.lock();
        try {
            TreeMultiset<Long> donors = pipelineBoostDonors.get(producer);
            if (donors == null) {
                return;
            }
            donors.remove(rank);
            if (donors.isEmpty()) {
                pipelineBoostDonors.remove(producer);
                queue.unboost(producer);
            }
            else {
                queue.boost(producer, donors.firstEntry().getElement());
            }
        }
        finally {
            boostLock.unlock();
        }
    }

    @VisibleForTesting
    public int pipelineBoostCount(Group pipeline)
    {
        boostLock.lock();
        try {
            TreeMultiset<Long> donors = pipelineBoostDonors.get(pipeline);
            return donors == null ? 0 : donors.size();
        }
        finally {
            boostLock.unlock();
        }
    }

    /// The urgency rank a boosted pipeline currently carries — the most urgent (minimum) of its
    /// donors' ranks — or [Long#MIN_VALUE] if it is not boosted.
    @VisibleForTesting
    public long pipelineBoostRank(Group pipeline)
    {
        boostLock.lock();
        try {
            TreeMultiset<Long> donors = pipelineBoostDonors.get(pipeline);
            return donors == null || donors.isEmpty() ? Long.MIN_VALUE : donors.firstEntry().getElement();
        }
        finally {
            boostLock.unlock();
        }
    }

    private void release(TaskControl task)
    {
        concurrencyControl.release(task);
        dispatch();
    }

    /// Hand free slots to runnable tasks in fair order, on the calling thread.
    private void dispatch()
    {
        while (!paused) {
            TaskControl task = queue.dequeue(QUANTUM_NANOS, concurrencyControl::tryReserve);
            if (task == null) {
                return;
            }

            concurrencyControl.register(task);
            if (!task.markReady()) {
                // cancelled before it could run
                concurrencyControl.release(task);
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
