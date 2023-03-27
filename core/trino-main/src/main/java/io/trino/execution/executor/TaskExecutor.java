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
package io.trino.execution.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeDistribution;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.VersionEmbedder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.execution.executor.MultilevelSplitQueue.computeLevel;
import static io.trino.version.EmbedVersion.testingVersionEmbedder;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class TaskExecutor
{
    private static final Logger log = Logger.get(TaskExecutor.class);
    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();

    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;

    private final int runnerThreads;
    private final int minimumNumberOfDrivers;
    private final int guaranteedNumberOfDriversPerTask;
    private final int maximumNumberOfDriversPerTask;
    private final VersionEmbedder versionEmbedder;

    private final Ticker ticker;

    private final Duration stuckSplitsWarningThreshold;
    private final ScheduledExecutorService splitMonitorExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("TaskExecutor"));
    private final SortedSet<RunningSplitInfo> runningSplitInfos = new ConcurrentSkipListSet<>();

    @GuardedBy("this")
    private final List<TaskHandle> tasks;

    /**
     * All splits registered with the task executor.
     */
    @GuardedBy("this")
    private final Set<PrioritizedSplitRunner> allSplits = new HashSet<>();

    /**
     * Intermediate splits (i.e. splits that should not be queued).
     */
    @GuardedBy("this")
    private final Set<PrioritizedSplitRunner> intermediateSplits = new HashSet<>();

    /**
     * Splits waiting for a runner thread.
     */
    private final MultilevelSplitQueue waitingSplits;

    /**
     * Splits running on a thread.
     */
    private final Set<PrioritizedSplitRunner> runningSplits = newConcurrentHashSet();

    /**
     * Splits blocked by the driver.
     */
    private final Map<PrioritizedSplitRunner, Future<Void>> blockedSplits = new ConcurrentHashMap<>();

    private final AtomicLongArray completedTasksPerLevel = new AtomicLongArray(5);
    private final AtomicLongArray completedSplitsPerLevel = new AtomicLongArray(5);

    private final TimeStat splitQueuedTime = new TimeStat(NANOSECONDS);
    private final TimeStat splitWallTime = new TimeStat(NANOSECONDS);

    private final TimeDistribution leafSplitWallTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitWallTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitScheduledTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitScheduledTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitWaitTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitWaitTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitCpuTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitCpuTime = new TimeDistribution(MICROSECONDS);

    // shared between SplitRunners
    private final CounterStat globalCpuTimeMicros = new CounterStat();
    private final CounterStat globalScheduledTimeMicros = new CounterStat();

    private final TimeStat blockedQuantaWallTime = new TimeStat(MICROSECONDS);
    private final TimeStat unblockedQuantaWallTime = new TimeStat(MICROSECONDS);

    private final DistributionStat leafSplitsSize = new DistributionStat();
    @GuardedBy("this")
    private long lastLeafSplitsSizeRecordTime;
    @GuardedBy("this")
    private long lastLeafSplitsSize;

    private volatile boolean closed;

    @Inject
    public TaskExecutor(TaskManagerConfig config, VersionEmbedder versionEmbedder, MultilevelSplitQueue splitQueue)
    {
        this(
                config.getMaxWorkerThreads(),
                config.getMinDrivers(),
                config.getMinDriversPerTask(),
                config.getMaxDriversPerTask(),
                config.getInterruptStuckSplitTasksWarningThreshold(),
                versionEmbedder,
                splitQueue,
                Ticker.systemTicker());
    }

    @VisibleForTesting
    public TaskExecutor(int runnerThreads, int minDrivers, int guaranteedNumberOfDriversPerTask, int maximumNumberOfDriversPerTask, Ticker ticker)
    {
        this(runnerThreads, minDrivers, guaranteedNumberOfDriversPerTask, maximumNumberOfDriversPerTask, new Duration(10, TimeUnit.MINUTES), testingVersionEmbedder(), new MultilevelSplitQueue(2), ticker);
    }

    @VisibleForTesting
    public TaskExecutor(int runnerThreads, int minDrivers, int guaranteedNumberOfDriversPerTask, int maximumNumberOfDriversPerTask, MultilevelSplitQueue splitQueue, Ticker ticker)
    {
        this(runnerThreads, minDrivers, guaranteedNumberOfDriversPerTask, maximumNumberOfDriversPerTask, new Duration(10, TimeUnit.MINUTES), testingVersionEmbedder(), splitQueue, ticker);
    }

    @VisibleForTesting
    public TaskExecutor(
            int runnerThreads,
            int minDrivers,
            int guaranteedNumberOfDriversPerTask,
            int maximumNumberOfDriversPerTask,
            Duration stuckSplitsWarningThreshold,
            VersionEmbedder versionEmbedder,
            MultilevelSplitQueue splitQueue,
            Ticker ticker)
    {
        checkArgument(runnerThreads > 0, "runnerThreads must be at least 1");
        checkArgument(guaranteedNumberOfDriversPerTask > 0, "guaranteedNumberOfDriversPerTask must be at least 1");
        checkArgument(maximumNumberOfDriversPerTask > 0, "maximumNumberOfDriversPerTask must be at least 1");
        checkArgument(guaranteedNumberOfDriversPerTask <= maximumNumberOfDriversPerTask, "guaranteedNumberOfDriversPerTask cannot be greater than maximumNumberOfDriversPerTask");

        // we manage thread pool size directly, so create an unlimited pool
        this.executor = newCachedThreadPool(threadsNamed("task-processor-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        this.runnerThreads = runnerThreads;
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");

        this.ticker = requireNonNull(ticker, "ticker is null");
        this.stuckSplitsWarningThreshold = requireNonNull(stuckSplitsWarningThreshold, "stuckSplitsWarningThreshold is null");

        this.minimumNumberOfDrivers = minDrivers;
        this.guaranteedNumberOfDriversPerTask = guaranteedNumberOfDriversPerTask;
        this.maximumNumberOfDriversPerTask = maximumNumberOfDriversPerTask;
        this.waitingSplits = requireNonNull(splitQueue, "splitQueue is null");
        this.tasks = new LinkedList<>();
        this.lastLeafSplitsSizeRecordTime = ticker.read();
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < runnerThreads; i++) {
            addRunnerThread();
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        closed = true;
        executor.shutdownNow();
        splitMonitorExecutor.shutdownNow();
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("runnerThreads", runnerThreads)
                .add("allSplits", allSplits.size())
                .add("intermediateSplits", intermediateSplits.size())
                .add("waitingSplits", waitingSplits.size())
                .add("runningSplits", runningSplits.size())
                .add("blockedSplits", blockedSplits.size())
                .toString();
    }

    private synchronized void addRunnerThread()
    {
        try {
            executor.execute(versionEmbedder.embedVersion(new TaskRunner()));
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    public synchronized TaskHandle addTask(
            TaskId taskId,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(utilizationSupplier, "utilizationSupplier is null");
        checkArgument(maxDriversPerTask.isEmpty() || maxDriversPerTask.getAsInt() <= maximumNumberOfDriversPerTask,
                "maxDriversPerTask cannot be greater than the configured value");

        log.debug("Task scheduled %s", taskId);

        TaskHandle taskHandle = new TaskHandle(taskId, waitingSplits, utilizationSupplier, initialSplitConcurrency, splitConcurrencyAdjustFrequency, maxDriversPerTask);

        tasks.add(taskHandle);
        return taskHandle;
    }

    public void removeTask(TaskHandle taskHandle)
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskHandle.getTaskId())) {
            // Skip additional scheduling if the task was already destroyed
            if (!doRemoveTask(taskHandle)) {
                return;
            }
        }

        // replace blocked splits that were terminated
        synchronized (this) {
            addNewEntrants();
            recordLeafSplitsSize();
        }
    }

    /**
     * Returns <code>true</code> if the task handle was destroyed and removed splits as a result that may need to be replaced. Otherwise,
     * if the {@link TaskHandle} was already destroyed or no splits were removed then this method returns <code>false</code> and no additional
     * splits need to be scheduled.
     */
    private boolean doRemoveTask(TaskHandle taskHandle)
    {
        List<PrioritizedSplitRunner> splits;
        synchronized (this) {
            tasks.remove(taskHandle);

            // Task is already destroyed
            if (taskHandle.isDestroyed()) {
                return false;
            }

            splits = taskHandle.destroy();
            // stop tracking splits (especially blocked splits which may never unblock)
            allSplits.removeAll(splits);
            intermediateSplits.removeAll(splits);
            blockedSplits.keySet().removeAll(splits);
            waitingSplits.removeAll(splits);
            recordLeafSplitsSize();
        }

        // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
        for (PrioritizedSplitRunner split : splits) {
            split.destroy();
        }

        // record completed stats
        long threadUsageNanos = taskHandle.getScheduledNanos();
        completedTasksPerLevel.incrementAndGet(computeLevel(threadUsageNanos));

        log.debug("Task finished or failed %s", taskHandle.getTaskId());
        return !splits.isEmpty();
    }

    public List<ListenableFuture<Void>> enqueueSplits(TaskHandle taskHandle, boolean intermediate, List<? extends SplitRunner> taskSplits)
    {
        List<PrioritizedSplitRunner> splitsToDestroy = new ArrayList<>();
        List<ListenableFuture<Void>> finishedFutures = new ArrayList<>(taskSplits.size());
        synchronized (this) {
            for (SplitRunner taskSplit : taskSplits) {
                PrioritizedSplitRunner prioritizedSplitRunner = new PrioritizedSplitRunner(
                        taskHandle,
                        taskSplit,
                        ticker,
                        globalCpuTimeMicros,
                        globalScheduledTimeMicros,
                        blockedQuantaWallTime,
                        unblockedQuantaWallTime);

                if (intermediate) {
                    // add the runner to the handle so it can be destroyed if the task is canceled
                    if (taskHandle.recordIntermediateSplit(prioritizedSplitRunner)) {
                        // Note: we do not record queued time for intermediate splits
                        startIntermediateSplit(prioritizedSplitRunner);
                    }
                    else {
                        splitsToDestroy.add(prioritizedSplitRunner);
                    }
                }
                else {
                    // add this to the work queue for the task
                    if (taskHandle.enqueueSplit(prioritizedSplitRunner)) {
                        // if task is under the limit for guaranteed splits, start one
                        scheduleTaskIfNecessary(taskHandle);
                        // if globally we have more resources, start more
                        addNewEntrants();
                    }
                    else {
                        splitsToDestroy.add(prioritizedSplitRunner);
                    }
                }

                finishedFutures.add(prioritizedSplitRunner.getFinishedFuture());
            }
            recordLeafSplitsSize();
        }
        for (PrioritizedSplitRunner split : splitsToDestroy) {
            split.destroy();
        }
        return finishedFutures;
    }

    private void splitFinished(PrioritizedSplitRunner split)
    {
        completedSplitsPerLevel.incrementAndGet(split.getPriority().getLevel());
        synchronized (this) {
            allSplits.remove(split);

            long wallNanos = System.nanoTime() - split.getCreatedNanos();
            splitWallTime.add(Duration.succinctNanos(wallNanos));

            if (intermediateSplits.remove(split)) {
                intermediateSplitWallTime.add(wallNanos);
                intermediateSplitScheduledTime.add(split.getScheduledNanos());
                intermediateSplitWaitTime.add(split.getWaitNanos());
                intermediateSplitCpuTime.add(split.getCpuTimeNanos());
            }
            else {
                leafSplitWallTime.add(wallNanos);
                leafSplitScheduledTime.add(split.getScheduledNanos());
                leafSplitWaitTime.add(split.getWaitNanos());
                leafSplitCpuTime.add(split.getCpuTimeNanos());
            }

            TaskHandle taskHandle = split.getTaskHandle();
            taskHandle.splitComplete(split);

            scheduleTaskIfNecessary(taskHandle);

            addNewEntrants();
            recordLeafSplitsSize();
        }
        // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
        split.destroy();
    }

    private synchronized void scheduleTaskIfNecessary(TaskHandle taskHandle)
    {
        // if task has less than the minimum guaranteed splits running,
        // immediately schedule new splits for this task.  This assures
        // that a task gets its fair amount of consideration (you have to
        // have splits to be considered for running on a thread).
        int splitsToSchedule = min(guaranteedNumberOfDriversPerTask, taskHandle.getMaxDriversPerTask().orElse(Integer.MAX_VALUE)) - taskHandle.getRunningLeafSplits();
        for (int i = 0; i < splitsToSchedule; ++i) {
            PrioritizedSplitRunner split = taskHandle.pollNextSplit();
            if (split == null) {
                // no more splits to schedule
                return;
            }

            startSplit(split);
            splitQueuedTime.add(Duration.nanosSince(split.getCreatedNanos()));
        }
        recordLeafSplitsSize();
    }

    private synchronized void addNewEntrants()
    {
        // Ignore intermediate splits when checking minimumNumberOfDrivers.
        // Otherwise with (for example) minimumNumberOfDrivers = 100, 200 intermediate splits
        // and 100 leaf splits, depending on order of appearing splits, number of
        // simultaneously running splits may vary. If leaf splits start first, there will
        // be 300 running splits. If intermediate splits start first, there will be only
        // 200 running splits.
        int running = allSplits.size() - intermediateSplits.size();
        for (int i = 0; i < minimumNumberOfDrivers - running; i++) {
            PrioritizedSplitRunner split = pollNextSplitWorker();
            if (split == null) {
                break;
            }

            splitQueuedTime.add(Duration.nanosSince(split.getCreatedNanos()));
            startSplit(split);
        }
    }

    private synchronized void startIntermediateSplit(PrioritizedSplitRunner split)
    {
        startSplit(split);
        intermediateSplits.add(split);
    }

    private synchronized void startSplit(PrioritizedSplitRunner split)
    {
        allSplits.add(split);
        waitingSplits.offer(split);
    }

    private synchronized PrioritizedSplitRunner pollNextSplitWorker()
    {
        // todo find a better algorithm for this
        // find the first task that produces a split, then move that task to the
        // end of the task list, so we get round robin
        for (Iterator<TaskHandle> iterator = tasks.iterator(); iterator.hasNext(); ) {
            TaskHandle task = iterator.next();
            // skip tasks that are already running the configured max number of drivers
            if (task.getRunningLeafSplits() >= task.getMaxDriversPerTask().orElse(maximumNumberOfDriversPerTask)) {
                continue;
            }
            PrioritizedSplitRunner split = task.pollNextSplit();
            if (split != null) {
                // move task to end of list
                iterator.remove();

                // CAUTION: we are modifying the list in the loop which would normally
                // cause a ConcurrentModificationException but we exit immediately
                tasks.add(task);
                return split;
            }
        }
        return null;
    }

    private synchronized void recordLeafSplitsSize()
    {
        long now = ticker.read();
        long timeDifference = now - this.lastLeafSplitsSizeRecordTime;
        if (timeDifference > 0) {
            this.leafSplitsSize.add(lastLeafSplitsSize, timeDifference);
            this.lastLeafSplitsSizeRecordTime = now;
        }
        // always record new lastLeafSplitsSize as it might have changed
        // even if timeDifference is 0
        this.lastLeafSplitsSize = allSplits.size() - intermediateSplits.size();
    }

    private class TaskRunner
            implements Runnable
    {
        private final long runnerId = NEXT_RUNNER_ID.getAndIncrement();

        @Override
        public void run()
        {
            try (SetThreadName runnerName = new SetThreadName("SplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    // select next worker
                    PrioritizedSplitRunner split;
                    try {
                        split = waitingSplits.take();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    String threadId = split.getTaskHandle().getTaskId() + "-" + split.getSplitId();
                    try (SetThreadName splitName = new SetThreadName(threadId)) {
                        RunningSplitInfo splitInfo = new RunningSplitInfo(ticker.read(), threadId, Thread.currentThread(), split);
                        runningSplitInfos.add(splitInfo);
                        runningSplits.add(split);

                        ListenableFuture<Void> blocked;
                        try {
                            blocked = split.process();
                        }
                        finally {
                            runningSplitInfos.remove(splitInfo);
                            runningSplits.remove(split);
                        }

                        if (split.isFinished()) {
                            if (log.isDebugEnabled()) {
                                log.debug("%s is finished", split.getInfo());
                            }
                            splitFinished(split);
                        }
                        else {
                            if (blocked.isDone()) {
                                waitingSplits.offer(split);
                            }
                            else {
                                blockedSplits.put(split, blocked);
                                blocked.addListener(() -> {
                                    blockedSplits.remove(split);
                                    // reset the level priority to prevent previously-blocked splits from starving existing splits
                                    split.resetLevelPriority();
                                    waitingSplits.offer(split);
                                }, executor);
                            }
                        }
                    }
                    catch (Throwable t) {
                        // ignore random errors due to driver thread interruption
                        if (!split.isDestroyed()) {
                            if (t instanceof TrinoException trinoException) {
                                log.error(t, "Error processing %s: %s: %s", split.getInfo(), trinoException.getErrorCode().getName(), trinoException.getMessage());
                            }
                            else {
                                log.error(t, "Error processing %s", split.getInfo());
                            }
                        }
                        splitFinished(split);
                    }
                    finally {
                        // Clear the interrupted flag on the current thread, driver cancellation may have triggered an interrupt
                        if (Thread.interrupted()) {
                            if (closed) {
                                // reset interrupted flag if closed before interrupt
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            }
            finally {
                // unless we have been closed, we need to replace this thread
                if (!closed) {
                    addRunnerThread();
                }
            }
        }
    }

    //
    // STATS
    //

    @Managed
    public synchronized int getTasks()
    {
        return tasks.size();
    }

    @Managed
    public int getRunnerThreads()
    {
        return runnerThreads;
    }

    @Managed
    public int getMinimumNumberOfDrivers()
    {
        return minimumNumberOfDrivers;
    }

    @Managed
    public synchronized int getTotalSplits()
    {
        return allSplits.size();
    }

    @Managed
    public synchronized int getIntermediateSplits()
    {
        return intermediateSplits.size();
    }

    @Managed
    public int getWaitingSplits()
    {
        return waitingSplits.size();
    }

    @Managed
    @Nested
    public DistributionStat getLeafSplitsSize()
    {
        return leafSplitsSize;
    }

    @Managed
    public int getRunningSplits()
    {
        return runningSplits.size();
    }

    @Managed
    public int getBlockedSplits()
    {
        return blockedSplits.size();
    }

    @Managed
    public long getCompletedTasksLevel0()
    {
        return completedTasksPerLevel.get(0);
    }

    @Managed
    public long getCompletedTasksLevel1()
    {
        return completedTasksPerLevel.get(1);
    }

    @Managed
    public long getCompletedTasksLevel2()
    {
        return completedTasksPerLevel.get(2);
    }

    @Managed
    public long getCompletedTasksLevel3()
    {
        return completedTasksPerLevel.get(3);
    }

    @Managed
    public long getCompletedTasksLevel4()
    {
        return completedTasksPerLevel.get(4);
    }

    @Managed
    public long getCompletedSplitsLevel0()
    {
        return completedSplitsPerLevel.get(0);
    }

    @Managed
    public long getCompletedSplitsLevel1()
    {
        return completedSplitsPerLevel.get(1);
    }

    @Managed
    public long getCompletedSplitsLevel2()
    {
        return completedSplitsPerLevel.get(2);
    }

    @Managed
    public long getCompletedSplitsLevel3()
    {
        return completedSplitsPerLevel.get(3);
    }

    @Managed
    public long getCompletedSplitsLevel4()
    {
        return completedSplitsPerLevel.get(4);
    }

    @Managed
    public long getRunningTasksLevel0()
    {
        return getRunningTasksForLevel(0);
    }

    @Managed
    public long getRunningTasksLevel1()
    {
        return getRunningTasksForLevel(1);
    }

    @Managed
    public long getRunningTasksLevel2()
    {
        return getRunningTasksForLevel(2);
    }

    @Managed
    public long getRunningTasksLevel3()
    {
        return getRunningTasksForLevel(3);
    }

    @Managed
    public long getRunningTasksLevel4()
    {
        return getRunningTasksForLevel(4);
    }

    @Managed
    @Nested
    public TimeStat getSplitQueuedTime()
    {
        return splitQueuedTime;
    }

    @Managed
    @Nested
    public TimeStat getSplitWallTime()
    {
        return splitWallTime;
    }

    @Managed
    @Nested
    public TimeStat getBlockedQuantaWallTime()
    {
        return blockedQuantaWallTime;
    }

    @Managed
    @Nested
    public TimeStat getUnblockedQuantaWallTime()
    {
        return unblockedQuantaWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitScheduledTime()
    {
        return leafSplitScheduledTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitScheduledTime()
    {
        return intermediateSplitScheduledTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitWallTime()
    {
        return leafSplitWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitWallTime()
    {
        return intermediateSplitWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitWaitTime()
    {
        return leafSplitWaitTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitWaitTime()
    {
        return intermediateSplitWaitTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitCpuTime()
    {
        return leafSplitCpuTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitCpuTime()
    {
        return intermediateSplitCpuTime;
    }

    @Managed
    @Nested
    public CounterStat getGlobalScheduledTimeMicros()
    {
        return globalScheduledTimeMicros;
    }

    @Managed
    @Nested
    public CounterStat getGlobalCpuTimeMicros()
    {
        return globalCpuTimeMicros;
    }

    private synchronized int getRunningTasksForLevel(int level)
    {
        int count = 0;
        for (TaskHandle task : tasks) {
            if (task.getPriority().getLevel() == level) {
                count++;
            }
        }
        return count;
    }

    public String getMaxActiveSplitsInfo()
    {
        // Sample output:
        //
        // 2 splits have been continuously active for more than 600.00ms seconds
        //
        // "20180907_054754_00000_88xi4.1.0-2" tid=99
        // at java.util.Formatter$FormatSpecifier.<init>(Formatter.java:2708)
        // at java.util.Formatter.parse(Formatter.java:2560)
        // at java.util.Formatter.format(Formatter.java:2501)
        // at ... (more lines of stacktrace)
        //
        // "20180907_054754_00000_88xi4.1.0-3" tid=106
        // at java.util.Formatter$FormatSpecifier.<init>(Formatter.java:2709)
        // at java.util.Formatter.parse(Formatter.java:2560)
        // at java.util.Formatter.format(Formatter.java:2501)
        // at ... (more line of stacktrace)
        StringBuilder stackTrace = new StringBuilder();
        int maxActiveSplitCount = 0;
        String message = "%s splits have been continuously active for more than %s seconds\n";
        for (RunningSplitInfo splitInfo : runningSplitInfos) {
            Duration duration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
            if (duration.compareTo(stuckSplitsWarningThreshold) >= 0) {
                maxActiveSplitCount++;
                stackTrace.append("\n");
                stackTrace.append(format("\"%s\" tid=%s", splitInfo.getThreadId(), splitInfo.getThread().getId())).append("\n");
                for (StackTraceElement traceElement : splitInfo.getThread().getStackTrace()) {
                    stackTrace.append("\tat ").append(traceElement).append("\n");
                }
            }
        }

        return format(message, maxActiveSplitCount, stuckSplitsWarningThreshold).concat(stackTrace.toString());
    }

    @Managed
    public long getRunAwaySplitCount()
    {
        int count = 0;
        for (RunningSplitInfo splitInfo : runningSplitInfos) {
            Duration duration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
            if (duration.compareTo(stuckSplitsWarningThreshold) > 0) {
                count++;
            }
        }
        return count;
    }

    public Set<TaskId> getStuckSplitTaskIds(Duration processingDurationThreshold, Predicate<RunningSplitInfo> filter)
    {
        return runningSplitInfos.stream()
                .filter((RunningSplitInfo splitInfo) -> {
                    Duration splitProcessingDuration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
                    return splitProcessingDuration.compareTo(processingDurationThreshold) > 0;
                })
                .filter(filter).map(RunningSplitInfo::getTaskId).collect(toImmutableSet());
    }

    /**
     * A class representing a split that is running on the TaskRunner.
     * It has a Thread object that gets assigned while assigning the split
     * to the taskRunner. However, when the TaskRunner moves to a different split,
     * the thread stored here will not remain assigned to this split anymore.
     */
    public static class RunningSplitInfo
            implements Comparable<RunningSplitInfo>
    {
        private final long startTime;
        private final String threadId;
        private final Thread thread;
        private boolean printed;
        private final PrioritizedSplitRunner split;

        public RunningSplitInfo(long startTime, String threadId, Thread thread, PrioritizedSplitRunner split)
        {
            this.startTime = startTime;
            this.threadId = requireNonNull(threadId, "threadId is null");
            this.thread = requireNonNull(thread, "thread is null");
            this.split = requireNonNull(split, "split is null");
            this.printed = false;
        }

        public long getStartTime()
        {
            return startTime;
        }

        public String getThreadId()
        {
            return threadId;
        }

        public Thread getThread()
        {
            return thread;
        }

        public TaskId getTaskId()
        {
            return split.getTaskHandle().getTaskId();
        }

        /**
         * {@link PrioritizedSplitRunner#getInfo()} provides runtime statistics for the split (such as total cpu utilization so far).
         * A value returned from this method changes over time and cannot be cached as a field of {@link RunningSplitInfo}.
         *
         * @return Formatted string containing runtime statistics for the split.
         */
        public String getSplitInfo()
        {
            return split.getInfo();
        }

        public boolean isPrinted()
        {
            return printed;
        }

        public void setPrinted()
        {
            printed = true;
        }

        @Override
        public int compareTo(RunningSplitInfo o)
        {
            return ComparisonChain.start()
                    .compare(startTime, o.getStartTime())
                    .compare(threadId, o.getThreadId())
                    .result();
        }
    }

    @Managed(description = "Task processor executor")
    @Nested
    public ThreadPoolExecutorMBean getProcessorExecutor()
    {
        return executorMBean;
    }
}
