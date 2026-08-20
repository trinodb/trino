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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.RunningSplitInfo;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.dedicated.TaskEntry.QueuedSplit;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.spi.VersionEmbedder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThreadPerDriverTaskExecutor
        implements TaskExecutor
{
    private static final Logger LOG = Logger.get(ThreadPerDriverTaskExecutor.class);

    private final FairScheduler scheduler;
    private final Tracer tracer;
    private final VersionEmbedder versionEmbedder;
    private final int targetGlobalLeafDrivers;
    private final int minDriversPerTask;
    private final int maxDriversPerTask;
    private final ScheduledThreadPoolExecutor backgroundTasks = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("task-executor-scheduler-%s"));

    @GuardedBy("this")
    private final Map<TaskId, TaskEntry> tasks = new HashMap<>();

    private volatile boolean closed;

    @GuardedBy("this")
    private int runningLeafDrivers;

    // Do not inline this field to avoid creating lambdas that cannot be cached by JVM.
    private final Runnable leafSplitDoneCallback = this::leafSplitDone;
    private final Runnable scheduleMoreLeafSplitsQuietly = maintenance(this::scheduleMoreLeafSplits, "Error scheduling leaf splits");

    @Inject
    public ThreadPerDriverTaskExecutor(TaskManagerConfig config, Tracer tracer, VersionEmbedder versionEmbedder)
    {
        this(tracer,
                versionEmbedder,
                new FairScheduler(config.getMaxWorkerThreads(), "SplitRunner-%d", Ticker.systemTicker()),
                config.getMinDriversPerTask(),
                config.getMaxDriversPerTask(),
                config.getMinDrivers());
    }

    @VisibleForTesting
    public ThreadPerDriverTaskExecutor(Tracer tracer, VersionEmbedder versionEmbedder, FairScheduler scheduler, int minDriversPerTask, int maxDriversPerTask, int targetGlobalLeafDrivers)
    {
        this.scheduler = scheduler;
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.minDriversPerTask = minDriversPerTask;
        this.maxDriversPerTask = maxDriversPerTask;
        this.targetGlobalLeafDrivers = targetGlobalLeafDrivers;
    }

    @PostConstruct
    @Override
    public synchronized void start()
    {
        scheduler.start();
        backgroundTasks.scheduleWithFixedDelay(scheduleMoreLeafSplitsQuietly, 0, 100, TimeUnit.MILLISECONDS);
        backgroundTasks.scheduleWithFixedDelay(maintenance(this::adjustConcurrency, "Error adjusting task concurrency"), 0, 10, TimeUnit.MILLISECONDS);
        backgroundTasks.scheduleWithFixedDelay(maintenance(this::logDiagnostics, "Error logging diagnostics"), 0, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    @Override
    public synchronized void stop()
    {
        closed = true;
        tasks.values().forEach(TaskEntry::destroy);
        backgroundTasks.shutdownNow();
        scheduler.close();
    }

    @Override
    public synchronized TaskHandle addTask(
            TaskId taskId,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        checkArgument(!closed, "Executor is already closed");
        TaskEntry task = new TaskEntry(
                taskId,
                scheduler,
                versionEmbedder,
                tracer,
                initialSplitConcurrency,
                utilizationSupplier);
        tasks.put(taskId, task);
        return task;
    }

    @Override
    public void removeTask(TaskHandle handle)
    {
        TaskEntry entry = (TaskEntry) handle;
        synchronized (this) {
            tasks.remove(entry.taskId());
        }
        if (!entry.isDestroyed()) {
            entry.destroy();
        }
    }

    @Override
    public List<ListenableFuture<Void>> enqueueSplits(TaskHandle handle, boolean intermediate, List<? extends SplitRunner> splits)
    {
        checkArgument(!closed, "Executor is already closed");

        TaskEntry entry = (TaskEntry) handle;

        List<ListenableFuture<Void>> futures = new ArrayList<>(splits.size());
        for (SplitRunner split : splits) {
            if (intermediate) {
                futures.add(entry.runSplit(split));
            }
            else {
                futures.add(entry.enqueueLeafSplit(split));
            }
        }

        scheduleMoreLeafSplits();
        return futures;
    }

    @VisibleForTesting
    void leafSplitDone()
    {
        synchronized (this) {
            runningLeafDrivers--;
        }
        // Must be the wrapped form: this runs on the thread of a split that just finished, which
        // is in no position to handle another task's split failing to start.
        scheduleMoreLeafSplitsQuietly.run();
    }

    private void scheduleMoreLeafSplits()
    {
        // Start the splits outside the lock. The scheduler creates a thread per split, which on a
        // loaded worker takes long enough that holding the lock across it stalls task registration,
        // task removal and split completion for every task on the worker.
        List<ClaimedLeafSplit> claimed = claimMoreLeafSplits();
        for (int i = 0; i < claimed.size(); i++) {
            ClaimedLeafSplit split = claimed.get(i);
            try {
                split.task().startLeafSplit(split.split(), leafSplitDoneCallback);
            }
            catch (Throwable e) {
                // A claim is only given back by the listener that starting the split installs, so
                // give back this claim and the ones behind it that never got one.
                releaseClaims(claimed.subList(i, claimed.size()), e);
                throw e;
            }
        }
    }

    private void releaseClaims(List<ClaimedLeafSplit> claimed, Throwable cause)
    {
        synchronized (this) {
            runningLeafDrivers -= claimed.size();
        }

        for (ClaimedLeafSplit split : claimed) {
            split.task().releaseLeafSplit(split.split(), cause);
        }
    }

    private synchronized List<ClaimedLeafSplit> claimMoreLeafSplits()
    {
        if (closed) {
            return ImmutableList.of();
        }

        List<ClaimedLeafSplit> claimed = new ArrayList<>();

        // claim minimum guaranteed leaf drivers for each task
        for (TaskEntry task : tasks.values()) {
            int target = max(0, minDriversPerTask - task.runningLeafSplits());
            for (int i = 0; i < target; i++) {
                if (!claimLeafSplit(task, claimed)) {
                    break;
                }
            }
        }

        // claim additional drivers up to the target global leaf drivers
        Queue<TaskEntry> queue = new ArrayDeque<>(tasks.values());
        int target = targetGlobalLeafDrivers - runningLeafDrivers;
        for (int i = 0; i < target && !queue.isEmpty(); i++) {
            TaskEntry task = queue.poll();
            if (task.runningLeafSplits() < min(task.targetConcurrency(), maxDriversPerTask)) {
                claimLeafSplit(task, claimed);
                if (task.hasPendingLeafSplits()) {
                    queue.add(task);
                }
            }
        }

        return claimed;
    }

    @GuardedBy("this")
    private boolean claimLeafSplit(TaskEntry task, List<ClaimedLeafSplit> claimed)
    {
        QueuedSplit split = task.claimLeafSplit();
        if (split == null) {
            return false;
        }

        runningLeafDrivers++;
        claimed.add(new ClaimedLeafSplit(task, split));

        return true;
    }

    private record ClaimedLeafSplit(TaskEntry task, QueuedSplit split) {}

    /// Wrap a task so that a failure does not take its caller down with it.
    /// [ScheduledThreadPoolExecutor#scheduleWithFixedDelay] silently stops rescheduling a task
    /// that throws, which would leave the worker permanently without leaf split scheduling or
    /// concurrency adjustment. Failures here are typically symptoms of an overloaded worker,
    /// such as being unable to create a thread, and it is expected to recover once load subsides.
    @VisibleForTesting
    static Runnable maintenance(Runnable task, String errorMessage)
    {
        return () -> {
            try {
                task.run();
            }
            catch (Throwable e) {
                LOG.warn(e, "%s", errorMessage);
            }
        };
    }

    private void adjustConcurrency()
    {
        for (TaskEntry task : activeTasks()) {
            task.updateConcurrency();
        }
    }

    private synchronized List<TaskEntry> activeTasks()
    {
        return ImmutableList.copyOf(tasks.values());
    }

    private void logDiagnostics()
    {
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Queue:\n");
            builder.append(scheduler.diagnostics().indent(4));

            builder.append("Query tasks:\n");
            for (TaskEntry task : activeTasks()) {
                builder.append("%s: [total running = %s, leaf running = %s, leaf pending = %s, target concurrency = %s]\n".formatted(
                        task.taskId(),
                        task.totalRunningSplits(),
                        task.runningLeafSplits(),
                        task.pendingLeafSplitCount(),
                        task.targetConcurrency()).indent(4));
            }

            LOG.debug("\n%s", builder);
        }
    }

    @Override
    public Set<TaskId> getStuckSplitTaskIds(Duration processingDurationThreshold, Predicate<RunningSplitInfo> filter)
    {
        // TODO
        return ImmutableSet.of();
    }

    @Managed
    public synchronized int getTasks()
    {
        return tasks.size();
    }

    @Managed
    public synchronized int getTotalRunningSplits()
    {
        return tasks.values().stream()
                .mapToInt(TaskEntry::totalRunningSplits)
                .sum();
    }

    @Managed
    public synchronized int getTotalRunningLeafSplits()
    {
        return tasks.values().stream()
                .mapToInt(TaskEntry::runningLeafSplits)
                .sum();
    }

    @Managed
    public synchronized int getTotalPendingLeafSplits()
    {
        return tasks.values().stream()
                .mapToInt(TaskEntry::pendingLeafSplitCount)
                .sum();
    }

    @Managed(description = "Scheduler executor")
    @Nested
    public ThreadPoolExecutorMBean getSchedulerExecutor()
    {
        return scheduler.getSchedulerExecutor();
    }

    @Managed(description = "Task executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskExecutor()
    {
        return scheduler.getTaskExecutor();
    }

    @Managed
    public int getConcurrencyControlTotalSlots()
    {
        return scheduler.getConcurrencyControlTotalSlots();
    }

    @Managed
    public int getConcurrencyControlAvailableSlots()
    {
        return scheduler.getConcurrencyControlAvailableSlots();
    }
}
