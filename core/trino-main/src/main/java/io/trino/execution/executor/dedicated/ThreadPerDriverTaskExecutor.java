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
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.spi.VersionEmbedder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThreadPerDriverTaskExecutor
        implements TaskExecutor
{
    private static final Logger LOG = Logger.get(ThreadPerDriverTaskExecutor.class);

    /**
     * How often tasks are offered a chance to re-evaluate their target concurrency. Each task
     * rate-limits itself to its own configured adjustment interval, so this only bounds how
     * closely that interval can be followed.
     */
    private static final long CONCURRENCY_ADJUSTMENT_TICK_MILLIS = 50;

    private final FairScheduler scheduler;
    private final Tracer tracer;
    private final VersionEmbedder versionEmbedder;
    private final Ticker ticker;
    private final int targetGlobalLeafDrivers;
    private final int minDriversPerTask;
    private final int maxDriversPerTask;
    private final ScheduledThreadPoolExecutor backgroundTasks = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("task-executor-scheduler-%s"));

    @GuardedBy("this")
    private final Map<TaskId, TaskEntry> tasks = new HashMap<>();

    /**
     * Tasks with leaf splits waiting to run. Iteration order is the round-robin order used to
     * hand out spare global capacity. Maintaining this incrementally keeps scheduling a single
     * split independent of how many tasks the worker is running.
     */
    @GuardedBy("this")
    private final Set<TaskEntry> pendingTasks = new LinkedHashSet<>();

    /**
     * Subset of {@link #pendingTasks} still below the per-task guarantee. Empty in steady state.
     */
    @GuardedBy("this")
    private final Set<TaskEntry> belowGuarantee = new LinkedHashSet<>();

    @GuardedBy("this")
    private boolean closed;

    @GuardedBy("this")
    private int runningLeafDrivers;

    @GuardedBy("this")
    private boolean scheduling;

    @GuardedBy("this")
    private boolean rescheduleNeeded;

    // Do not inline this field to avoid creating lambdas that cannot be cached by JVM.
    private final Consumer<TaskEntry> leafSplitDoneCallback = this::leafSplitDone;

    @Inject
    public ThreadPerDriverTaskExecutor(TaskManagerConfig config, Tracer tracer, VersionEmbedder versionEmbedder)
    {
        this(tracer,
                versionEmbedder,
                new FairScheduler(config.getMaxWorkerThreads(), config.getSchedulerShards(), "SplitRunner-%d", Ticker.systemTicker()),
                config.getMinDriversPerTask(),
                config.getMaxDriversPerTask(),
                config.getMinDrivers());
    }

    @VisibleForTesting
    public ThreadPerDriverTaskExecutor(Tracer tracer, VersionEmbedder versionEmbedder, FairScheduler scheduler, int minDriversPerTask, int maxDriversPerTask, int targetGlobalLeafDrivers)
    {
        this(tracer, versionEmbedder, scheduler, minDriversPerTask, maxDriversPerTask, targetGlobalLeafDrivers, Ticker.systemTicker());
    }

    @VisibleForTesting
    public ThreadPerDriverTaskExecutor(Tracer tracer, VersionEmbedder versionEmbedder, FairScheduler scheduler, int minDriversPerTask, int maxDriversPerTask, int targetGlobalLeafDrivers, Ticker ticker)
    {
        this.scheduler = scheduler;
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.minDriversPerTask = minDriversPerTask;
        this.maxDriversPerTask = maxDriversPerTask;
        this.targetGlobalLeafDrivers = targetGlobalLeafDrivers;
    }

    @PostConstruct
    @Override
    public synchronized void start()
    {
        scheduler.start();
        backgroundTasks.scheduleWithFixedDelay(this::reconcileAndScheduleMoreLeafSplits, 0, 100, TimeUnit.MILLISECONDS);
        backgroundTasks.scheduleWithFixedDelay(this::adjustConcurrency, 0, CONCURRENCY_ADJUSTMENT_TICK_MILLIS, TimeUnit.MILLISECONDS);
        backgroundTasks.scheduleWithFixedDelay(this::logDiagnostics, 0, 30, TimeUnit.SECONDS);
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
                splitConcurrencyAdjustFrequency,
                utilizationSupplier,
                ticker);
        tasks.put(taskId, task);
        return task;
    }

    @Override
    public void removeTask(TaskHandle handle)
    {
        TaskEntry entry = (TaskEntry) handle;
        synchronized (this) {
            tasks.remove(entry.taskId());
            pendingTasks.remove(entry);
            belowGuarantee.remove(entry);
        }
        if (!entry.isDestroyed()) {
            entry.destroy();
        }
    }

    @Override
    public synchronized List<ListenableFuture<Void>> enqueueSplits(TaskHandle handle, boolean intermediate, List<? extends SplitRunner> splits)
    {
        checkArgument(!closed, "Executor is already closed");

        TaskEntry entry = (TaskEntry) handle;

        List<ListenableFuture<Void>> futures = new ArrayList<>();
        for (SplitRunner split : splits) {
            if (intermediate) {
                futures.add(entry.runSplit(split));
            }
            else {
                futures.add(entry.enqueueLeafSplit(split));
            }
        }

        updateSchedulability(entry);
        scheduleMoreLeafSplits();
        return futures;
    }

    @GuardedBy("this")
    private boolean scheduleLeafSplit(TaskEntry task)
    {
        // Count the driver before starting it. A split that finishes immediately reports back
        // from within dequeueAndRunLeafSplit, and that callback decrements this counter.
        runningLeafDrivers++;
        boolean scheduled = task.dequeueAndRunLeafSplit(leafSplitDoneCallback);
        if (!scheduled) {
            runningLeafDrivers--;
        }
        updateSchedulability(task);

        return scheduled;
    }

    private synchronized void leafSplitDone(TaskEntry task)
    {
        runningLeafDrivers--;
        updateSchedulability(task);
        scheduleMoreLeafSplits();
    }

    /**
     * Brings the scheduling sets back in sync with a task whose pending queue or running driver
     * count just changed. Membership in {@link #pendingTasks} is not reordered for a task that
     * is already present, so a task keeps its place in the round-robin.
     */
    @GuardedBy("this")
    private void updateSchedulability(TaskEntry task)
    {
        if (task.isDestroyed() || !task.hasPendingLeafSplits()) {
            pendingTasks.remove(task);
            belowGuarantee.remove(task);
            return;
        }

        pendingTasks.add(task);
        if (task.runningLeafSplits() < minDriversPerTask) {
            belowGuarantee.add(task);
        }
        else {
            belowGuarantee.remove(task);
        }
    }

    /**
     * Rebuilds the scheduling sets from the authoritative task map before scheduling. The sets
     * are maintained incrementally on the hot path, so this runs only on the periodic tick and
     * exists so that a missed update cannot leave a task with pending splits stuck forever.
     * It also picks up tasks whose target concurrency was raised since the last tick.
     */
    private synchronized void reconcileAndScheduleMoreLeafSplits()
    {
        for (TaskEntry task : tasks.values()) {
            updateSchedulability(task);
        }

        scheduleMoreLeafSplits();
    }

    /**
     * A split that finishes as soon as it is started reports back on the thread that started it,
     * which re-enters this method from inside the loops below. Rather than recursing, which is
     * unbounded when many splits finish immediately, record that another pass is needed and let
     * the in-progress call make it.
     */
    private synchronized void scheduleMoreLeafSplits()
    {
        if (scheduling) {
            rescheduleNeeded = true;
            return;
        }

        scheduling = true;
        try {
            do {
                rescheduleNeeded = false;
                doScheduleMoreLeafSplits();
            }
            while (rescheduleNeeded);
        }
        finally {
            scheduling = false;
        }
    }

    @GuardedBy("this")
    private void doScheduleMoreLeafSplits()
    {
        // Honor the per-task guarantee first, ignoring global capacity, as before.
        while (!belowGuarantee.isEmpty()) {
            TaskEntry task = belowGuarantee.iterator().next();
            if (!scheduleLeafSplit(task)) {
                // nothing left to schedule for this task; updateSchedulability already dropped it
                belowGuarantee.remove(task);
            }
        }

        // Then hand out spare global capacity round-robin. A task that is at its own concurrency
        // limit is rotated to the back and skipped; once every task has been skipped in turn
        // there is nothing left to place.
        int skipped = 0;
        while (runningLeafDrivers < targetGlobalLeafDrivers && skipped < pendingTasks.size()) {
            TaskEntry task = pendingTasks.iterator().next();

            if (task.runningLeafSplits() < min(task.targetConcurrency(), maxDriversPerTask) && scheduleLeafSplit(task)) {
                skipped = 0;
            }
            else {
                skipped++;
            }

            // rotate to the back so the next spare slot goes to a different task
            if (pendingTasks.remove(task)) {
                pendingTasks.add(task);
            }
        }
    }

    private void adjustConcurrency()
    {
        for (TaskEntry task : tasks.values()) {
            task.updateConcurrency();
        }
    }

    private void logDiagnostics()
    {
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Queue:\n");
            builder.append(scheduler.diagnostics().indent(4));

            builder.append("Query tasks:\n");
            for (TaskEntry task : tasks.values()) {
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

    @Managed(description = "Number of independent partitions of the split scheduling queue")
    public int getSchedulerShardCount()
    {
        return scheduler.getShardCount();
    }

    @Managed(description = "Unblocked splits that resumed without involving the scheduler thread")
    public long getBypassedResumeCount()
    {
        return scheduler.getBypassedResumeCount();
    }

    @Managed(description = "Unblocked splits that had to go through the scheduler thread to resume")
    public long getScheduledResumeCount()
    {
        return scheduler.getScheduledResumeCount();
    }
}
