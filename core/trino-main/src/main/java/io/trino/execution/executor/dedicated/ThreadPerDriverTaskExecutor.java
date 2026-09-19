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
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThreadPerDriverTaskExecutor
        implements TaskExecutor
{
    private static final Logger LOG = Logger.get(ThreadPerDriverTaskExecutor.class);
    private static final long FAILURE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    private final FairScheduler scheduler;
    private final Tracer tracer;
    private final VersionEmbedder versionEmbedder;
    private final int targetGlobalLeafDrivers;
    private final int minDriversPerTask;
    private final int maxDriversPerTask;
    private final ScheduledThreadPoolExecutor backgroundTasks = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("task-executor-scheduler-%s"));

    private final Map<TaskId, TaskEntry> tasks = new ConcurrentHashMap<>();

    private volatile boolean closed;

    @GuardedBy("this")
    private int runningLeafDrivers;

    private final AtomicBoolean schedulingLeafSplits = new AtomicBoolean();
    private final AtomicBoolean rescheduleLeafSplits = new AtomicBoolean();

    // Do not inline this field to avoid creating lambdas that cannot be cached by JVM.
    private final Consumer<TaskEntry> leafSplitDoneCallback = this::leafSplitDone;
    private final FailureLogger schedulingFailureLogger = new FailureLogger("Error scheduling leaf splits");
    private final Runnable scheduleMoreLeafSplitsQuietly = maintenance(this::scheduleMoreLeafSplits, schedulingFailureLogger);

    @Inject
    public ThreadPerDriverTaskExecutor(TaskManagerConfig config, Tracer tracer, VersionEmbedder versionEmbedder)
    {
        this(tracer,
                versionEmbedder,
                new FairScheduler(
                        config.getMaxWorkerThreads(),
                        "SplitRunner-%d",
                        Ticker.systemTicker(),
                        config.isThreadPerDriverSchedulerVirtualThreadsEnabled()),
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
        if (closed) {
            return;
        }
        closed = true;

        Throwable failure = null;
        for (TaskEntry task : tasks.values()) {
            try {
                task.destroy();
            }
            catch (Throwable t) {
                failure = addFailure(failure, t);
            }
        }
        backgroundTasks.shutdownNow();
        try {
            scheduler.close();
        }
        catch (Throwable t) {
            failure = addFailure(failure, t);
        }

        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
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
        tasks.remove(entry.taskId(), entry);
        entry.destroy();
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

        scheduleMoreLeafSplitsQuietly.run();
        return futures;
    }

    private void leafSplitDone(TaskEntry task)
    {
        ClaimedLeafSplit replacement;
        synchronized (this) {
            runningLeafDrivers--;
            replacement = claimLeafSplitForTask(task);
        }

        if (replacement != null) {
            startClaimedSplits(ImmutableList.of(replacement));
            return;
        }

        // When the task drains, let another task use the freed slot immediately. This runs the
        // global pass once per task drain instead of once per split completion.
        if (!task.hasPendingLeafSplits()) {
            scheduleMoreLeafSplitsQuietly.run();
        }
    }

    @GuardedBy("this")
    @Nullable
    private ClaimedLeafSplit claimLeafSplitForTask(TaskEntry task)
    {
        if (closed) {
            return null;
        }

        int taskRunning = task.runningLeafSplits();
        if (taskRunning >= minDriversPerTask &&
                (runningLeafDrivers >= targetGlobalLeafDrivers || taskRunning >= min(task.targetConcurrency(), maxDriversPerTask))) {
            return null;
        }

        List<ClaimedLeafSplit> claimed = new ArrayList<>(1);
        if (!claimLeafSplit(task, claimed)) {
            return null;
        }
        return claimed.getFirst();
    }

    @VisibleForTesting
    void scheduleMoreLeafSplits()
    {
        rescheduleLeafSplits.set(true);
        boolean retry = true;
        while (true) {
            if (!schedulingLeafSplits.compareAndSet(false, true)) {
                return;
            }

            try {
                do {
                    rescheduleLeafSplits.set(false);
                    retry = startClaimedSplits(claimMoreLeafSplits());
                }
                while (retry && rescheduleLeafSplits.get());
            }
            finally {
                schedulingLeafSplits.set(false);
            }

            // Close the race where another caller requested a pass after the last flag check but
            // before the gate was released.
            if (!retry || !rescheduleLeafSplits.get()) {
                return;
            }
        }
    }

    private boolean startClaimedSplits(List<ClaimedLeafSplit> claimed)
    {
        // Start the splits outside all locks. Thread creation is slow on an overloaded worker.
        for (int i = 0; i < claimed.size(); i++) {
            ClaimedLeafSplit split = claimed.get(i);
            try {
                split.task().startLeafSplit(split.split(), leafSplitDoneCallback);
            }
            catch (Throwable e) {
                // Claims before this one have listeners and remain running. Requeue this claim and
                // every unstarted claim behind it; the periodic pass retries them later.
                releaseClaims(claimed.subList(i, claimed.size()));
                schedulingFailureLogger.log(e);
                return false;
            }
        }
        return true;
    }

    private void releaseClaims(List<ClaimedLeafSplit> claimed)
    {
        synchronized (this) {
            runningLeafDrivers -= claimed.size();
        }

        // Each task requeues at the head, so release in reverse to preserve claim order.
        for (int i = claimed.size() - 1; i >= 0; i--) {
            ClaimedLeafSplit split = claimed.get(i);
            split.task().releaseLeafSplit(split.split());
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

        // Claim additional drivers up to the target global leaf drivers. Iterate in rounds to
        // retain the previous round-robin behavior without copying the task map into a queue.
        int target = targetGlobalLeafDrivers - runningLeafDrivers;
        boolean progress = true;
        while (target > 0 && progress) {
            progress = false;
            for (TaskEntry task : tasks.values()) {
                if (target == 0) {
                    break;
                }
                if (task.runningLeafSplits() < min(task.targetConcurrency(), maxDriversPerTask) && claimLeafSplit(task, claimed)) {
                    target--;
                    progress = true;
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
        return maintenance(task, new FailureLogger(errorMessage));
    }

    private static Runnable maintenance(Runnable task, FailureLogger failureLogger)
    {
        return () -> {
            try {
                task.run();
            }
            catch (Throwable e) {
                failureLogger.log(e);
            }
        };
    }

    private static Throwable addFailure(Throwable failure, Throwable newFailure)
    {
        if (failure == null) {
            return newFailure;
        }
        failure.addSuppressed(newFailure);
        return failure;
    }

    private static final class FailureLogger
    {
        private final String message;
        private final AtomicLong lastLogNanos = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong suppressedFailures = new AtomicLong();

        private FailureLogger(String message)
        {
            this.message = requireNonNull(message, "message is null");
        }

        public void log(Throwable failure)
        {
            long now = System.nanoTime();
            while (true) {
                long last = lastLogNanos.get();
                if (last != Long.MIN_VALUE && now - last < FAILURE_LOG_INTERVAL_NANOS) {
                    suppressedFailures.incrementAndGet();
                    return;
                }
                if (lastLogNanos.compareAndSet(last, now)) {
                    break;
                }
            }

            long suppressed = suppressedFailures.getAndSet(0);
            if (suppressed == 0) {
                LOG.warn(failure, "%s", message);
            }
            else {
                LOG.warn(failure, "%s (%s similar failures suppressed)", message, suppressed);
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
    public int getTasks()
    {
        return tasks.size();
    }

    @Managed
    public int getTotalRunningSplits()
    {
        return tasks.values().stream()
                .mapToInt(TaskEntry::totalRunningSplits)
                .sum();
    }

    @Managed
    public int getTotalRunningLeafSplits()
    {
        return tasks.values().stream()
                .mapToInt(TaskEntry::runningLeafSplits)
                .sum();
    }

    @Managed
    public int getTotalPendingLeafSplits()
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
