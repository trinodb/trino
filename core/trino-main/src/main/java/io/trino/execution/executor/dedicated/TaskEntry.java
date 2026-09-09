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

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.executor.RunningSplitInfo;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.execution.executor.scheduler.Group;
import io.trino.execution.executor.scheduler.Schedulable;
import io.trino.execution.executor.scheduler.SchedulerContext;
import io.trino.spi.VersionEmbedder;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class TaskEntry
        implements TaskHandle
{
    private final TaskId taskId;
    private final Group group;
    private final FairScheduler scheduler;
    private final VersionEmbedder versionEmbedder;
    private final Tracer tracer;
    private final DoubleSupplier utilization;
    private final AtomicInteger nextSplitId = new AtomicInteger();
    private final Ticker ticker;
    private final long concurrencyAdjustmentIntervalNanos;
    private final Collection<RunningSplitInfo> runningSplitInfos;

    @GuardedBy("this")
    private final ConcurrencyController concurrency;

    @GuardedBy("this")
    private long lastConcurrencyAdjustmentNanos;

    private volatile boolean destroyed;

    @GuardedBy("this")
    private int runningLeafSplits;

    @GuardedBy("this")
    private final Queue<QueuedSplit> pending = new LinkedList<>();

    @GuardedBy("this")
    private final Set<SplitRunner> running = new HashSet<>();

    public TaskEntry(
            TaskId taskId,
            FairScheduler scheduler,
            VersionEmbedder versionEmbedder,
            Tracer tracer,
            int initialConcurrency,
            Duration concurrencyAdjustmentInterval,
            DoubleSupplier utilization,
            Collection<RunningSplitInfo> runningSplitInfos,
            Ticker ticker)
    {
        this.runningSplitInfos = requireNonNull(runningSplitInfos, "runningSplitInfos is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.utilization = requireNonNull(utilization, "utilization is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.concurrencyAdjustmentIntervalNanos = concurrencyAdjustmentInterval.roundTo(NANOSECONDS);

        this.group = scheduler.createGroup(taskId.toString());
        this.concurrency = new ConcurrencyController(initialConcurrency);
        this.lastConcurrencyAdjustmentNanos = ticker.read();
    }

    public TaskId taskId()
    {
        return taskId;
    }

    public synchronized void destroy()
    {
        if (destroyed) {
            return;
        }

        destroyed = true;

        // Close the splits before cancelling the group. Driver.close() records that it is the one
        // interrupting the driver thread, and that record is what lets Driver.process() treat the
        // interrupt as termination instead of failing the query with it. Cancelling first would
        // let the interrupt arrive before the record exists.
        for (SplitRunner split : running) {
            split.close();
        }
        running.clear();

        for (QueuedSplit split : pending) {
            split.split().close();
            split.done.set(null);
        }
        pending.clear();

        scheduler.removeGroup(group);
    }

    public synchronized ListenableFuture<Void> enqueueLeafSplit(SplitRunner split)
    {
        SettableFuture<Void> done = SettableFuture.create();
        pending.add(new QueuedSplit(split, done));
        return done;
    }

    /**
     * @return true if a split was scheduled; false if no splits are pending
     */
    public boolean dequeueAndRunLeafSplit(Consumer<TaskEntry> doneCallback)
    {
        QueuedSplit split;
        ListenableFuture<Void> done;
        synchronized (this) {
            split = pending.poll();
            if (split == null) {
                return false;
            }

            done = runSplit(split.split());
            // account for the driver before anything can observe it, since the split may already
            // have finished by the time the listener below is attached
            runningLeafSplits++;
        }

        // The listener runs inline when the split has already finished, and it calls back into
        // the executor. Attaching it outside the monitor keeps that from happening while this
        // lock is held, which would take the executor and task locks in the opposite order from
        // the scheduling path and would re-enter the executor with stale driver counts.
        done.addListener(() -> {
            leafSplitDone(split);
            doneCallback.accept(this);
        }, directExecutor());

        return true;
    }

    private synchronized void leafSplitDone(QueuedSplit split)
    {
        runningLeafSplits--;
        split.done().set(null);
    }

    public synchronized ListenableFuture<Void> runSplit(SplitRunner split)
    {
        int splitId = nextSplitId();
        ListenableFuture<Void> done = scheduler.submit(
                group,
                splitId,
                new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(taskId, splitId, split, tracer, runningSplitInfos, ticker)));
        done.addListener(() -> splitDone(split), directExecutor());
        running.add(split);

        return done;
    }

    private synchronized void splitDone(SplitRunner split)
    {
        split.close();
        running.remove(split);
    }

    private int nextSplitId()
    {
        return nextSplitId.incrementAndGet();
    }

    public synchronized int runningLeafSplits()
    {
        return runningLeafSplits;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    /**
     * Re-evaluates the target concurrency, at most once per configured adjustment interval. The
     * caller ticks more often than that so every task gets a chance to adjust close to its own
     * interval, but each task rate-limits itself here.
     */
    public synchronized void updateConcurrency()
    {
        long now = ticker.read();
        if (now - lastConcurrencyAdjustmentNanos < concurrencyAdjustmentIntervalNanos) {
            return;
        }
        lastConcurrencyAdjustmentNanos = now;

        concurrency.update(utilization.getAsDouble(), runningLeafSplits);
    }

    public synchronized int pendingLeafSplitCount()
    {
        return pending.size();
    }

    public synchronized int totalRunningSplits()
    {
        return running.size();
    }

    public synchronized boolean hasPendingLeafSplits()
    {
        return !pending.isEmpty();
    }

    public synchronized int targetConcurrency()
    {
        return concurrency.targetConcurrency();
    }

    private record QueuedSplit(SplitRunner split, SettableFuture<Void> done) {}

    private record VersionEmbedderBridge(VersionEmbedder versionEmbedder, Schedulable delegate)
            implements Schedulable
    {
        @Override
        public void run(SchedulerContext context)
        {
            Runnable adapter = () -> delegate.run(context);
            versionEmbedder.embedVersion(adapter).run();
        }
    }
}
