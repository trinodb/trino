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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.execution.executor.scheduler.Group;
import io.trino.execution.executor.scheduler.Schedulable;
import io.trino.execution.executor.scheduler.SchedulerContext;
import io.trino.spi.VersionEmbedder;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

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

    @GuardedBy("this")
    private final ConcurrencyController concurrency;

    private volatile boolean destroyed;

    @GuardedBy("this")
    private final Deque<QueuedSplit> pending = new ArrayDeque<>();

    @GuardedBy("this")
    private final Set<SplitRunner> running = new HashSet<>();

    @GuardedBy("this")
    private final Set<QueuedSplit> runningLeafSplits = new HashSet<>();

    public TaskEntry(TaskId taskId, FairScheduler scheduler, VersionEmbedder versionEmbedder, Tracer tracer, int initialConcurrency, DoubleSupplier utilization)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.utilization = requireNonNull(utilization, "utilization is null");

        this.group = scheduler.createGroup(taskId.toString());
        this.concurrency = new ConcurrencyController(initialConcurrency);
    }

    public TaskId taskId()
    {
        return taskId;
    }

    public void destroy()
    {
        List<SplitRunner> runningSplits;
        List<QueuedSplit> pendingSplits;
        List<QueuedSplit> claimedSplits;
        synchronized (this) {
            if (destroyed) {
                return;
            }

            destroyed = true;
            runningSplits = new ArrayList<>(running);
            pendingSplits = new ArrayList<>(pending);
            claimedSplits = new ArrayList<>(runningLeafSplits);
            running.clear();
            pending.clear();
            runningLeafSplits.clear();
        }

        // Complete futures before closing. Driver.close() can rethrow operator close failures, and
        // no such failure may strand another split or prevent its task from finishing.
        pendingSplits.forEach(split -> split.done().set(null));
        claimedSplits.forEach(split -> split.done().set(null));

        Throwable failure = null;
        try {
            scheduler.removeGroup(group);
        }
        catch (Throwable t) {
            failure = t;
        }

        for (SplitRunner split : runningSplits) {
            failure = closeSplit(split, failure);
        }
        for (QueuedSplit split : pendingSplits) {
            failure = closeSplit(split.split(), failure);
        }

        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }

    public ListenableFuture<Void> enqueueLeafSplit(SplitRunner split)
    {
        SettableFuture<Void> done = SettableFuture.create();
        synchronized (this) {
            if (!destroyed) {
                pending.addLast(new QueuedSplit(split, done));
                return done;
            }
        }

        // The task was removed concurrently with enqueueSplits. There is nobody left to claim the
        // split, so finish it immediately rather than leaving its future in a destroyed queue.
        done.set(null);
        split.close();
        return done;
    }

    /// Claim the next pending leaf split and account for it as running. The claimed split must be
    /// handed to [#startLeafSplit] to actually run; the two steps are separate so the caller can
    /// start the split without holding any lock.
    ///
    /// The task can be destroyed between claiming and starting. A production `SplitRunner`
    /// tolerates being started after close, and the scheduler rejects work for the removed group.
    ///
    /// @return null if no splits are pending or the task has been destroyed
    @Nullable
    public synchronized QueuedSplit claimLeafSplit()
    {
        if (destroyed) {
            return null;
        }

        QueuedSplit split = pending.poll();
        if (split == null) {
            return null;
        }

        runningLeafSplits.add(split);
        running.add(split.split());

        return split;
    }

    /// Start a split claimed via [#claimLeafSplit()]. Must be called without holding a lock.
    /// If this throws, the claim was not consumed and must be requeued via [#releaseLeafSplit].
    public void startLeafSplit(QueuedSplit split, Consumer<TaskEntry> doneCallback)
    {
        submit(split.split())
                .addListener(() -> {
                    finishLeafSplit(split, doneCallback);
                }, directExecutor());
    }

    /// Requeue a claimed split that could not be started. Thread creation can fail temporarily on
    /// an overloaded worker, and the periodic scheduling pass will retry it after capacity clears.
    public void releaseLeafSplit(QueuedSplit split)
    {
        boolean destroyed;
        synchronized (this) {
            runningLeafSplits.remove(split);
            running.remove(split.split());
            destroyed = this.destroyed;
            if (!destroyed) {
                pending.addFirst(split);
            }
        }

        if (destroyed) {
            split.done().set(null);
        }
    }

    private void finishLeafSplit(QueuedSplit split, Consumer<TaskEntry> doneCallback)
    {
        boolean close;
        synchronized (this) {
            runningLeafSplits.remove(split);
            close = running.remove(split.split());
        }

        // Complete the future and notify the executor even when Driver.close() fails.
        split.done().set(null);
        try {
            if (close) {
                split.split().close();
            }
        }
        finally {
            doneCallback.accept(this);
        }
    }

    public ListenableFuture<Void> runSplit(SplitRunner split)
    {
        boolean destroyed;
        synchronized (this) {
            destroyed = this.destroyed;
            if (!destroyed) {
                running.add(split);
            }
        }

        if (destroyed) {
            SettableFuture<Void> done = SettableFuture.create();
            done.set(null);
            split.close();
            return done;
        }

        try {
            ListenableFuture<Void> done = submit(split);
            done.addListener(() -> splitDone(split), directExecutor());
            return done;
        }
        catch (Throwable t) {
            try {
                splitDone(split);
            }
            catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
            throw t;
        }
    }

    /// Hand a split that is already accounted for in `running` to the scheduler. Must be called
    /// without holding a lock: the scheduler creates the thread that runs the split, which on a
    /// loaded worker is slow enough to stall every other operation on the lock.
    private ListenableFuture<Void> submit(SplitRunner split)
    {
        int splitId = nextSplitId();
        return scheduler.submit(
                group,
                splitId,
                new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(taskId, splitId, split, tracer)));
    }

    private void splitDone(SplitRunner split)
    {
        boolean close;
        synchronized (this) {
            close = running.remove(split);
        }
        if (close) {
            split.close();
        }
    }

    private int nextSplitId()
    {
        return nextSplitId.incrementAndGet();
    }

    public synchronized int runningLeafSplits()
    {
        return runningLeafSplits.size();
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    public synchronized void updateConcurrency()
    {
        concurrency.update(utilization.getAsDouble(), runningLeafSplits.size());
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

    record QueuedSplit(SplitRunner split, SettableFuture<Void> done) {}

    private static Throwable closeSplit(SplitRunner split, @Nullable Throwable failure)
    {
        try {
            split.close();
        }
        catch (Throwable t) {
            if (failure == null) {
                return t;
            }
            failure.addSuppressed(t);
        }
        return failure;
    }

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
