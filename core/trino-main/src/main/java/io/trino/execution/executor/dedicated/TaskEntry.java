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

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.execution.executor.dedicated.SplitProcessor.NOOP_SPLIT_STATE_CHANGE_LISTENER;
import static io.trino.execution.executor.dedicated.SplitProcessor.SplitBlockedStateChangeListener;
import static java.util.Collections.newSetFromMap;
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
    private int runningLeafSplits;

    @GuardedBy("this")
    private int blockedLeafSplits;

    @GuardedBy("this")
    private final Set<SplitRunner> blockedLeafSplitSet = newSetFromMap(new IdentityHashMap<>());

    @GuardedBy("this")
    private final Queue<QueuedSplit> pending = new LinkedList<>();

    @GuardedBy("this")
    private final Set<SplitRunner> running = new HashSet<>();

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

    public synchronized void destroy()
    {
        scheduler.removeGroup(group);

        destroyed = true;

        for (SplitRunner split : running) {
            split.close();
        }
        running.clear();

        blockedLeafSplitSet.clear();
        runningLeafSplits = 0;
        blockedLeafSplits = 0;

        for (QueuedSplit split : pending) {
            split.split().close();
            split.done.set(null);
        }
        pending.clear();
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
    public synchronized boolean dequeueAndRunLeafSplit(Runnable doneCallback)
    {
        QueuedSplit split = pending.poll();
        if (split == null) {
            return false;
        }

        runSplit(split.split())
                .addListener(() -> {
                    leafSplitDone(split);
                    doneCallback.run();
                }, directExecutor());

        runningLeafSplits++;

        return true;
    }

    private synchronized void leafSplitDone(QueuedSplit split)
    {
        SplitRunner splitRunner = split.split();
        if (blockedLeafSplitSet.remove(splitRunner)) {
            blockedLeafSplits--;
        }
        runningLeafSplits--;
        split.done().set(null);
    }

    public synchronized ListenableFuture<Void> runSplit(SplitRunner split)
    {
        return runSplit(split, false);
    }

    private synchronized ListenableFuture<Void> runSplit(SplitRunner split, boolean trackLeafSplit)
    {
        SplitBlockedStateChangeListener listener = trackLeafSplit ? new SplitBlockedStateChangeListener()
        {
            @Override
            public void splitBlocked()
            {
                onSplitBlocked(split);
            }

            @Override
            public void splitRunning()
            {
                onSplitRunning(split);
            }
        } : NOOP_SPLIT_STATE_CHANGE_LISTENER;

        int splitId = nextSplitId();
        ListenableFuture<Void> done = scheduler.submit(
                group,
                splitId,
                new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(taskId, splitId, split, tracer, listener)));
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

    public synchronized int blockedLeafSplits()
    {
        return blockedLeafSplits;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    public synchronized void updateConcurrency()
    {
        concurrency.update(utilization.getAsDouble(), runningLeafSplits - blockedLeafSplits);
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

    private synchronized void onSplitBlocked(SplitRunner split)
    {
        if (blockedLeafSplitSet.add(split)) {
            blockedLeafSplits++;
        }
    }

    private synchronized void onSplitRunning(SplitRunner split)
    {
        if (blockedLeafSplitSet.remove(split)) {
            blockedLeafSplits--;
        }
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
