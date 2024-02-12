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
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

import static com.google.common.base.Preconditions.checkArgument;
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
    private int runningLeafSplits;

    @GuardedBy("this")
    private final Queue<QueuedSplit> pending = new LinkedList<>();

    @GuardedBy("this")
    private final Set<SplitRunner> running = new HashSet<>();

    public TaskEntry(TaskId taskId, FairScheduler scheduler, VersionEmbedder versionEmbedder, Tracer tracer, int initialConcurrency, int maxConcurrency, DoubleSupplier utilization)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.utilization = requireNonNull(utilization, "utilization is null");

        this.group = scheduler.createGroup(taskId.toString());
        this.concurrency = new ConcurrencyController(initialConcurrency, maxConcurrency);
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

        for (QueuedSplit split : pending) {
            split.split().close();
            split.done.set(null);
        }
        pending.clear();
    }

    public synchronized ListenableFuture<Void> addSplit(SplitRunner split, boolean intermediate)
    {
        checkArgument(!destroyed, "Task already destroyed: %s", taskId);

        if (intermediate) {
            return submit(split, false);
        }
        else {
            QueuedSplit queuedSplit = new QueuedSplit(split, SettableFuture.create());
            pending.add(queuedSplit);
            scheduleMoreLeafSplits();
            return queuedSplit.done();
        }
    }

    private ListenableFuture<Void> submit(SplitRunner split, boolean leaf)
    {
        running.add(split);
        int splitId = nextSplitId();
        ListenableFuture<Void> done = scheduler.submit(
                group,
                splitId,
                new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(taskId, splitId, split, tracer)));
        done.addListener(() -> splitDone(split, leaf), directExecutor());
        return done;
    }

    private void splitDone(SplitRunner split, boolean leaf)
    {
        split.close();
        synchronized (this) {
            running.remove(split);

            if (leaf) {
                runningLeafSplits--;
            }

            scheduleMoreLeafSplits();
        }
    }

    public synchronized void scheduleMoreLeafSplits()
    {
        int target = concurrency.targetConcurrency();

        while (runningLeafSplits < target && !pending.isEmpty()) {
            QueuedSplit entry = pending.poll();
            submit(entry.split(), true)
                    .addListener(() -> entry.done().set(null), directExecutor());
            runningLeafSplits++;
        }
    }

    private int nextSplitId()
    {
        return nextSplitId.incrementAndGet();
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    public synchronized void updateConcurrency()
    {
        concurrency.update(utilization.getAsDouble(), runningLeafSplits);
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
