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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

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
    private final Function<TaskId, Optional<Group>> producerTaskGroup;
    private final AtomicInteger nextSplitId = new AtomicInteger();

    @GuardedBy("this")
    private final ConcurrencyController concurrency;

    // One scheduling group per pipeline, nested under the task group, so drivers are scheduled
    // fairly across pipelines and a pipeline can be donated priority as a whole.
    @GuardedBy("this")
    private final Map<Integer, Group> pipelineGroups = new HashMap<>();

    private volatile boolean destroyed;

    @GuardedBy("this")
    private int runningLeafSplits;

    @GuardedBy("this")
    private final Queue<QueuedSplit> pending = new LinkedList<>();

    @GuardedBy("this")
    private final Set<SplitRunner> running = new HashSet<>();

    public TaskEntry(TaskId taskId, FairScheduler scheduler, VersionEmbedder versionEmbedder, Tracer tracer, int initialConcurrency, DoubleSupplier utilization, Function<TaskId, Optional<Group>> producerTaskGroup)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.utilization = requireNonNull(utilization, "utilization is null");
        this.producerTaskGroup = requireNonNull(producerTaskGroup, "producerTaskGroup is null");

        this.group = scheduler.createGroup(taskId.toString());
        this.concurrency = new ConcurrencyController(initialConcurrency);
    }

    /// The scheduling group a consumer donates to when it depends on this task's output over an
    /// exchange, or empty once the task is being torn down (its group subtree is gone). Boosting the
    /// task group pulls every pipeline feeding this task's output ahead of fair order.
    public synchronized Optional<Group> donationGroup()
    {
        return destroyed ? Optional.empty() : Optional.of(group);
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
        runningLeafSplits--;
        split.done().set(null);
    }

    public synchronized ListenableFuture<Void> runSplit(SplitRunner split)
    {
        int splitId = nextSplitId();
        ListenableFuture<Void> done = scheduler.submit(
                pipelineGroup(split.getPipelineId()),
                splitId,
                new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(taskId, splitId, split, tracer, this::pipelineGroupForDonation, producerTaskGroup)));
        done.addListener(() -> splitDone(split), directExecutor());
        running.add(split);

        return done;
    }

    @GuardedBy("this")
    private Group pipelineGroup(int pipelineId)
    {
        return pipelineGroups.computeIfAbsent(pipelineId, id -> scheduler.createGroup(group, "pipeline-" + id));
    }

    /// The scheduling group of a pipeline that already has scheduled splits, or empty.
    public synchronized Optional<Group> pipelineGroupIfPresent(int pipelineId)
    {
        return Optional.ofNullable(pipelineGroups.get(pipelineId));
    }

    /// The scheduling group of the producer pipeline a blocked split donates priority to, creating it
    /// on demand if that pipeline has not scheduled a split yet — otherwise a probe that blocks before
    /// its build pipeline is scheduled would silently get no donation, the case where it matters most.
    /// Creating it early is harmless: an empty group is blocked, so it stays out of fair order until
    /// its splits arrive, at which point they inherit any boost already applied to the group. Empty
    /// once the task is being torn down, since its group subtree is gone.
    public synchronized Optional<Group> pipelineGroupForDonation(int pipelineId)
    {
        if (destroyed) {
            return Optional.empty();
        }
        return Optional.of(pipelineGroup(pipelineId));
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

    public synchronized void updateConcurrency()
    {
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
