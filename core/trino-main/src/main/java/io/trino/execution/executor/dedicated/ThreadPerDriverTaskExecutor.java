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
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.RunningSplitInfo;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.execution.executor.scheduler.Group;
import io.trino.execution.executor.scheduler.Schedulable;
import io.trino.execution.executor.scheduler.SchedulerContext;
import io.trino.spi.VersionEmbedder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThreadPerDriverTaskExecutor
        implements TaskExecutor
{
    private final FairScheduler scheduler;
    private final Tracer tracer;
    private final VersionEmbedder versionEmbedder;
    private volatile boolean closed;

    @Inject
    public ThreadPerDriverTaskExecutor(TaskManagerConfig config, Tracer tracer, VersionEmbedder versionEmbedder)
    {
        this(tracer, versionEmbedder, new FairScheduler(config.getMaxWorkerThreads(), "SplitRunner-%d", Ticker.systemTicker()));
    }

    @VisibleForTesting
    public ThreadPerDriverTaskExecutor(Tracer tracer, VersionEmbedder versionEmbedder, FairScheduler scheduler)
    {
        this.scheduler = scheduler;
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
    }

    @PostConstruct
    @Override
    public synchronized void start()
    {
        scheduler.start();
    }

    @PreDestroy
    @Override
    public synchronized void stop()
    {
        closed = true;
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

        Group group = scheduler.createGroup(taskId.toString());
        return new TaskEntry(taskId, group);
    }

    @Override
    public synchronized void removeTask(TaskHandle handle)
    {
        TaskEntry entry = (TaskEntry) handle;

        if (!entry.isDestroyed()) {
            scheduler.removeGroup(entry.group());
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
            entry.addSplit(split);

            int splitId = entry.nextSplitId();
            ListenableFuture<Void> done = scheduler.submit(entry.group(), splitId, new VersionEmbedderBridge(versionEmbedder, new SplitProcessor(entry.taskId(), splitId, split, tracer)));
            done.addListener(split::close, directExecutor());
            futures.add(done);
        }

        return futures;
    }

    @Override
    public Set<TaskId> getStuckSplitTaskIds(Duration processingDurationThreshold, Predicate<RunningSplitInfo> filter)
    {
        // TODO
        return ImmutableSet.of();
    }

    private static class TaskEntry
            implements TaskHandle
    {
        private final TaskId taskId;
        private final Group group;
        private final AtomicInteger nextSplitId = new AtomicInteger();
        private volatile boolean destroyed;

        @GuardedBy("this")
        private Set<SplitRunner> splits = new HashSet<>();

        public TaskEntry(TaskId taskId, Group group)
        {
            this.taskId = taskId;
            this.group = group;
        }

        public TaskId taskId()
        {
            return taskId;
        }

        public Group group()
        {
            return group;
        }

        public synchronized void destroy()
        {
            destroyed = true;

            for (SplitRunner split : splits) {
                split.close();
            }
        }

        public synchronized void addSplit(SplitRunner split)
        {
            checkArgument(!destroyed, "Task already destroyed: %s", taskId);
            splits.add(split);
        }

        public int nextSplitId()
        {
            return nextSplitId.incrementAndGet();
        }

        @Override
        public boolean isDestroyed()
        {
            return destroyed;
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
