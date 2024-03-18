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
package io.trino.plugin.varada.dispatcher.warmup;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.warmup.export.WeGroupCloudExporterTask;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsWorkerTaskExecutorService;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerTaskExecutorService
{
    public static final String WORKER_TASK_EXECUTOR_STAT_GROUP = "worker-task-executor";
    private static final Logger logger = Logger.get(WorkerTaskExecutorService.class);
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final NativeConfiguration nativeConfiguration;
    private final VaradaStatsWorkerTaskExecutorService statsWorkerTaskExecutorService;
    private final Map<RowGroupKey, UUID> submittedRowGroups = new ConcurrentHashMap<>();
    private final SetMultimap<RowGroupKey, WorkerSubmittableTask> pendingTasks = Multimaps.newSetMultimap(new ConcurrentHashMap<>(), () -> {
        Comparator<WorkerSubmittableTask> c = Comparator.comparing(o -> (o.getPriority() + "." + o.getId()));
        return new TreeSet<>(c.reversed());
    });
    private final ReentrantLock lock = new ReentrantLock();
    private final ExecutorService prioritizeExecutorService;
    private final ExecutorService cloudExecutorService;
    private final ExecutorService proxyExecutorService;
    private final ScheduledExecutorService scheduledCloudExecutorService;
    private final int queueSize;

    @Inject
    public WorkerTaskExecutorService(
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager)
    {
        this.warmupDemoterConfiguration = requireNonNull(warmupDemoterConfiguration);
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        this.statsWorkerTaskExecutorService = metricsManager.registerMetric(new VaradaStatsWorkerTaskExecutorService(WORKER_TASK_EXECUTOR_STAT_GROUP));
        this.queueSize = warmupDemoterConfiguration.getTasksExecutorQueueSize();
        prioritizeExecutorService = getPrioritizeExecutorService();
        cloudExecutorService = getCloudExecutorService();
        scheduledCloudExecutorService = getScheduledCloudExecutorService();
        proxyExecutorService = getProxyExecutorService();
    }

    private ExecutorService getPrioritizeExecutorService()
    {
        int poolSize = warmupDemoterConfiguration.getPrioritizeExecutorPollSize();
        long keepAliveTTL = Math.min(warmupDemoterConfiguration.getTasksExecutorKeepAliveTtl(), 1000);

        return new ThreadPoolExecutor(poolSize, poolSize,
                keepAliveTTL, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize));
    }

    private ExecutorService getCloudExecutorService()
    {
        int poolSize = warmupDemoterConfiguration.getCloudExecutorPollSize();
        long keepAliveTTL = Math.min(warmupDemoterConfiguration.getTasksExecutorKeepAliveTtl(), 1000);
        BlockingQueue<Runnable> blockingQueue = new PriorityBlockingQueue<>(
                queueSize,
                Comparator.comparingDouble(x -> ((WorkerSubmittableTask) x).getPriority()).reversed());
        return new ThreadPoolExecutor(poolSize, poolSize,
                keepAliveTTL, TimeUnit.MILLISECONDS,
                blockingQueue);
    }

    private ScheduledExecutorService getScheduledCloudExecutorService()
    {
        int poolSize = warmupDemoterConfiguration.getCloudExecutorPollSize();
        return new ScheduledThreadPoolExecutor(poolSize);
    }

    private ExecutorService getProxyExecutorService()
    {
        int poolSize = nativeConfiguration.getTaskMaxWorkerThreads();
        long keepAliveTTL = Math.min(warmupDemoterConfiguration.getTasksExecutorKeepAliveTtl(), 1000);
        BlockingQueue<Runnable> blockingQueue = new PriorityBlockingQueue<>(
                queueSize,
                Comparator.comparingDouble(x -> ((WorkerSubmittableTask) x).getPriority()).reversed());

        return new ThreadPoolExecutor(poolSize, poolSize,
                keepAliveTTL, TimeUnit.MILLISECONDS,
                blockingQueue);
    }

    public SubmissionResult submitTask(WorkerSubmittableTask task, boolean allowConflicts)
    {
        SubmissionResult ret = SubmissionResult.SCHEDULED;
        lock.lock();
        try {
            if (submittedRowGroups.size() < queueSize) {
                UUID newTaskId = task.getId();
                try {
                    UUID savedTaskId = submittedRowGroups.computeIfAbsent(task.getRowGroupKey(), k -> {
                        statsWorkerTaskExecutorService.inctask_scheduled();
                        task.taskScheduled();
                        executeTask(task);
                        return task.getId();
                    });
                    if (!savedTaskId.equals(newTaskId)) {
                        if (allowConflicts) {
                            statsWorkerTaskExecutorService.inctask_pended();
                            pendingTasks.put(task.getRowGroupKey(), task);
                            ret = SubmissionResult.CONFLICT;
                        }
                        else {
                            ret = SubmissionResult.REJECTED;
                        }
                    }
                }
                catch (RejectedExecutionException e) {
                    logger.warn("too many elements in the warming queue dropping key: %s", task.getRowGroupKey());
                    statsWorkerTaskExecutorService.inctask_skipped_due_queue_size();
                    ret = SubmissionResult.REJECTED;
                }
            }
            else {
                statsWorkerTaskExecutorService.inctask_skipped_due_queue_size();
                ret = SubmissionResult.REJECTED;
            }
        }
        finally {
            lock.unlock();
        }
        return ret;
    }

    private void executeTask(WorkerSubmittableTask task)
    {
        if (task instanceof PrioritizeTask) {
            prioritizeExecutorService.execute(task);
        }
        else if (task instanceof ImportExecutionTask || task instanceof WeGroupCloudExporterTask) {
            cloudExecutorService.execute(task);
        }
        else if (task instanceof ProxyExecutionTask) {
            proxyExecutorService.execute(task);
        }
        else if (task instanceof WarpCacheTask) {
            proxyExecutorService.execute(task);
        }
    }

    public void taskFinished(RowGroupKey rowGroupKey)
    {
        lock.lock();
        try {
            submittedRowGroups.remove(rowGroupKey);
            Set<WorkerSubmittableTask> pendingRowGroupTasks = pendingTasks.get(rowGroupKey);
            if (!pendingRowGroupTasks.isEmpty()) {
                Iterator<WorkerSubmittableTask> iterator = pendingRowGroupTasks.iterator();
                WorkerSubmittableTask workerSubmittableTask = iterator.next();
                iterator.remove();
                statsWorkerTaskExecutorService.inctask_resubmitted();
                submitTask(workerSubmittableTask, true);
            }
            statsWorkerTaskExecutorService.inctask_finished();
        }
        finally {
            lock.unlock();
        }
    }

    public void delaySubmit(long delayInSeconds, WorkerSubmittableTask task, Consumer<WorkerSubmittableTask> conflictCallback)
    {
        if (delayInSeconds > 0) {
            statsWorkerTaskExecutorService.inctask_delayed();
            DelayedTask delayedTask = new DelayedTask(this, task, conflictCallback);
            ScheduledFuture<?> unused = scheduledCloudExecutorService.schedule(delayedTask, delayInSeconds, TimeUnit.SECONDS);
        }
        else {
            SubmissionResult submissionResult = submitTask(task, true);
            if ((submissionResult == SubmissionResult.CONFLICT) && (conflictCallback != null)) {
                try {
                    conflictCallback.accept(task);
                }
                catch (Exception e) {
                    logger.warn("failed to call conflict callback for task %s", task.getRowGroupKey());
                }
            }
        }
    }

    public enum SubmissionResult
    {
        SCHEDULED,
        REJECTED,
        CONFLICT
    }

    public enum TaskExecutionType
    {
        CLASSIFY(0),
        PROXY(0),
        //import end export tasks run in the same executor, priority will impact which task will be handled first.
        //import should be handled before export
        IMPORT(10),
        EXPORT(0),
        CACHE(0);

        private final int priority;

        TaskExecutionType(int priority)
        {
            this.priority = priority;
        }

        public int getPriority()
        {
            return priority;
        }
    }

    private record DelayedTask(
            @SuppressWarnings("unused") WorkerTaskExecutorService workerTaskExecutorService,
            @SuppressWarnings("unused") WorkerSubmittableTask task,
            @SuppressWarnings("unused") Consumer<WorkerSubmittableTask> conflictCallback)
            implements Runnable
    {
        @Override
        public void run()
        {
            SubmissionResult submissionResult = workerTaskExecutorService().submitTask(task(), true);
            if ((submissionResult == SubmissionResult.CONFLICT) && (conflictCallback != null)) {
                try {
                    conflictCallback.accept(task());
                }
                catch (Exception e) {
                    logger.warn("failed to call conflict callback for task %s", task().getRowGroupKey());
                }
            }
        }
    }
}
