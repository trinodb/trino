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
package io.trino.execution.scheduler;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputBuffers;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultCoordinatorTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultTaskMemory;
import static io.trino.SystemSessionProperties.getRetryDelayScaleFactor;
import static io.trino.SystemSessionProperties.getRetryInitialDelay;
import static io.trino.SystemSessionProperties.getRetryMaxDelay;
import static io.trino.execution.scheduler.ErrorCodes.isOutOfMemoryError;
import static io.trino.execution.scheduler.Exchanges.getAllSourceHandles;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.util.Failures.toFailure;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FaultTolerantStageScheduler
{
    private static final Logger log = Logger.get(FaultTolerantStageScheduler.class);

    private final Session session;
    private final SqlStage stage;
    private final FailureDetector failureDetector;
    private final TaskSourceFactory taskSourceFactory;
    private final NodeAllocator nodeAllocator;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final PartitionMemoryEstimator partitionMemoryEstimator;
    private final TaskExecutionStats taskExecutionStats;
    private final int maxRetryAttemptsPerTask;
    private final int maxTasksWaitingForNodePerStage;

    private final Exchange sinkExchange;
    private final FaultTolerantPartitioningScheme sinkPartitioningScheme;

    private final Map<PlanFragmentId, FaultTolerantStageScheduler> sourceSchedulers;
    private final Map<PlanFragmentId, Exchange> sourceExchanges;
    private final FaultTolerantPartitioningScheme sourcePartitioningScheme;

    private final DelayedFutureCompletor futureCompletor;

    @GuardedBy("this")
    private ListenableFuture<Void> blocked = immediateVoidFuture();

    @GuardedBy("this")
    private ListenableFuture<Void> tasksPopulatedFuture = immediateVoidFuture();

    @GuardedBy("this")
    private SettableFuture<Void> taskFinishedFuture;

    private final Duration minRetryDelay;
    private final Duration maxRetryDelay;
    private final double retryDelayScaleFactor;

    @GuardedBy("this")
    private Optional<Duration> delaySchedulingDuration = Optional.empty();
    @GuardedBy("this")
    private final Stopwatch delayStopwatch;
    @GuardedBy("this")
    private SettableFuture<Void> delaySchedulingFuture;

    @GuardedBy("this")
    private TaskSource taskSource;
    @GuardedBy("this")
    private final Map<Integer, ExchangeSinkHandle> partitionToExchangeSinkHandleMap = new HashMap<>();
    @GuardedBy("this")
    private final Multimap<Integer, RemoteTask> partitionToRemoteTaskMap = ArrayListMultimap.create();
    @GuardedBy("this")
    private final Map<TaskId, RemoteTask> runningTasks = new HashMap<>();
    @GuardedBy("this")
    private final Map<TaskId, NodeAllocator.NodeLease> runningNodes = new HashMap<>();
    @GuardedBy("this")
    private final Set<Integer> allPartitions = new HashSet<>();
    @GuardedBy("this")
    private boolean noMorePartitions;
    @GuardedBy("this")
    private final Queue<Integer> queuedPartitions = new ArrayDeque<>();
    @GuardedBy("this")
    private final Queue<PendingPartition> pendingPartitions = new ArrayDeque<>();
    @GuardedBy("this")
    private final Map<Integer, Integer> finishedPartitions = new HashMap<>();
    @GuardedBy("this")
    private final AtomicInteger remainingRetryAttemptsOverall;
    @GuardedBy("this")
    private final Map<Integer, Integer> remainingAttemptsPerTask = new HashMap<>();
    @GuardedBy("this")
    private final Map<Integer, MemoryRequirements> partitionMemoryRequirements = new HashMap<>();
    @GuardedBy("this")
    private Multimap<PlanNodeId, Split> outputSelectorSplits;

    private final DynamicFilterService dynamicFilterService;

    @GuardedBy("this")
    private Throwable failure;
    @GuardedBy("this")
    private boolean closed;

    public FaultTolerantStageScheduler(
            Session session,
            SqlStage stage,
            FailureDetector failureDetector,
            TaskSourceFactory taskSourceFactory,
            NodeAllocator nodeAllocator,
            TaskDescriptorStorage taskDescriptorStorage,
            PartitionMemoryEstimator partitionMemoryEstimator,
            TaskExecutionStats taskExecutionStats,
            DelayedFutureCompletor futureCompletor,
            Ticker ticker,
            Exchange sinkExchange,
            FaultTolerantPartitioningScheme sinkPartitioningScheme,
            Map<PlanFragmentId, FaultTolerantStageScheduler> sourceSchedulers,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            AtomicInteger remainingRetryAttemptsOverall,
            int taskRetryAttemptsPerTask,
            int maxTasksWaitingForNodePerStage,
            DynamicFilterService dynamicFilterService)
    {
        this.session = requireNonNull(session, "session is null");
        this.stage = requireNonNull(stage, "stage is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.partitionMemoryEstimator = requireNonNull(partitionMemoryEstimator, "partitionMemoryEstimator is null");
        this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
        this.futureCompletor = requireNonNull(futureCompletor, "futureCompletor is null");
        this.sinkExchange = requireNonNull(sinkExchange, "sinkExchange is null");
        this.sinkPartitioningScheme = requireNonNull(sinkPartitioningScheme, "sinkPartitioningScheme is null");
        Set<PlanFragmentId> sourceFragments = stage.getFragment().getRemoteSourceNodes().stream()
                .flatMap(remoteSource -> remoteSource.getSourceFragmentIds().stream())
                .collect(toImmutableSet());
        requireNonNull(sourceSchedulers, "sourceSchedulers is null");
        checkArgument(sourceSchedulers.keySet().containsAll(sourceFragments), "sourceSchedulers map is incomplete");
        this.sourceSchedulers = ImmutableMap.copyOf(sourceSchedulers);
        requireNonNull(sourceExchanges, "sourceExchanges is null");
        checkArgument(sourceExchanges.keySet().containsAll(sourceFragments), "sourceExchanges map is incomplete");
        this.sourceExchanges = ImmutableMap.copyOf(sourceExchanges);
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        this.remainingRetryAttemptsOverall = requireNonNull(remainingRetryAttemptsOverall, "remainingRetryAttemptsOverall is null");
        this.maxRetryAttemptsPerTask = taskRetryAttemptsPerTask;
        this.maxTasksWaitingForNodePerStage = maxTasksWaitingForNodePerStage;
        this.minRetryDelay = Duration.ofMillis(getRetryInitialDelay(session).toMillis());
        this.maxRetryDelay = Duration.ofMillis(getRetryMaxDelay(session).toMillis());
        this.retryDelayScaleFactor = getRetryDelayScaleFactor(session);
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.delayStopwatch = Stopwatch.createUnstarted(ticker);
    }

    public StageId getStageId()
    {
        return stage.getStageId();
    }

    public synchronized ListenableFuture<Void> isBlocked()
    {
        return nonCancellationPropagating(blocked);
    }

    public synchronized void schedule()
            throws Exception
    {
        if (failure != null) {
            propagateIfPossible(failure, Exception.class);
            throw new RuntimeException(failure);
        }

        if (closed) {
            return;
        }

        if (isFinished()) {
            return;
        }

        if (!blocked.isDone()) {
            return;
        }

        if (delaySchedulingFuture != null && !delaySchedulingFuture.isDone()) {
            // let's wait a bit more
            blocked = delaySchedulingFuture;
            return;
        }

        if (taskSource == null) {
            Map<PlanFragmentId, ListenableFuture<List<ExchangeSourceHandle>>> sourceHandles = sourceExchanges.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> getAllSourceHandles(entry.getValue().getSourceHandles())));

            List<ListenableFuture<List<ExchangeSourceHandle>>> blockedFutures = sourceHandles.values().stream()
                    .filter(future -> !future.isDone())
                    .collect(toImmutableList());

            if (!blockedFutures.isEmpty()) {
                blocked = asVoid(allAsList(blockedFutures));
                return;
            }

            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSources = sourceHandles.entrySet().stream()
                    .collect(flatteningToImmutableListMultimap(Map.Entry::getKey, entry -> getFutureValue(entry.getValue()).stream()));

            taskSource = taskSourceFactory.create(
                    session,
                    stage.getFragment(),
                    exchangeSources,
                    stage::recordGetSplitTime,
                    sourcePartitioningScheme);
        }

        while (!pendingPartitions.isEmpty() || !queuedPartitions.isEmpty() || !taskSource.isFinished()) {
            while (queuedPartitions.isEmpty() && pendingPartitions.size() < maxTasksWaitingForNodePerStage && !taskSource.isFinished()) {
                tasksPopulatedFuture = Futures.transform(
                        taskSource.getMoreTasks(),
                        tasks -> {
                            synchronized (this) {
                                for (TaskDescriptor task : tasks) {
                                    queuedPartitions.add(task.getPartitionId());
                                    allPartitions.add(task.getPartitionId());
                                    taskDescriptorStorage.put(stage.getStageId(), task);
                                    ExchangeSinkHandle exchangeSinkHandle = sinkExchange.addSink(task.getPartitionId());
                                    partitionToExchangeSinkHandleMap.put(task.getPartitionId(), exchangeSinkHandle);
                                }
                                if (taskSource.isFinished()) {
                                    dynamicFilterService.stageCannotScheduleMoreTasks(stage.getStageId(), 0, allPartitions.size());
                                    sinkExchange.noMoreSinks();
                                    noMorePartitions = true;
                                }
                                if (noMorePartitions && finishedPartitions.keySet().containsAll(allPartitions)) {
                                    sinkExchange.allRequiredSinksFinished();
                                }
                                return null;
                            }
                        },
                        directExecutor());
                if (!tasksPopulatedFuture.isDone()) {
                    blocked = tasksPopulatedFuture;
                    return;
                }
            }

            Iterator<PendingPartition> pendingPartitionsIterator = pendingPartitions.iterator();
            boolean startedTask = false;
            while (pendingPartitionsIterator.hasNext()) {
                PendingPartition pendingPartition = pendingPartitionsIterator.next();
                if (pendingPartition.getNodeLease().getNode().isDone()) {
                    MemoryRequirements memoryRequirements = partitionMemoryRequirements.get(pendingPartition.getPartition());
                    verify(memoryRequirements != null, "no entry for %s.%s in partitionMemoryRequirements", stage.getStageId(), pendingPartition.getPartition());
                    startTask(pendingPartition.getPartition(), pendingPartition.getNodeLease(), memoryRequirements);
                    startedTask = true;
                    pendingPartitionsIterator.remove();
                }
            }

            if (!startedTask && (queuedPartitions.isEmpty() || pendingPartitions.size() >= maxTasksWaitingForNodePerStage)) {
                break;
            }

            while (pendingPartitions.size() < maxTasksWaitingForNodePerStage && !queuedPartitions.isEmpty()) {
                int partition = queuedPartitions.poll();
                Optional<TaskDescriptor> taskDescriptorOptional = taskDescriptorStorage.get(stage.getStageId(), partition);
                if (taskDescriptorOptional.isEmpty()) {
                    // query has been terminated
                    return;
                }
                TaskDescriptor taskDescriptor = taskDescriptorOptional.get();
                DataSize defaultTaskMemory = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION) ?
                        getFaultTolerantExecutionDefaultCoordinatorTaskMemory(session) :
                        getFaultTolerantExecutionDefaultTaskMemory(session);
                MemoryRequirements memoryRequirements = partitionMemoryRequirements.computeIfAbsent(partition, ignored -> partitionMemoryEstimator.getInitialMemoryRequirements(session, defaultTaskMemory));
                log.debug("Computed initial memory requirements for task from stage %s; requirements=%s; estimator=%s", stage.getStageId(), memoryRequirements, partitionMemoryEstimator);
                NodeRequirements nodeRequirements = taskDescriptor.getNodeRequirements();
                NodeAllocator.NodeLease nodeLease = nodeAllocator.acquire(nodeRequirements, memoryRequirements.getRequiredMemory());

                pendingPartitions.add(new PendingPartition(partition, nodeLease));
            }
        }

        List<ListenableFuture<?>> futures = new ArrayList<>();
        if (taskFinishedFuture != null && !taskFinishedFuture.isDone()) {
            futures.add(taskFinishedFuture);
        }
        for (PendingPartition pendingPartition : pendingPartitions) {
            futures.add(pendingPartition.getNodeLease().getNode());
        }
        if (!futures.isEmpty()) {
            blocked = asVoid(MoreFutures.whenAnyComplete(futures));
        }
    }

    @GuardedBy("this")
    private void startTask(int partition, NodeAllocator.NodeLease nodeLease, MemoryRequirements memoryRequirements)
    {
        Optional<TaskDescriptor> taskDescriptorOptional = taskDescriptorStorage.get(stage.getStageId(), partition);
        if (taskDescriptorOptional.isEmpty()) {
            // query has been terminated
            return;
        }
        TaskDescriptor taskDescriptor = taskDescriptorOptional.get();

        InternalNode node = getFutureValue(nodeLease.getNode());

        int attemptId = getNextAttemptIdForPartition(partition);

        ExchangeSinkHandle sinkHandle = partitionToExchangeSinkHandleMap.get(partition);
        ExchangeSinkInstanceHandle exchangeSinkInstanceHandle = sinkExchange.instantiateSink(sinkHandle, attemptId);
        OutputBuffers outputBuffers = SpoolingOutputBuffers.createInitial(exchangeSinkInstanceHandle, sinkPartitioningScheme.getPartitionCount());

        Set<PlanNodeId> allSourcePlanNodeIds = ImmutableSet.<PlanNodeId>builder()
                .addAll(stage.getFragment().getPartitionedSources())
                .addAll(stage.getFragment()
                        .getRemoteSourceNodes().stream()
                        .map(RemoteSourceNode::getId)
                        .iterator())
                .build();

        createOutputSelectorSplitsIfNecessary();

        RemoteTask task = stage.createTask(
                node,
                partition,
                attemptId,
                sinkPartitioningScheme.getBucketToPartitionMap(),
                outputBuffers,
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .putAll(outputSelectorSplits)
                        .putAll(taskDescriptor.getSplits())
                        .build(),
                allSourcePlanNodeIds,
                Optional.of(memoryRequirements.getRequiredMemory())).orElseThrow(() -> new VerifyException("stage execution is expected to be active"));

        nodeLease.attachTaskId(task.getTaskId());
        partitionToRemoteTaskMap.put(partition, task);
        runningTasks.put(task.getTaskId(), task);
        runningNodes.put(task.getTaskId(), nodeLease);

        if (taskFinishedFuture == null) {
            taskFinishedFuture = SettableFuture.create();
        }

        task.addStateChangeListener(taskStatus -> updateTaskStatus(taskStatus, sinkHandle));
        task.addFinalTaskInfoListener(taskExecutionStats::update);
        task.start();
    }

    @GuardedBy("this")
    private void createOutputSelectorSplitsIfNecessary()
    {
        if (outputSelectorSplits != null) {
            return;
        }

        ImmutableListMultimap.Builder<PlanNodeId, Split> selectors = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
            List<PlanFragmentId> sourceFragmentIds = remoteSource.getSourceFragmentIds();
            Set<ExchangeId> sourceExchangeIds = sourceExchanges.entrySet().stream()
                    .filter(entry -> sourceFragmentIds.contains(entry.getKey()))
                    .map(entry -> entry.getValue().getId())
                    .collect(toImmutableSet());
            ExchangeSourceOutputSelector.Builder selector = ExchangeSourceOutputSelector.builder(sourceExchangeIds);
            for (PlanFragmentId sourceFragment : sourceFragmentIds) {
                FaultTolerantStageScheduler sourceScheduler = sourceSchedulers.get(sourceFragment);
                Exchange sourceExchange = sourceExchanges.get(sourceFragment);
                Map<Integer, Integer> successfulAttempts = sourceScheduler.getSuccessfulAttempts();
                successfulAttempts.forEach((taskPartitionId, attemptId) ->
                        selector.include(sourceExchange.getId(), taskPartitionId, attemptId));
                selector.setPartitionCount(sourceExchange.getId(), successfulAttempts.size());
            }
            selector.setFinal();
            selectors.put(remoteSource.getId(), new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.of(), Optional.of(selector.build())))));
        }
        outputSelectorSplits = selectors.build();
    }

    public synchronized boolean isFinished()
    {
        return failure == null &&
                taskSource != null &&
                taskSource.isFinished() &&
                tasksPopulatedFuture.isDone() &&
                queuedPartitions.isEmpty() &&
                allPartitions.stream().allMatch(finishedPartitions::containsKey);
    }

    public synchronized Map<Integer, Integer> getSuccessfulAttempts()
    {
        return ImmutableMap.copyOf(finishedPartitions);
    }

    public void cancel()
    {
        close(false);
    }

    public void abort()
    {
        close(true);
    }

    private void fail(Throwable t)
    {
        synchronized (this) {
            if (failure == null) {
                failure = t;
            }
        }
        close(true);
    }

    private void close(boolean abort)
    {
        boolean closed;
        synchronized (this) {
            closed = this.closed;
            this.closed = true;
        }
        if (!closed) {
            cancelRunningTasks(abort);
            cancelBlockedFuture();
            releasePendingNodes();
            closeTaskSource();
            closeSinkExchange();
        }
    }

    private void cancelRunningTasks(boolean abort)
    {
        List<RemoteTask> tasks;
        synchronized (this) {
            tasks = ImmutableList.copyOf(runningTasks.values());
        }
        if (abort) {
            tasks.forEach(RemoteTask::abort);
        }
        else {
            tasks.forEach(RemoteTask::cancel);
        }
    }

    private void cancelBlockedFuture()
    {
        verify(!Thread.holdsLock(this));
        ListenableFuture<Void> future;
        synchronized (this) {
            future = blocked;
        }
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }

    private void releasePendingNodes()
    {
        verify(!Thread.holdsLock(this));
        List<NodeAllocator.NodeLease> leases = new ArrayList<>();
        synchronized (this) {
            for (PendingPartition pendingPartition : pendingPartitions) {
                leases.add(pendingPartition.getNodeLease());
            }
            pendingPartitions.clear();
        }
        for (NodeAllocator.NodeLease lease : leases) {
            lease.release();
        }
    }

    private void closeTaskSource()
    {
        TaskSource taskSource;
        synchronized (this) {
            taskSource = this.taskSource;
        }
        if (taskSource != null) {
            try {
                taskSource.close();
            }
            catch (RuntimeException e) {
                log.warn(e, "Error closing task source for stage: %s", stage.getStageId());
            }
        }
    }

    private void closeSinkExchange()
    {
        try {
            sinkExchange.close();
        }
        catch (RuntimeException e) {
            log.warn(e, "Error closing sink exchange for stage: %s", stage.getStageId());
        }
    }

    private int getNextAttemptIdForPartition(int partition)
    {
        int latestAttemptId = partitionToRemoteTaskMap.get(partition).stream()
                .mapToInt(task -> task.getTaskId().getAttemptId())
                .max()
                .orElse(-1);
        return latestAttemptId + 1;
    }

    private void updateTaskStatus(TaskStatus taskStatus, ExchangeSinkHandle exchangeSinkHandle)
    {
        TaskState state = taskStatus.getState();
        if (!state.isDone()) {
            return;
        }

        try {
            RuntimeException failure = null;
            SettableFuture<Void> previousTaskFinishedFuture;
            SettableFuture<Void> previousDelaySchedulingFuture = null;
            synchronized (this) {
                TaskId taskId = taskStatus.getTaskId();

                runningTasks.remove(taskId);
                previousTaskFinishedFuture = taskFinishedFuture;
                if (!runningTasks.isEmpty()) {
                    taskFinishedFuture = SettableFuture.create();
                }
                else {
                    taskFinishedFuture = null;
                }

                NodeAllocator.NodeLease nodeLease = requireNonNull(runningNodes.remove(taskId), () -> "node not found for task id: " + taskId);
                nodeLease.release();

                int partitionId = taskId.getPartitionId();

                if (!finishedPartitions.containsKey(partitionId) && !closed) {
                    MemoryRequirements memoryLimits = partitionMemoryRequirements.get(partitionId);
                    verify(memoryLimits != null);
                    switch (state) {
                        case FINISHED:
                            finishedPartitions.put(partitionId, taskId.getAttemptId());
                            sinkExchange.sinkFinished(exchangeSinkHandle, taskId.getAttemptId());
                            if (noMorePartitions && finishedPartitions.keySet().containsAll(allPartitions)) {
                                sinkExchange.allRequiredSinksFinished();
                            }
                            partitionToRemoteTaskMap.get(partitionId).forEach(RemoteTask::abort);
                            partitionMemoryEstimator.registerPartitionFinished(session, memoryLimits, taskStatus.getPeakMemoryReservation(), true, Optional.empty());

                            if (delayStopwatch.isRunning() && delayStopwatch.elapsed().compareTo(delaySchedulingDuration.get()) > 0) {
                                // we are past delay period and task completed successfully; reset delay
                                previousDelaySchedulingFuture = delaySchedulingFuture;
                                delayStopwatch.reset();
                                delaySchedulingDuration = Optional.empty();
                                delaySchedulingFuture = null;
                            }

                            // Remove taskDescriptor for finished partition to conserve memory
                            // We may revisit the approach when we support volatile exchanges, for which
                            // it may be needed to restart already finished task to recreate output it produced.
                            taskDescriptorStorage.remove(stage.getStageId(), partitionId);

                            break;
                        case CANCELED:
                            log.debug("Task cancelled: %s", taskId);
                            // no need for partitionMemoryEstimator.registerPartitionFinished; task cancelled mid-way
                            break;
                        case ABORTED:
                            log.debug("Task aborted: %s", taskId);
                            // no need for partitionMemoryEstimator.registerPartitionFinished; task aborted mid-way
                            break;
                        case FAILED:
                            ExecutionFailureInfo failureInfo = taskStatus.getFailures().stream()
                                    .findFirst()
                                    .map(this::rewriteTransportFailure)
                                    .orElse(toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason")));
                            log.warn(failureInfo.toException(), "Task failed: %s", taskId);
                            ErrorCode errorCode = failureInfo.getErrorCode();
                            partitionMemoryEstimator.registerPartitionFinished(session, memoryLimits, taskStatus.getPeakMemoryReservation(), false, Optional.ofNullable(errorCode));

                            boolean coordinatorStage = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION);
                            // coordinator tasks cannot be retried
                            int taskRemainingAttempts = remainingAttemptsPerTask.getOrDefault(partitionId, coordinatorStage ? 0 : maxRetryAttemptsPerTask);
                            if (remainingRetryAttemptsOverall.get() > 0
                                    && taskRemainingAttempts > 0
                                    && (errorCode == null || errorCode.getType() != USER_ERROR)) {
                                remainingRetryAttemptsOverall.decrementAndGet();
                                remainingAttemptsPerTask.put(partitionId, taskRemainingAttempts - 1);

                                // update memory limits for next attempt
                                MemoryRequirements newMemoryLimits = partitionMemoryEstimator.getNextRetryMemoryRequirements(session, memoryLimits, taskStatus.getPeakMemoryReservation(), errorCode);
                                log.debug("Computed next memory requirements for task from stage %s; previous=%s; new=%s; peak=%s; estimator=%s", stage.getStageId(), memoryLimits, newMemoryLimits, taskStatus.getPeakMemoryReservation(), partitionMemoryEstimator);

                                if (errorCode != null && isOutOfMemoryError(errorCode) && newMemoryLimits.getRequiredMemory().toBytes() * 0.99 <= taskStatus.getPeakMemoryReservation().toBytes()) {
                                    String message = format(
                                            "Cannot allocate enough memory for task %s. Reported peak memory reservation: %s. Maximum possible reservation: %s.",
                                            taskId,
                                            taskStatus.getPeakMemoryReservation(),
                                            newMemoryLimits.getRequiredMemory());
                                    failure = new TrinoException(() -> errorCode, message, failureInfo.toException());
                                    break;
                                }

                                partitionMemoryRequirements.put(partitionId, newMemoryLimits);

                                // reschedule
                                queuedPartitions.add(partitionId);
                                log.debug("Retrying partition %s for stage %s", partitionId, stage.getStageId());

                                if (errorCode != null && shouldDelayScheduling(errorCode)) {
                                    if (delayStopwatch.isRunning()) {
                                        // we are currently delaying tasks scheduling
                                        checkState(delaySchedulingDuration.isPresent());

                                        if (delayStopwatch.elapsed().compareTo(delaySchedulingDuration.get()) > 0) {
                                            // we are past previous delay period and still getting failures; let's make it longer
                                            delayStopwatch.reset().start();
                                            delaySchedulingDuration = delaySchedulingDuration.map(duration ->
                                                    Ordering.natural().min(
                                                            Duration.ofMillis((long) (duration.toMillis() * retryDelayScaleFactor)),
                                                            maxRetryDelay));

                                            // create new future
                                            previousDelaySchedulingFuture = delaySchedulingFuture;
                                            SettableFuture<Void> newDelaySchedulingFuture = SettableFuture.create();
                                            delaySchedulingFuture = newDelaySchedulingFuture;
                                            futureCompletor.completeFuture(newDelaySchedulingFuture, delaySchedulingDuration.get());
                                        }
                                    }
                                    else {
                                        // initialize delaying of tasks scheduling
                                        delayStopwatch.start();
                                        delaySchedulingDuration = Optional.of(minRetryDelay);
                                        delaySchedulingFuture = SettableFuture.create();
                                        futureCompletor.completeFuture(delaySchedulingFuture, delaySchedulingDuration.get());
                                    }
                                }
                            }
                            else {
                                failure = failureInfo.toException();
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected task state: " + state);
                    }
                }
            }
            if (failure != null) {
                // must be called outside the lock
                fail(failure);
            }
            if (previousTaskFinishedFuture != null && !previousTaskFinishedFuture.isDone()) {
                previousTaskFinishedFuture.set(null);
            }
            if (previousDelaySchedulingFuture != null && !previousDelaySchedulingFuture.isDone()) {
                previousDelaySchedulingFuture.set(null);
            }
        }
        catch (Throwable t) {
            fail(t);
        }
    }

    private boolean shouldDelayScheduling(ErrorCode errorCode)
    {
        return errorCode.getType() == INTERNAL_ERROR || errorCode.getType() == EXTERNAL;
    }

    private ExecutionFailureInfo rewriteTransportFailure(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo.getRemoteHost() == null || failureDetector.getState(executionFailureInfo.getRemoteHost()) != GONE) {
            return executionFailureInfo;
        }

        return new ExecutionFailureInfo(
                executionFailureInfo.getType(),
                executionFailureInfo.getMessage(),
                executionFailureInfo.getCause(),
                executionFailureInfo.getSuppressed(),
                executionFailureInfo.getStack(),
                executionFailureInfo.getErrorLocation(),
                REMOTE_HOST_GONE.toErrorCode(),
                executionFailureInfo.getRemoteHost());
    }

    private static class PendingPartition
    {
        private final int partition;
        private final NodeAllocator.NodeLease nodeLease;

        public PendingPartition(int partition, NodeAllocator.NodeLease nodeLease)
        {
            this.partition = partition;
            this.nodeLease = requireNonNull(nodeLease, "nodeLease is null");
        }

        public int getPartition()
        {
            return partition;
        }

        public NodeAllocator.NodeLease getNodeLease()
        {
            return nodeLease;
        }
    }

    public interface DelayedFutureCompletor
    {
        void completeFuture(SettableFuture<Void> future, Duration delay);
    }
}
