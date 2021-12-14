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

import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Lifespan;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.Split;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;
import io.trino.split.RemoteSplit.SpoolingExchangeInput;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.execution.buffer.OutputBuffers.createSpoolingExchangeOutputBuffers;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static io.trino.util.Failures.toFailure;
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

    private final TaskLifecycleListener taskLifecycleListener;
    // empty when the results are consumed via a direct exchange
    private final Optional<Exchange> sinkExchange;
    private final Optional<int[]> sinkBucketToPartitionMap;

    private final Map<PlanFragmentId, Exchange> sourceExchanges;
    private final Optional<int[]> sourceBucketToPartitionMap;
    private final Optional<BucketNodeMap> sourceBucketNodeMap;

    @GuardedBy("this")
    private ListenableFuture<Void> blocked = immediateVoidFuture();

    @GuardedBy("this")
    private NodeAllocator.NodeLease nodeLease;
    @GuardedBy("this")
    private SettableFuture<Void> taskFinishedFuture;

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
    private final Queue<Integer> queuedPartitions = new ArrayDeque<>();
    @GuardedBy("this")
    private final Set<Integer> finishedPartitions = new HashSet<>();
    @GuardedBy("this")
    private int remainingRetryAttempts;

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
            TaskLifecycleListener taskLifecycleListener,
            Optional<Exchange> sinkExchange,
            Optional<int[]> sinkBucketToPartitionMap,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Optional<int[]> sourceBucketToPartitionMap,
            Optional<BucketNodeMap> sourceBucketNodeMap,
            int retryAttempts)
    {
        checkArgument(!stage.getFragment().getStageExecutionDescriptor().isStageGroupedExecution(), "grouped execution is expected to be disabled");

        this.session = requireNonNull(session, "session is null");
        this.stage = requireNonNull(stage, "stage is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.taskLifecycleListener = requireNonNull(taskLifecycleListener, "taskLifecycleListener is null");
        this.sinkExchange = requireNonNull(sinkExchange, "sinkExchange is null");
        this.sinkBucketToPartitionMap = requireNonNull(sinkBucketToPartitionMap, "sinkBucketToPartitionMap is null");
        this.sourceExchanges = ImmutableMap.copyOf(requireNonNull(sourceExchanges, "sourceExchanges is null"));
        this.sourceBucketToPartitionMap = requireNonNull(sourceBucketToPartitionMap, "sourceBucketToPartitionMap is null");
        this.sourceBucketNodeMap = requireNonNull(sourceBucketNodeMap, "sourceBucketNodeMap is null");
        checkArgument(retryAttempts >= 0, "retryAttempts must be greater than or equal to 0: %s", retryAttempts);
        this.remainingRetryAttempts = retryAttempts;
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

        if (taskSource == null) {
            Map<PlanFragmentId, ListenableFuture<List<ExchangeSourceHandle>>> sourceHandles = sourceExchanges.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> toListenableFuture(entry.getValue().getSourceHandles())));

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
                    sourceExchanges,
                    exchangeSources,
                    stage::recordGetSplitTime,
                    sourceBucketToPartitionMap,
                    sourceBucketNodeMap);
        }

        while (!queuedPartitions.isEmpty() || !taskSource.isFinished()) {
            while (queuedPartitions.isEmpty() && !taskSource.isFinished()) {
                List<TaskDescriptor> tasks = taskSource.getMoreTasks();
                for (TaskDescriptor task : tasks) {
                    queuedPartitions.add(task.getPartitionId());
                    allPartitions.add(task.getPartitionId());
                    taskDescriptorStorage.put(stage.getStageId(), task);
                    sinkExchange.ifPresent(exchange -> {
                        ExchangeSinkHandle exchangeSinkHandle = exchange.addSink(task.getPartitionId());
                        partitionToExchangeSinkHandleMap.put(task.getPartitionId(), exchangeSinkHandle);
                    });
                }
                if (taskSource.isFinished()) {
                    sinkExchange.ifPresent(Exchange::noMoreSinks);
                }
            }

            if (queuedPartitions.isEmpty()) {
                break;
            }

            int partition = queuedPartitions.peek();
            Optional<TaskDescriptor> taskDescriptorOptional = taskDescriptorStorage.get(stage.getStageId(), partition);
            if (taskDescriptorOptional.isEmpty()) {
                // query has been terminated
                return;
            }
            TaskDescriptor taskDescriptor = taskDescriptorOptional.get();

            if (nodeLease == null) {
                nodeLease = nodeAllocator.acquire(taskDescriptor.getNodeRequirements());
            }
            if (!nodeLease.getNode().isDone()) {
                blocked = asVoid(nodeLease.getNode());
                return;
            }
            NodeInfo node = getFutureValue(nodeLease.getNode());

            queuedPartitions.poll();

            Multimap<PlanNodeId, Split> tableScanSplits = taskDescriptor.getSplits();
            Multimap<PlanNodeId, Split> remoteSplits = createRemoteSplits(taskDescriptor.getExchangeSourceHandles());

            Multimap<PlanNodeId, Split> taskSplits = ImmutableListMultimap.<PlanNodeId, Split>builder()
                    .putAll(tableScanSplits)
                    .putAll(remoteSplits)
                    .build();

            int attemptId = getNextAttemptIdForPartition(partition);

            OutputBuffers outputBuffers;
            Optional<ExchangeSinkInstanceHandle> exchangeSinkInstanceHandle;
            if (sinkExchange.isPresent()) {
                ExchangeSinkHandle sinkHandle = partitionToExchangeSinkHandleMap.get(partition);
                exchangeSinkInstanceHandle = Optional.of(sinkExchange.get().instantiateSink(sinkHandle, attemptId));
                outputBuffers = createSpoolingExchangeOutputBuffers(exchangeSinkInstanceHandle.get());
            }
            else {
                exchangeSinkInstanceHandle = Optional.empty();
                // stage will be consumed by the coordinator using direct exchange
                outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(new OutputBuffers.OutputBufferId(0), 0)
                        .withNoMoreBufferIds();
            }

            Set<PlanNodeId> allSourcePlanNodeIds = ImmutableSet.<PlanNodeId>builder()
                    .addAll(stage.getFragment().getPartitionedSources())
                    .addAll(stage.getFragment()
                            .getRemoteSourceNodes().stream()
                            .map(RemoteSourceNode::getId)
                            .iterator())
                    .build();

            RemoteTask task = stage.createTask(
                    node.getNode(),
                    partition,
                    attemptId,
                    sinkBucketToPartitionMap,
                    outputBuffers,
                    taskSplits,
                    allSourcePlanNodeIds.stream()
                            .collect(toImmutableListMultimap(Function.identity(), planNodeId -> Lifespan.taskWide())),
                    allSourcePlanNodeIds).orElseThrow(() -> new VerifyException("stage execution is expected to be active"));

            partitionToRemoteTaskMap.put(partition, task);
            runningTasks.put(task.getTaskId(), task);
            runningNodes.put(task.getTaskId(), nodeLease);
            nodeLease = null;

            if (taskFinishedFuture == null) {
                taskFinishedFuture = SettableFuture.create();
            }

            taskLifecycleListener.taskCreated(stage.getFragment().getId(), task);

            task.addStateChangeListener(taskStatus -> updateTaskStatus(taskStatus, exchangeSinkInstanceHandle));
            task.start();
        }

        if (taskFinishedFuture != null && !taskFinishedFuture.isDone()) {
            blocked = taskFinishedFuture;
        }
    }

    public synchronized boolean isFinished()
    {
        return failure == null &&
                taskSource != null &&
                taskSource.isFinished() &&
                queuedPartitions.isEmpty() &&
                finishedPartitions.containsAll(allPartitions);
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
            releaseAcquiredNode();
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

    private void releaseAcquiredNode()
    {
        verify(!Thread.holdsLock(this));
        NodeAllocator.NodeLease lease;
        synchronized (this) {
            lease = nodeLease;
            nodeLease = null;
        }
        if (lease != null) {
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
            sinkExchange.ifPresent(Exchange::close);
        }
        catch (RuntimeException e) {
            log.warn(e, "Error closing sink exchange for stage: %s", stage.getStageId());
        }
    }

    public synchronized void reportTaskFailure(TaskId taskId, Throwable failureCause)
    {
        RemoteTask task = runningTasks.get(taskId);
        if (task != null) {
            task.fail(failureCause);
        }
    }

    public void failTaskRemotely(TaskId taskId, Throwable failureCause)
    {
        RemoteTask task = runningTasks.get(taskId);
        if (task != null) {
            task.failRemotely(failureCause);
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

    private static Multimap<PlanNodeId, Split> createRemoteSplits(Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles)
    {
        ImmutableListMultimap.Builder<PlanNodeId, Split> result = ImmutableListMultimap.builder();
        for (PlanNodeId planNodeId : exchangeSourceHandles.keySet()) {
            result.put(planNodeId, new Split(REMOTE_CONNECTOR_ID, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.copyOf(exchangeSourceHandles.get(planNodeId)))), Lifespan.taskWide()));
        }
        return result.build();
    }

    private void updateTaskStatus(TaskStatus taskStatus, Optional<ExchangeSinkInstanceHandle> exchangeSinkInstanceHandle)
    {
        TaskState state = taskStatus.getState();
        if (!state.isDone()) {
            return;
        }

        try {
            RuntimeException failure = null;
            SettableFuture<Void> future;
            synchronized (this) {
                TaskId taskId = taskStatus.getTaskId();

                runningTasks.remove(taskId);
                future = taskFinishedFuture;
                if (!runningTasks.isEmpty()) {
                    taskFinishedFuture = SettableFuture.create();
                }
                else {
                    taskFinishedFuture = null;
                }

                NodeAllocator.NodeLease nodeLease = requireNonNull(runningNodes.remove(taskId), () -> "node not found for task id: " + taskId);
                nodeLease.release();

                int partitionId = taskId.getPartitionId();

                if (!finishedPartitions.contains(partitionId) && !closed) {
                    switch (state) {
                        case FINISHED:
                            finishedPartitions.add(partitionId);
                            if (sinkExchange.isPresent()) {
                                checkArgument(exchangeSinkInstanceHandle.isPresent(), "exchangeSinkInstanceHandle is expected to be present");
                                sinkExchange.get().sinkFinished(exchangeSinkInstanceHandle.get());
                            }
                            partitionToRemoteTaskMap.get(partitionId).forEach(RemoteTask::abort);
                            break;
                        case CANCELED:
                            log.debug("Task cancelled: %s", taskId);
                            break;
                        case ABORTED:
                            log.debug("Task aborted: %s", taskId);
                            break;
                        case FAILED:
                            ExecutionFailureInfo failureInfo = taskStatus.getFailures().stream()
                                    .findFirst()
                                    .map(this::rewriteTransportFailure)
                                    .orElse(toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason")));
                            log.warn(failureInfo.toException(), "Task failed: %s", taskId);
                            ErrorCode errorCode = failureInfo.getErrorCode();
                            if (remainingRetryAttempts > 0 && (errorCode == null || errorCode.getType() != USER_ERROR)) {
                                remainingRetryAttempts--;
                                queuedPartitions.add(partitionId);
                                log.debug("Retrying partition %s for stage %s", partitionId, stage.getStageId());
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
            if (future != null && !future.isDone()) {
                future.set(null);
            }
        }
        catch (Throwable t) {
            fail(t);
        }
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
}
