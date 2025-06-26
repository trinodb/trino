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
package io.trino.execution;

import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.DynamicFilterService.getOutboundDynamicFilters;
import static java.util.Objects.requireNonNull;

/**
 * This class is merely a container used by coordinator to track tasks for a single stage.
 * <p>
 * It is designed to keep track of execution statistics for tasks from the same stage as well
 * as aggregating them and providing a final stage info when the stage execution is completed.
 * <p>
 * This class doesn't imply anything about the nature of execution. It is not responsible
 * for scheduling tasks in a certain order, gang scheduling or any other execution primitives.
 */
@ThreadSafe
public final class SqlStage
{
    private final Session session;
    private final StageStateMachine stateMachine;
    private final RemoteTaskFactory remoteTaskFactory;
    private final NodeTaskMap nodeTaskMap;
    private final boolean summarizeTaskInfo;

    private final Set<DynamicFilterId> outboundDynamicFilterIds;
    private final LocalExchangeBucketCountProvider bucketCountProvider;

    private final Map<TaskId, RemoteTask> tasks = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<TaskId> allTasks = new HashSet<>();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = new HashSet<>();
    @GuardedBy("this")
    private final Set<TaskId> tasksWithFinalInfo = new HashSet<>();

    public static SqlStage createSqlStage(
            StageId stageId,
            PlanFragment fragment,
            Map<PlanNodeId, TableInfo> tables,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            Executor stateMachineExecutor,
            Tracer tracer,
            Span schedulerSpan,
            SplitSchedulerStats schedulerStats,
            LocalExchangeBucketCountProvider bucketCountProvider)
    {
        requireNonNull(stageId, "stageId is null");
        requireNonNull(fragment, "fragment is null");
        checkArgument(fragment.getOutputPartitioningScheme().getBucketToPartition().isEmpty(), "bucket to partition is not expected to be set at this point");
        requireNonNull(tables, "tables is null");
        requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        requireNonNull(session, "session is null");
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        requireNonNull(stateMachineExecutor, "stateMachineExecutor is null");
        requireNonNull(tracer, "tracer is null");
        requireNonNull(schedulerStats, "schedulerStats is null");

        StageStateMachine stateMachine = new StageStateMachine(
                stageId,
                fragment,
                tables,
                stateMachineExecutor,
                tracer,
                schedulerSpan,
                schedulerStats);

        SqlStage sqlStage = new SqlStage(
                session,
                stateMachine,
                remoteTaskFactory,
                nodeTaskMap,
                summarizeTaskInfo,
                bucketCountProvider);
        sqlStage.initialize();
        return sqlStage;
    }

    private SqlStage(
            Session session,
            StageStateMachine stateMachine,
            RemoteTaskFactory remoteTaskFactory,
            NodeTaskMap nodeTaskMap,
            boolean summarizeTaskInfo,
            LocalExchangeBucketCountProvider bucketCountProvider)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = stateMachine;
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.bucketCountProvider = requireNonNull(bucketCountProvider, "bucketCountProvider is null");

        this.outboundDynamicFilterIds = getOutboundDynamicFilters(stateMachine.getFragment());
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        stateMachine.addStateChangeListener(newState -> checkAllTaskFinal());
    }

    public StageId getStageId()
    {
        return stateMachine.getStageId();
    }

    public Span getStageSpan()
    {
        return stateMachine.getStageSpan();
    }

    public StageState getState()
    {
        return stateMachine.getState();
    }

    public synchronized void finish()
    {
        if (stateMachine.transitionToFinished()) {
            tasks.values().forEach(RemoteTask::cancel);
        }
    }

    public synchronized void abort()
    {
        if (stateMachine.transitionToAborted()) {
            tasks.values().forEach(RemoteTask::abort);
        }
    }

    public synchronized void fail(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        if (stateMachine.transitionToFailed(throwable)) {
            tasks.values().forEach(RemoteTask::abort);
        }
    }

    public void failTaskRemotely(TaskId taskId, Throwable failureCause)
    {
        RemoteTask task = requireNonNull(tasks.get(taskId), () -> "task not found: " + taskId);
        task.failRemotely(failureCause);
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateChangeListener<StageInfo> stateChangeListener)
    {
        stateMachine.addFinalStageInfoListener(stateChangeListener);
    }

    public PlanFragment getFragment()
    {
        return stateMachine.getFragment();
    }

    public long getUserMemoryReservation()
    {
        return stateMachine.getUserMemoryReservation();
    }

    public long getTotalMemoryReservation()
    {
        return stateMachine.getTotalMemoryReservation();
    }

    public Duration getTotalCpuTime()
    {
        long millis = tasks.values().stream()
                .mapToLong(task -> task.getTaskInfo().stats().getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public BasicStageStats getBasicStageStats()
    {
        return stateMachine.getBasicStageStats(this::getAllTaskInfo);
    }

    public StageInfo getStageInfo()
    {
        return stateMachine.getStageInfo(this::getAllTaskInfo);
    }

    public BasicStageInfo getBasicStageInfo()
    {
        return stateMachine.getBasicStageInfo(this::getAllTaskInfo);
    }

    private Iterable<TaskInfo> getAllTaskInfo()
    {
        return tasks.values().stream()
                .map(RemoteTask::getTaskInfo)
                .collect(toImmutableList());
    }

    public synchronized Optional<RemoteTask> createTask(
            InternalNode node,
            int partition,
            int attempt,
            Optional<int[]> bucketToPartition,
            OptionalInt skewedBucketCount,
            OutputBuffers outputBuffers,
            Multimap<PlanNodeId, Split> splits,
            Set<PlanNodeId> noMoreSplits,
            Optional<DataSize> estimatedMemory,
            boolean speculative)
    {
        if (stateMachine.getState().isDone()) {
            return Optional.empty();
        }
        TaskId taskId = new TaskId(stateMachine.getStageId(), partition, attempt);
        checkArgument(!tasks.containsKey(taskId), "A task with id %s already exists", taskId);

        stateMachine.transitionToScheduling();

        // set partitioning information on coordinator side
        PlanFragment fragment = stateMachine.getFragment();
        fragment = fragment.withOutputPartitioning(bucketToPartition, skewedBucketCount);
        PlanNode newRoot = fragment.getRoot();
        LocalExchangePartitionRewriter rewriter = new LocalExchangePartitionRewriter(handle -> bucketCountProvider.getBucketCount(session, handle));
        newRoot = SimplePlanRewriter.rewriteWith(rewriter, newRoot);
        fragment = fragment.withRoot(newRoot);

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                session,
                stateMachine.getStageSpan(),
                taskId,
                node,
                speculative,
                fragment,
                splits,
                outputBuffers,
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
                outboundDynamicFilterIds,
                estimatedMemory,
                summarizeTaskInfo);

        noMoreSplits.forEach(task::noMoreSplits);

        tasks.put(taskId, task);
        allTasks.add(taskId);
        nodeTaskMap.addTask(node, task);

        task.addStateChangeListener(this::updateTaskStatus);
        task.addStateChangeListener(new MemoryUsageListener());
        task.addFinalTaskInfoListener(this::updateFinalTaskInfo);

        return Optional.of(task);
    }

    public void recordSplitSourceMetrics(PlanNodeId nodeId, Metrics metrics, long start)
    {
        stateMachine.recordSplitSourceMetrics(nodeId, metrics, start);
    }

    private void updateTaskStatus(TaskStatus status)
    {
        boolean isDone = status.getState().isDone();
        if (!isDone && stateMachine.getState() == StageState.RUNNING) {
            return;
        }
        synchronized (this) {
            if (isDone) {
                finishedTasks.add(status.getTaskId());
            }
            if (finishedTasks.size() == allTasks.size()) {
                stateMachine.transitionToPending();
            }
            else {
                stateMachine.transitionToRunning();
            }
        }
    }

    private synchronized void updateFinalTaskInfo(TaskInfo finalTaskInfo)
    {
        tasksWithFinalInfo.add(finalTaskInfo.taskStatus().getTaskId());
        checkAllTaskFinal();
    }

    private void checkAllTaskFinal()
    {
        if (!stateMachine.getState().isDone()) {
            return;
        }
        synchronized (this) {
            if (tasksWithFinalInfo.size() == allTasks.size()) {
                List<TaskInfo> finalTaskInfos = tasks.values().stream()
                        .map(RemoteTask::getTaskInfo)
                        .collect(toImmutableList());
                stateMachine.setAllTasksFinal(finalTaskInfos);
            }
        }
    }

    @Override
    // for debugging
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("stateMachine", stateMachine)
                .add("summarizeTaskInfo", summarizeTaskInfo)
                .add("outboundDynamicFilterIds", outboundDynamicFilterIds)
                .add("tasks", tasks)
                .add("allTasks", allTasks)
                .add("finishedTasks", finishedTasks)
                .add("tasksWithFinalInfo", tasksWithFinalInfo)
                .toString();
    }

    private class MemoryUsageListener
            implements StateChangeListener<TaskStatus>
    {
        private long previousUserMemory;
        private long previousRevocableMemory;
        private boolean finalUsageReported;

        @Override
        public synchronized void stateChanged(TaskStatus taskStatus)
        {
            if (finalUsageReported) {
                return;
            }
            long currentUserMemory = taskStatus.getMemoryReservation().toBytes();
            long currentRevocableMemory = taskStatus.getRevocableMemoryReservation().toBytes();
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaRevocableMemoryInBytes = currentRevocableMemory - previousRevocableMemory;
            long deltaTotalMemoryInBytes = (currentUserMemory + currentRevocableMemory) - (previousUserMemory + previousRevocableMemory);
            previousUserMemory = currentUserMemory;
            previousRevocableMemory = currentRevocableMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaRevocableMemoryInBytes, deltaTotalMemoryInBytes);

            if (taskStatus.getState().isDone()) {
                // if task is finished perform final memory update to 0
                stateMachine.updateMemoryUsage(-currentUserMemory, -currentRevocableMemory, -(currentUserMemory + currentRevocableMemory));
                previousUserMemory = 0;
                previousRevocableMemory = 0;
                finalUsageReported = true;
            }
        }
    }

    public interface LocalExchangeBucketCountProvider
    {
        Optional<Integer> getBucketCount(Session session, PartitioningHandle partitioning);
    }

    private static final class LocalExchangePartitionRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Function<PartitioningHandle, Optional<Integer>> bucketCountProvider;

        public LocalExchangePartitionRewriter(Function<PartitioningHandle, Optional<Integer>> bucketCountProvider)
        {
            this.bucketCountProvider = requireNonNull(bucketCountProvider, "bucketCountProvider is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme().withBucketCount(bucketCountProvider.apply(node.getPartitioningScheme().getPartitioning().getHandle())),
                    node.getSources(),
                    node.getInputs(),
                    node.getOrderingScheme());
        }
    }
}
