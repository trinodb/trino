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
import io.trino.cache.SplitAdmissionControllerProvider;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

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
    private final SplitAdmissionControllerProvider splitAdmissionControllerProvider;

    private final Set<DynamicFilterId> outboundDynamicFilterIds;

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
            SplitAdmissionControllerProvider splitAdmissionControllerProvider)
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
                splitAdmissionControllerProvider);
        sqlStage.initialize();
        return sqlStage;
    }

    private SqlStage(
            Session session,
            StageStateMachine stateMachine,
            RemoteTaskFactory remoteTaskFactory,
            NodeTaskMap nodeTaskMap,
            boolean summarizeTaskInfo,
            SplitAdmissionControllerProvider splitAdmissionControllerProvider)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = stateMachine;
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.splitAdmissionControllerProvider = requireNonNull(splitAdmissionControllerProvider, "splitAdmissionControllerProvider is null");

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

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                session,
                stateMachine.getStageSpan(),
                taskId,
                node,
                speculative,
                stateMachine.getFragment().withBucketToPartition(bucketToPartition),
                splits,
                outputBuffers,
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
                outboundDynamicFilterIds,
                estimatedMemory,
                summarizeTaskInfo,
                splitAdmissionControllerProvider);

        noMoreSplits.forEach(task::noMoreSplits);

        tasks.put(taskId, task);
        allTasks.add(taskId);
        nodeTaskMap.addTask(node, task);

        task.addStateChangeListener(this::updateTaskStatus);
        task.addStateChangeListener(new MemoryUsageListener());
        task.addFinalTaskInfoListener(this::updateFinalTaskInfo);

        return Optional.of(task);
    }

    public void recordGetSplitTime(long start)
    {
        stateMachine.recordGetSplitTime(start);
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
}
