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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.trino.SystemSessionProperties.isEnableCoordinatorDynamicFiltersDistribution;
import static io.trino.server.DynamicFilterService.getOutboundDynamicFilters;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SqlStageExecution
{
    private final Session session;
    private final StageStateMachine stateMachine;
    private final RemoteTaskFactory remoteTaskFactory;
    private final NodeTaskMap nodeTaskMap;
    private final boolean summarizeTaskInfo;

    private final Set<DynamicFilterId> outboundDynamicFilterIds;

    private final Map<TaskId, RemoteTask> tasks = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> tasksWithFinalInfo = newConcurrentHashSet();

    public static SqlStageExecution createSqlStageExecution(
            StageId stageId,
            PlanFragment fragment,
            Map<PlanNodeId, TableInfo> tables,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats)
    {
        requireNonNull(stageId, "stageId is null");
        requireNonNull(fragment, "fragment is null");
        checkArgument(fragment.getPartitioningScheme().getBucketToPartition().isEmpty(), "bucket to partition is not expected to be set at this point");
        requireNonNull(tables, "tables is null");
        requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        requireNonNull(session, "session is null");
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(schedulerStats, "schedulerStats is null");

        SqlStageExecution sqlStageExecution = new SqlStageExecution(
                session,
                new StageStateMachine(stageId, fragment, tables, executor, schedulerStats),
                remoteTaskFactory,
                nodeTaskMap,
                summarizeTaskInfo);
        sqlStageExecution.initialize();
        return sqlStageExecution;
    }

    private SqlStageExecution(
            Session session,
            StageStateMachine stateMachine,
            RemoteTaskFactory remoteTaskFactory,
            NodeTaskMap nodeTaskMap,
            boolean summarizeTaskInfo)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = stateMachine;
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        if (isEnableCoordinatorDynamicFiltersDistribution(session)) {
            this.outboundDynamicFilterIds = getOutboundDynamicFilters(stateMachine.getFragment());
        }
        else {
            this.outboundDynamicFilterIds = ImmutableSet.of();
        }
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

    public synchronized boolean transitionToFinished()
    {
        abortRunningTasks();
        return stateMachine.transitionToFinished();
    }

    public synchronized boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        abortRunningTasks();
        return stateMachine.transitionToFailed(throwable);
    }

    private synchronized void abortRunningTasks()
    {
        for (RemoteTask task : tasks.values()) {
            if (!task.getTaskStatus().getState().isDone()) {
                task.abort();
            }
        }
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
                .mapToLong(task -> task.getTaskInfo().getStats().getTotalCpuTime().toMillis())
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

    private Iterable<TaskInfo> getAllTaskInfo()
    {
        return tasks.values().stream()
                .map(RemoteTask::getTaskInfo)
                .collect(toImmutableList());
    }

    // do not synchronize
    // this is used for query info building which should be independent of scheduling work
    public boolean hasTasks()
    {
        return !tasks.isEmpty();
    }

    public synchronized Optional<RemoteTask> createTask(
            InternalNode node,
            int partition,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            Multimap<PlanNodeId, Split> splits,
            Multimap<PlanNodeId, Lifespan> noMoreSplitsForLifespan,
            Set<PlanNodeId> noMoreSplits)
    {
        if (stateMachine.getState().isDone()) {
            return Optional.empty();
        }

        TaskId taskId = new TaskId(stateMachine.getStageId(), partition);
        checkArgument(!tasks.containsKey(taskId), "A task with id %s already exists", taskId);

        stateMachine.transitionToScheduling();

        RemoteTask task = remoteTaskFactory.createRemoteTask(
                session,
                taskId,
                node,
                stateMachine.getFragment().withBucketToPartition(bucketToPartition),
                splits,
                outputBuffers,
                nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
                outboundDynamicFilterIds,
                summarizeTaskInfo);

        noMoreSplitsForLifespan.forEach(task::noMoreSplits);
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

    private synchronized void updateTaskStatus(TaskStatus status)
    {
        if (status.getState().isDone()) {
            finishedTasks.add(status.getTaskId());
        }

        if (!finishedTasks.containsAll(allTasks)) {
            stateMachine.transitionToRunning();
        }
        else {
            stateMachine.transitionToPending();
        }
    }

    private synchronized void updateFinalTaskInfo(TaskInfo finalTaskInfo)
    {
        tasksWithFinalInfo.add(finalTaskInfo.getTaskStatus().getTaskId());
        checkAllTaskFinal();
    }

    private synchronized void checkAllTaskFinal()
    {
        if (stateMachine.getState().isDone() && tasksWithFinalInfo.containsAll(tasks.keySet())) {
            List<TaskInfo> finalTaskInfos = tasks.values().stream()
                    .map(RemoteTask::getTaskInfo)
                    .collect(toImmutableList());
            stateMachine.setAllTasksFinal(finalTaskInfos);
        }
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private class MemoryUsageListener
            implements StateChangeListener<TaskStatus>
    {
        private long previousUserMemory;
        private long previousSystemMemory;
        private long previousRevocableMemory;

        @Override
        public synchronized void stateChanged(TaskStatus taskStatus)
        {
            long currentUserMemory = taskStatus.getMemoryReservation().toBytes();
            long currentSystemMemory = taskStatus.getSystemMemoryReservation().toBytes();
            long currentRevocableMemory = taskStatus.getRevocableMemoryReservation().toBytes();
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaRevocableMemoryInBytes = currentRevocableMemory - previousRevocableMemory;
            long deltaTotalMemoryInBytes = (currentUserMemory + currentSystemMemory + currentRevocableMemory) - (previousUserMemory + previousSystemMemory + previousRevocableMemory);
            previousUserMemory = currentUserMemory;
            previousSystemMemory = currentSystemMemory;
            previousRevocableMemory = currentRevocableMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaRevocableMemoryInBytes, deltaTotalMemoryInBytes);
        }
    }
}
