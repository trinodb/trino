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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.operator.TaskStats;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.util.Failures.toFailures;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingRemoteTaskFactory
        implements RemoteTaskFactory
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private final Map<TaskId, TestingRemoteTask> tasks = new HashMap<>();

    @Override
    public synchronized RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            Set<DynamicFilterId> outboundDynamicFilterIds,
            Optional<DataSize> estimatedMemory,
            boolean summarizeTaskInfo)
    {
        TestingRemoteTask task = new TestingRemoteTask(taskId, node.getNodeIdentifier(), fragment);
        task.addSplits(initialSplits);
        task.setOutputBuffers(outputBuffers);
        checkState(tasks.put(taskId, task) == null, "task already exist: %s", taskId);
        return task;
    }

    public synchronized Map<TaskId, TestingRemoteTask> getTasks()
    {
        return ImmutableMap.copyOf(tasks);
    }

    public static class TestingRemoteTask
            implements RemoteTask
    {
        private final TaskStateMachine taskStateMachine;
        private final String nodeId;
        private final URI location;
        private final PlanFragment fragment;

        private final AtomicLong nextTaskStatusVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

        private final AtomicBoolean started = new AtomicBoolean();
        private final Set<PlanNodeId> noMoreSplits = newConcurrentHashSet();
        @GuardedBy("this")
        private final Multimap<PlanNodeId, Split> splits = ArrayListMultimap.create();
        @GuardedBy("this")
        private OutputBuffers outputBuffers;

        public TestingRemoteTask(TaskId taskId, String nodeId, PlanFragment fragment)
        {
            this.taskStateMachine = new TaskStateMachine(taskId, directExecutor());
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            this.location = URI.create("fake://task/" + taskId + "/node/" + nodeId);
            this.fragment = requireNonNull(fragment, "fragment is null");
        }

        public PlanFragment getFragment()
        {
            return fragment;
        }

        @Override
        public TaskId getTaskId()
        {
            return taskStateMachine.getTaskId();
        }

        @Override
        public String getNodeId()
        {
            return nodeId;
        }

        @Override
        public TaskInfo getTaskInfo()
        {
            return new TaskInfo(
                    getTaskStatus(),
                    DateTime.now(),
                    new OutputBufferInfo(
                            "TESTING",
                            BufferState.FINISHED,
                            false,
                            false,
                            0,
                            0,
                            0,
                            0,
                            Optional.empty(),
                            Optional.of(new TDigestHistogram(new TDigest())),
                            Optional.empty()),
                    ImmutableSet.copyOf(noMoreSplits),
                    new TaskStats(DateTime.now(), null),
                    Optional.empty(),
                    false);
        }

        @Override
        public TaskStatus getTaskStatus()
        {
            TaskState state = taskStateMachine.getState();
            List<ExecutionFailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }
            return new TaskStatus(
                    taskStateMachine.getTaskId(),
                    TASK_INSTANCE_ID,
                    nextTaskStatusVersion.getAndIncrement(),
                    state,
                    location,
                    nodeId,
                    failures,
                    0,
                    0,
                    OutputBufferStatus.initial(),
                    DataSize.of(0, BYTE),
                    DataSize.of(0, BYTE),
                    Optional.empty(),
                    DataSize.of(0, BYTE),
                    DataSize.of(0, BYTE),
                    DataSize.of(0, BYTE),
                    0,
                    new Duration(0, MILLISECONDS),
                    INITIAL_DYNAMIC_FILTERS_VERSION,
                    0,
                    0);
        }

        @Override
        public void start()
        {
            started.set(true);
        }

        public boolean isStarted()
        {
            return started.get();
        }

        @Override
        public synchronized void addSplits(Multimap<PlanNodeId, Split> splits)
        {
            this.splits.putAll(splits);
        }

        public synchronized Multimap<PlanNodeId, Split> getSplits()
        {
            return ImmutableListMultimap.copyOf(splits);
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);
        }

        public Set<PlanNodeId> getNoMoreSplits()
        {
            return ImmutableSet.copyOf(noMoreSplits);
        }

        @Override
        public synchronized void setOutputBuffers(OutputBuffers outputBuffers)
        {
            this.outputBuffers = outputBuffers;
        }

        public synchronized OutputBuffers getOutputBuffers()
        {
            return outputBuffers;
        }

        @Override
        public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(newValue -> stateChangeListener.stateChanged(getTaskStatus()));
        }

        @Override
        public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
        {
            AtomicBoolean done = new AtomicBoolean();
            StateChangeListener<TaskState> fireOnceStateChangeListener = state -> {
                if (state.isDone() && done.compareAndSet(false, true)) {
                    stateChangeListener.stateChanged(getTaskInfo());
                }
            };
            taskStateMachine.addStateChangeListener(fireOnceStateChangeListener);
            fireOnceStateChangeListener.stateChanged(taskStateMachine.getState());
        }

        @Override
        public ListenableFuture<Void> whenSplitQueueHasSpace(long weightThreshold)
        {
            return immediateVoidFuture();
        }

        @Override
        public void cancel()
        {
            taskStateMachine.cancel();
            taskStateMachine.terminationComplete();
        }

        @Override
        public void abort()
        {
            taskStateMachine.abort();
            taskStateMachine.terminationComplete();
        }

        @Override
        public PartitionedSplitsInfo getPartitionedSplitsInfo()
        {
            return PartitionedSplitsInfo.forZeroSplits();
        }

        @Override
        public void failRemotely(Throwable cause)
        {
            taskStateMachine.failed(cause);
            taskStateMachine.terminationComplete();
        }

        @Override
        public void failLocallyImmediately(Throwable cause)
        {
            taskStateMachine.failed(cause);
            taskStateMachine.terminationComplete();
        }

        @Override
        public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo()
        {
            return PartitionedSplitsInfo.forZeroSplits();
        }

        public void finish()
        {
            taskStateMachine.finished();
        }

        @Override
        public int getUnacknowledgedPartitionedSplitCount()
        {
            return 0;
        }

        @Override
        public SpoolingOutputStats.Snapshot retrieveAndDropSpoolingOutputStats()
        {
            throw new UnsupportedOperationException();
        }
    }
}
