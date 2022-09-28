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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MemoryTrackingRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final RemoteTaskFactory remoteTaskFactory;
    private final QueryStateMachine stateMachine;

    public MemoryTrackingRemoteTaskFactory(RemoteTaskFactory remoteTaskFactory, QueryStateMachine stateMachine)
    {
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
    }

    @Override
    public RemoteTask createRemoteTask(
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
        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                fragment,
                initialSplits,
                outputBuffers,
                partitionedSplitCountTracker,
                outboundDynamicFilterIds,
                estimatedMemory,
                summarizeTaskInfo);

        task.addStateChangeListener(new UpdatePeakMemory(stateMachine));
        return task;
    }

    private static final class UpdatePeakMemory
            implements StateChangeListener<TaskStatus>
    {
        private final QueryStateMachine stateMachine;
        private long previousUserMemory;
        private long previousRevocableMemory;

        public UpdatePeakMemory(QueryStateMachine stateMachine)
        {
            this.stateMachine = stateMachine;
        }

        @Override
        public synchronized void stateChanged(TaskStatus newStatus)
        {
            long currentUserMemory = newStatus.getMemoryReservation().toBytes();
            long currentRevocableMemory = newStatus.getRevocableMemoryReservation().toBytes();
            long currentTotalMemory = currentUserMemory + currentRevocableMemory;
            long deltaUserMemoryInBytes = currentUserMemory - previousUserMemory;
            long deltaRevocableMemoryInBytes = currentRevocableMemory - previousRevocableMemory;
            long deltaTotalMemoryInBytes = currentTotalMemory - (previousUserMemory + previousRevocableMemory);
            previousUserMemory = currentUserMemory;
            previousRevocableMemory = currentRevocableMemory;
            stateMachine.updateMemoryUsage(deltaUserMemoryInBytes, deltaRevocableMemoryInBytes, deltaTotalMemoryInBytes, currentUserMemory, currentRevocableMemory, currentTotalMemory);
        }
    }
}
