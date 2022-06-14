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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.execution.Lifespan;
import io.trino.execution.RemoteTask;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.scheduler.ScheduleResult.BlockedReason;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(FixedSourcePartitionedScheduler.class);

    private final StageExecution stageExecution;
    private final List<InternalNode> nodes;
    private final List<SourceScheduler> sourceSchedulers;
    private final List<ConnectorPartitionHandle> partitionHandles;

    private final PartitionIdAllocator partitionIdAllocator;
    private final Map<InternalNode, RemoteTask> scheduledTasks;

    public FixedSourcePartitionedScheduler(
            StageExecution stageExecution,
            Map<PlanNodeId, SplitSource> splitSources,
            List<PlanNodeId> schedulingOrder,
            List<InternalNode> nodes,
            BucketNodeMap bucketNodeMap,
            int splitBatchSize,
            NodeSelector nodeSelector,
            List<ConnectorPartitionHandle> partitionHandles,
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager)
    {
        requireNonNull(stageExecution, "stageExecution is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        checkArgument(!requireNonNull(nodes, "nodes is null").isEmpty(), "nodes is empty");
        requireNonNull(partitionHandles, "partitionHandles is null");
        requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");

        this.stageExecution = stageExecution;
        this.nodes = ImmutableList.copyOf(nodes);
        this.partitionHandles = ImmutableList.copyOf(partitionHandles);

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        BucketedSplitPlacementPolicy splitPlacementPolicy = new BucketedSplitPlacementPolicy(nodeSelector, nodes, bucketNodeMap, stageExecution::getAllTasks);

        ArrayList<SourceScheduler> sourceSchedulers = new ArrayList<>();
        int concurrentLifespans = partitionHandles.size();

        boolean firstPlanNode = true;

        partitionIdAllocator = new PartitionIdAllocator();
        scheduledTasks = new HashMap<>();
        for (PlanNodeId planNodeId : schedulingOrder) {
            SplitSource splitSource = splitSources.get(planNodeId);
            // TODO : change anySourceTaskBlocked to accommodate the correct blocked status of source tasks
            //  (ref : https://github.com/trinodb/trino/issues/4713)
            SourceScheduler sourceScheduler = newSourcePartitionedSchedulerAsSourceScheduler(
                    stageExecution,
                    planNodeId,
                    splitSource,
                    splitPlacementPolicy,
                    Math.max(splitBatchSize / concurrentLifespans, 1),
                    false,
                    dynamicFilterService,
                    tableExecuteContextManager,
                    () -> true,
                    partitionIdAllocator,
                    scheduledTasks);

            sourceSchedulers.add(sourceScheduler);

            if (firstPlanNode) {
                firstPlanNode = false;
                sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
                sourceScheduler.noMoreLifespans();
            }
        }
        this.sourceSchedulers = sourceSchedulers;
    }

    private ConnectorPartitionHandle partitionHandleFor(Lifespan lifespan)
    {
        if (lifespan.isTaskWide()) {
            return NOT_PARTITIONED;
        }
        return partitionHandles.get(lifespan.getId());
    }

    @Override
    public ScheduleResult schedule()
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (scheduledTasks.isEmpty()) {
            ImmutableList.Builder<RemoteTask> newTasksBuilder = ImmutableList.builder();
            for (InternalNode node : nodes) {
                Optional<RemoteTask> task = stageExecution.scheduleTask(node, partitionIdAllocator.getNextId(), ImmutableMultimap.of(), ImmutableMultimap.of());
                if (task.isPresent()) {
                    scheduledTasks.put(node, task.get());
                    newTasksBuilder.add(task.get());
                }
            }
            newTasks = newTasksBuilder.build();
        }

        boolean allBlocked = true;
        List<ListenableFuture<Void>> blocked = new ArrayList<>();
        BlockedReason blockedReason = BlockedReason.NO_ACTIVE_DRIVER_GROUP;

        int splitsScheduled = 0;
        Iterator<SourceScheduler> schedulerIterator = sourceSchedulers.iterator();
        List<Lifespan> driverGroupsToStart = ImmutableList.of();
        boolean shouldInvokeNoMoreDriverGroups = false;
        while (schedulerIterator.hasNext()) {
            SourceScheduler sourceScheduler = schedulerIterator.next();

            for (Lifespan lifespan : driverGroupsToStart) {
                sourceScheduler.startLifespan(lifespan, partitionHandleFor(lifespan));
            }
            if (shouldInvokeNoMoreDriverGroups) {
                sourceScheduler.noMoreLifespans();
            }

            ScheduleResult schedule = sourceScheduler.schedule();
            splitsScheduled += schedule.getSplitsScheduled();
            if (schedule.getBlockedReason().isPresent()) {
                blocked.add(schedule.getBlocked());
                blockedReason = blockedReason.combineWith(schedule.getBlockedReason().get());
            }
            else {
                verify(schedule.getBlocked().isDone(), "blockedReason not provided when scheduler is blocked");
                allBlocked = false;
            }

            driverGroupsToStart = sourceScheduler.drainCompletedLifespans();

            if (schedule.isFinished()) {
                stageExecution.schedulingComplete(sourceScheduler.getPlanNodeId());
                schedulerIterator.remove();
                sourceScheduler.close();
                shouldInvokeNoMoreDriverGroups = true;
            }
            else {
                shouldInvokeNoMoreDriverGroups = false;
            }
        }

        if (allBlocked) {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, whenAnyComplete(blocked), blockedReason, splitsScheduled);
        }
        else {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, splitsScheduled);
        }
    }

    @Override
    public void close()
    {
        for (SourceScheduler sourceScheduler : sourceSchedulers) {
            try {
                sourceScheduler.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }
        sourceSchedulers.clear();
    }

    public static class BucketedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final NodeSelector nodeSelector;
        private final List<InternalNode> allNodes;
        private final BucketNodeMap bucketNodeMap;
        private final Supplier<? extends List<RemoteTask>> remoteTasks;

        public BucketedSplitPlacementPolicy(
                NodeSelector nodeSelector,
                List<InternalNode> allNodes,
                BucketNodeMap bucketNodeMap,
                Supplier<? extends List<RemoteTask>> remoteTasks)
        {
            this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
            this.allNodes = ImmutableList.copyOf(requireNonNull(allNodes, "allNodes is null"));
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits)
        {
            return nodeSelector.computeAssignments(splits, remoteTasks.get(), bucketNodeMap);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<InternalNode> allNodes()
        {
            return allNodes;
        }

        public InternalNode getNodeForBucket(int bucketId)
        {
            return bucketNodeMap.getAssignedNode(bucketId).get();
        }
    }
}
