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
import io.trino.execution.RemoteTask;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(FixedSourcePartitionedScheduler.class);

    private final StageExecution stageExecution;
    private final List<InternalNode> nodes;
    private final Queue<SourceScheduler> sourceSchedulers;

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
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager)
    {
        requireNonNull(stageExecution, "stageExecution is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        checkArgument(!nodes.isEmpty(), "nodes is empty");
        requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");

        this.stageExecution = stageExecution;
        this.nodes = ImmutableList.copyOf(nodes);

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        BucketedSplitPlacementPolicy splitPlacementPolicy = new BucketedSplitPlacementPolicy(nodeSelector, nodes, bucketNodeMap, stageExecution::getAllTasks);

        ArrayList<SourceScheduler> sourceSchedulers = new ArrayList<>();

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
                    splitBatchSize,
                    dynamicFilterService,
                    tableExecuteContextManager,
                    () -> true,
                    partitionIdAllocator,
                    scheduledTasks);

            sourceSchedulers.add(sourceScheduler);
        }
        this.sourceSchedulers = new ArrayDeque<>(sourceSchedulers);
    }

    @Override
    public ScheduleResult schedule()
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (scheduledTasks.isEmpty()) {
            ImmutableList.Builder<RemoteTask> newTasksBuilder = ImmutableList.builder();
            for (InternalNode node : nodes) {
                Optional<RemoteTask> task = stageExecution.scheduleTask(node, partitionIdAllocator.getNextId(), ImmutableMultimap.of());
                if (task.isPresent()) {
                    scheduledTasks.put(node, task.get());
                    newTasksBuilder.add(task.get());
                }
            }
            newTasks = newTasksBuilder.build();
        }

        ListenableFuture<Void> blocked = immediateFuture(null);
        ScheduleResult.BlockedReason blockedReason = null;
        int splitsScheduled = 0;
        while (!sourceSchedulers.isEmpty()) {
            SourceScheduler scheduler = sourceSchedulers.peek();
            ScheduleResult schedule = scheduler.schedule();
            splitsScheduled += schedule.getSplitsScheduled();
            blocked = schedule.getBlocked();

            if (schedule.getBlockedReason().isPresent()) {
                blockedReason = schedule.getBlockedReason().get();
            }
            else {
                blockedReason = null;
            }

            // if the source is not done scheduling, stop scheduling for now
            if (!blocked.isDone() || !schedule.isFinished()) {
                break;
            }

            stageExecution.schedulingComplete(scheduler.getPlanNodeId());
            sourceSchedulers.remove().close();
        }

        if (blockedReason != null) {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, blocked, blockedReason, splitsScheduled);
        }
        checkState(blocked.isDone(), "blockedReason not provided when scheduler is blocked");
        return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, splitsScheduled);
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
    }
}
