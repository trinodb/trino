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
import io.trino.server.DynamicFilterService;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class MultiSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger log = Logger.get(MultiSourcePartitionedScheduler.class);

    private final StageExecution stageExecution;
    private final Queue<SourceScheduler> sourceSchedulers;
    private final Map<InternalNode, RemoteTask> scheduledTasks = new HashMap<>();
    private final DynamicFilterService dynamicFilterService;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final PartitionIdAllocator partitionIdAllocator = new PartitionIdAllocator();

    public MultiSourcePartitionedScheduler(
            StageExecution stageExecution,
            Map<PlanNodeId, SplitSource> splitSources,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager,
            BooleanSupplier anySourceTaskBlocked)
    {
        requireNonNull(splitSources, "splitSources is null");
        checkArgument(splitSources.size() > 1, "It is expected that there will be more than one split sources");

        ImmutableList.Builder<SourceScheduler> sourceSchedulers = ImmutableList.builder();
        for (PlanNodeId planNodeId : splitSources.keySet()) {
            SplitSource splitSource = splitSources.get(planNodeId);
            SourceScheduler sourceScheduler = newSourcePartitionedSchedulerAsSourceScheduler(
                    stageExecution,
                    planNodeId,
                    splitSource,
                    splitPlacementPolicy,
                    splitBatchSize,
                    dynamicFilterService,
                    tableExecuteContextManager,
                    anySourceTaskBlocked,
                    partitionIdAllocator,
                    scheduledTasks);
            sourceSchedulers.add(sourceScheduler);
        }
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.sourceSchedulers = new ArrayDeque<>(sourceSchedulers.build());
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");
    }

    @Override
    public synchronized void start()
    {
        /*
         * Avoid deadlocks by immediately scheduling a task for collecting dynamic filters because:
         *  * there can be task in other stage blocked waiting for the dynamic filters, or
         *  * connector split source for this stage might be blocked waiting the dynamic filters.
        */
        if (dynamicFilterService.isCollectingTaskNeeded(stageExecution.getStageId().getQueryId(), stageExecution.getFragment())) {
            stageExecution.beginScheduling();
            /*
             * We can select node randomly because DynamicFilterSourceOperator is not dependent on splits
             * scheduled by this scheduler.
             */
            scheduleTaskOnRandomNode();
        }
    }

    @Override
    public synchronized ScheduleResult schedule()
    {
        ImmutableSet.Builder<RemoteTask> newScheduledTasks = ImmutableSet.builder();
        ListenableFuture<Void> blocked = immediateVoidFuture();
        Optional<ScheduleResult.BlockedReason> blockedReason = Optional.empty();
        int splitsScheduled = 0;

        while (!sourceSchedulers.isEmpty()) {
            SourceScheduler scheduler = sourceSchedulers.peek();
            ScheduleResult scheduleResult = scheduler.schedule();

            splitsScheduled += scheduleResult.getSplitsScheduled();
            newScheduledTasks.addAll(scheduleResult.getNewTasks());
            blocked = scheduleResult.getBlocked();
            blockedReason = scheduleResult.getBlockedReason();

            // if the source is not done scheduling, stop scheduling for now
            if (!blocked.isDone() || !scheduleResult.isFinished()) {
                break;
            }

            stageExecution.schedulingComplete(scheduler.getPlanNodeId());
            sourceSchedulers.remove().close();
        }
        if (blockedReason.isPresent()) {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newScheduledTasks.build(), blocked, blockedReason.get(), splitsScheduled);
        }
        return new ScheduleResult(sourceSchedulers.isEmpty(), newScheduledTasks.build(), splitsScheduled);
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

    private void scheduleTaskOnRandomNode()
    {
        checkState(scheduledTasks.isEmpty(), "Stage task is already scheduled on node");
        List<InternalNode> allNodes = splitPlacementPolicy.allNodes();
        checkState(allNodes.size() > 0, "No nodes available");
        InternalNode node = allNodes.get(ThreadLocalRandom.current().nextInt(0, allNodes.size()));
        Optional<RemoteTask> remoteTask = stageExecution.scheduleTask(node, partitionIdAllocator.getNextId(), ImmutableMultimap.of());
        remoteTask.ifPresent(task -> scheduledTasks.put(node, task));
    }
}
