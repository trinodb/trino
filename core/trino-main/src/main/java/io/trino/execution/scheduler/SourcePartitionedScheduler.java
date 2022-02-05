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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.execution.RemoteTask;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.split.EmptySplit;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.NO_ACTIVE_DRIVER_GROUP;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static java.util.Objects.requireNonNull;

public class SourcePartitionedScheduler
        implements SourceScheduler
{
    private enum State
    {
        /**
         * No splits have been added to pendingSplits set.
         */
        INITIALIZED,

        /**
         * All splits from underlying SplitSource have been discovered.
         * No more splits will be added to the pendingSplits set.
         */
        SPLITS_SCHEDULED,

        /**
         * All splits have been provided to caller of this scheduler.
         * Cleanup operations are done
         */
        FINISHED
    }

    private final StageExecution stageExecution;
    private final SplitSource splitSource;
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;
    private final PlanNodeId partitionedNode;
    private final DynamicFilterService dynamicFilterService;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final BooleanSupplier anySourceTaskBlocked;
    private final PartitionIdAllocator partitionIdAllocator;
    private final Map<InternalNode, RemoteTask> scheduledTasks;

    public ListenableFuture<SplitBatch> nextSplitBatchFuture;
    public ListenableFuture<Void> placementFuture = immediateVoidFuture();
    public final Set<Split> pendingSplits = new HashSet<>();
    private State state = State.INITIALIZED;

    private SettableFuture<Void> whenFinished = SettableFuture.create();

    private SourcePartitionedScheduler(
            StageExecution stageExecution,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager,
            BooleanSupplier anySourceTaskBlocked,
            PartitionIdAllocator partitionIdAllocator,
            Map<InternalNode, RemoteTask> scheduledTasks)
    {
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");
        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;
        this.partitionedNode = requireNonNull(partitionedNode, "partitionedNode is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.anySourceTaskBlocked = requireNonNull(anySourceTaskBlocked, "anySourceTaskBlocked is null");
        this.partitionIdAllocator = requireNonNull(partitionIdAllocator, "partitionIdAllocator is null");
        this.scheduledTasks = requireNonNull(scheduledTasks, "scheduledTasks is null");
    }

    @Override
    public PlanNodeId getPlanNodeId()
    {
        return partitionedNode;
    }

    /**
     * Obtains an instance of {@code SourcePartitionedScheduler} suitable for use as a
     * stage scheduler.
     * <p>
     * This returns an ungrouped {@code SourcePartitionedScheduler} that requires
     * minimal management from the caller, which is ideal for use as a stage scheduler.
     */
    public static StageScheduler newSourcePartitionedSchedulerAsStageScheduler(
            StageExecution stageExecution,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager,
            BooleanSupplier anySourceTaskBlocked)
    {
        SourcePartitionedScheduler sourcePartitionedScheduler = new SourcePartitionedScheduler(
                stageExecution,
                partitionedNode,
                splitSource,
                splitPlacementPolicy,
                splitBatchSize,
                dynamicFilterService,
                tableExecuteContextManager,
                anySourceTaskBlocked,
                new PartitionIdAllocator(),
                new HashMap<>());

        return new StageScheduler()
        {
            @Override
            public void start()
            {
                sourcePartitionedScheduler.start();
            }

            @Override
            public ScheduleResult schedule()
            {
                return sourcePartitionedScheduler.schedule();
            }

            @Override
            public void close()
            {
                sourcePartitionedScheduler.close();
            }
        };
    }

    /**
     * Obtains a {@code SourceScheduler} suitable for use in FixedSourcePartitionedScheduler.
     * <p>
     * This returns a {@code SourceScheduler} that can be used for a pipeline
     * that is either ungrouped or grouped. However, the caller is responsible initializing
     * the driver groups in this scheduler accordingly.
     */
    public static SourceScheduler newSourcePartitionedSchedulerAsSourceScheduler(
            StageExecution stageExecution,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager,
            BooleanSupplier anySourceTaskBlocked,
            PartitionIdAllocator partitionIdAllocator,
            Map<InternalNode, RemoteTask> scheduledTasks)
    {
        return new SourcePartitionedScheduler(
                stageExecution,
                partitionedNode,
                splitSource,
                splitPlacementPolicy,
                splitBatchSize,
                dynamicFilterService,
                tableExecuteContextManager,
                anySourceTaskBlocked,
                partitionIdAllocator,
                scheduledTasks);
    }

    @Override
    public synchronized void start()
    {
        // Avoid deadlocks by immediately scheduling a task for collecting dynamic filters because:
        // * there can be task in other stage blocked waiting for the dynamic filters, or
        // * connector split source for this stage might be blocked waiting the dynamic filters.
        if (dynamicFilterService.isCollectingTaskNeeded(stageExecution.getStageId().getQueryId(), stageExecution.getFragment())) {
            stageExecution.beginScheduling();
            createTaskOnRandomNode();
        }
    }

    @Override
    public synchronized ScheduleResult schedule()
    {
        dropListenersFromWhenFinished();

        int overallSplitAssignmentCount = 0;
        ImmutableSet.Builder<RemoteTask> overallNewTasks = ImmutableSet.builder();
        List<ListenableFuture<?>> overallBlockedFutures = new ArrayList<>();
        boolean anyBlockedOnPlacements = false;
        boolean anyBlockedOnNextSplitBatch = false;
        boolean anyNotBlocked = false;

        if (state != State.FINISHED) {
            if (state == State.SPLITS_SCHEDULED) {
                verify(nextSplitBatchFuture == null);
            }
            else if (pendingSplits.isEmpty()) {
                // try to get the next batch
                if (nextSplitBatchFuture == null) {
                    nextSplitBatchFuture = splitSource.getNextBatch(splitBatchSize);

                    long start = System.nanoTime();
                    addSuccessCallback(nextSplitBatchFuture, () -> stageExecution.recordGetSplitTime(start));
                }

                if (nextSplitBatchFuture.isDone()) {
                    SplitBatch nextSplits = getFutureValue(nextSplitBatchFuture);
                    nextSplitBatchFuture = null;
                    pendingSplits.addAll(nextSplits.getSplits());
                    if (nextSplits.isLastBatch()) {
                        if (state == State.INITIALIZED && pendingSplits.isEmpty()) {
                            // Add an empty split in case no splits have been produced for the source.
                            // For source operators, they never take input, but they may produce output.
                            // This is well handled by the execution engine.
                            // However, there are certain non-source operators that may produce output without any input,
                            // for example, 1) an AggregationOperator, 2) a HashAggregationOperator where one of the grouping sets is ().
                            // Scheduling an empty split kicks off necessary driver instantiation to make this work.
                            pendingSplits.add(new Split(
                                    splitSource.getCatalogHandle(),
                                    new EmptySplit(splitSource.getCatalogHandle())));
                        }
                        state = State.SPLITS_SCHEDULED;
                    }
                }
                else {
                    overallBlockedFutures.add(nextSplitBatchFuture);
                    anyBlockedOnNextSplitBatch = true;
                }
            }
            if (!anyBlockedOnNextSplitBatch) {
                Multimap<InternalNode, Split> splitAssignment = ImmutableMultimap.of();
                boolean skip = false;
                if (!pendingSplits.isEmpty()) {
                    if (!placementFuture.isDone()) {
                        anyBlockedOnPlacements = true;
                        skip = true;
                    }
                    else { // calculate placements for splits
                        SplitPlacementResult splitPlacementResult = splitPlacementPolicy.computeAssignments(pendingSplits);
                        splitAssignment = splitPlacementResult.getAssignments(); // remove splits with successful placements
                        splitAssignment.values().forEach(pendingSplits::remove); // AbstractSet.removeAll performs terribly here.
                        overallSplitAssignmentCount += splitAssignment.size(); // if not completed placed, mark scheduleGroup as blocked on placement
                        if (!pendingSplits.isEmpty()) {
                            placementFuture = splitPlacementResult.getBlocked();
                            overallBlockedFutures.add(placementFuture);
                            anyBlockedOnPlacements = true;
                        }
                    }
                }
                if (!skip) { // if no new splits will be assigned, update state and attach completion event
                    if (pendingSplits.isEmpty() && state == State.SPLITS_SCHEDULED) {
                        state = State.FINISHED;
                    }

                    // assign the splits with successful placements
                    overallNewTasks.addAll(assignSplits(splitAssignment));

                    // Assert that "placement future is not done" implies "pendingSplits is not empty".
                    // The other way around is not true. One obvious reason is (un)lucky timing, where the placement is unblocked between `computeAssignments` and this line.
                    // However, there are other reasons that could lead to this.
                    // Note that `computeAssignments` is quite broken:
                    // 1. It always returns a completed future when there are no tasks, regardless of whether all nodes are blocked.
                    // 2. The returned future will only be completed when a node with an assigned task becomes unblocked. Other nodes don't trigger future completion.
                    // As a result, to avoid busy loops caused by 1, we check pendingSplits.isEmpty() instead of placementFuture.isDone() here.
                    if (nextSplitBatchFuture == null && pendingSplits.isEmpty() && state != State.FINISHED) {
                        anyNotBlocked = true;
                    }
                }
            }
        }

        // * `splitSource.isFinished` invocation may fail after `splitSource.close` has been invoked.
        //   If state is FINISHED, splitSource.isFinished has previously returned true, and splitSource is closed now.
        // * Even if `splitSource.isFinished()` return true, it is not necessarily safe to tear down the split source.
        //   * If anyBlockedOnNextSplitBatch is true, it means we have not checked out the recently completed nextSplitBatch futures,
        //     which may contain recently published splits. We must not ignore those.
        if (state == State.FINISHED && splitSource.isFinished()) {
            Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();

            // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
            tableExecuteSplitsInfo.ifPresent(info -> {
                TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(stageExecution.getStageId().getQueryId());
                tableExecuteContext.setSplitsInfo(info);
            });

            splitSource.close();
            return new ScheduleResult(
                    true,
                    overallNewTasks.build(),
                    overallSplitAssignmentCount);
        }

        if (anyNotBlocked) {
            return new ScheduleResult(false, overallNewTasks.build(), overallSplitAssignmentCount);
        }

        boolean anySourceTaskBlocked = this.anySourceTaskBlocked.getAsBoolean();
        if (anySourceTaskBlocked) {
            // Dynamic filters might not be collected due to build side source tasks being blocked on full buffer.
            // In such case probe split generation that is waiting for dynamic filters should be unblocked to prevent deadlock.
            dynamicFilterService.unblockStageDynamicFilters(stageExecution.getStageId().getQueryId(), stageExecution.getAttemptId(), stageExecution.getFragment());
        }

        if (anyBlockedOnPlacements && anySourceTaskBlocked) {
            // In a broadcast join, output buffers of the tasks in build source stage have to
            // hold onto all data produced before probe side task scheduling finishes,
            // even if the data is acknowledged by all known consumers. This is because
            // new consumers may be added until the probe side task scheduling finishes.
            //
            // As a result, the following line is necessary to prevent deadlock
            // due to neither build nor probe can make any progress.
            // The build side blocks due to a full output buffer.
            // In the meantime the probe side split cannot be consumed since
            // builder side hash table construction has not finished.
            overallNewTasks.addAll(finalizeTaskCreationIfNecessary());
        }

        ScheduleResult.BlockedReason blockedReason;
        if (anyBlockedOnNextSplitBatch) {
            blockedReason = anyBlockedOnPlacements ? MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE : WAITING_FOR_SOURCE;
        }
        else {
            blockedReason = anyBlockedOnPlacements ? SPLIT_QUEUES_FULL : NO_ACTIVE_DRIVER_GROUP;
        }

        overallBlockedFutures.add(whenFinished);

        if (state == State.FINISHED && splitSource.isFinished()) {
            // Wake up blocked caller so that it will invoke schedule() right away.
            // Once schedule is invoked, state will be transitioned to FINISHED.
            whenFinished.set(null);
            whenFinished = SettableFuture.create();
        }

        return new ScheduleResult(
                false,
                overallNewTasks.build(),
                nonCancellationPropagating(asVoid(whenAnyComplete(overallBlockedFutures))),
                blockedReason,
                overallSplitAssignmentCount);
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    private synchronized void dropListenersFromWhenFinished()
    {
        // whenFinished may remain in a not-done state for an extended period of time.
        // As a result, over time, it can retain a huge number of listener objects.

        // Whenever schedule is called, holding onto the previous listener is not useful anymore.
        // Therefore, we drop those listeners here by recreating the future.

        // Note: The following implementation is thread-safe because whenFinished can only be completed
        // while holding the monitor of this.

        if (whenFinished.isDone()) {
            return;
        }

        whenFinished.cancel(true);
        whenFinished = SettableFuture.create();
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    private Set<RemoteTask> assignSplits(Multimap<InternalNode, Split> splitAssignment)
    {
        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();

        ImmutableSet<InternalNode> nodes = ImmutableSet.<InternalNode>builder()
                .addAll(splitAssignment.keySet())
                .build();
        for (InternalNode node : nodes) {
            // source partitioned tasks can only receive broadcast data; otherwise it would have a different distribution
            ImmutableMultimap<PlanNodeId, Split> splits = ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(partitionedNode, splitAssignment.get(node))
                    .build();
            RemoteTask task = scheduledTasks.get(node);
            if (task != null) {
                task.addSplits(splits);
            }
            else {
                scheduleTask(node, splits).ifPresent(newTasks::add);
            }
        }
        return newTasks.build();
    }

    private void createTaskOnRandomNode()
    {
        checkState(scheduledTasks.isEmpty(), "Stage task is already scheduled on node");
        List<InternalNode> allNodes = splitPlacementPolicy.allNodes();
        checkState(allNodes.size() > 0, "No nodes available");
        InternalNode node = allNodes.get(ThreadLocalRandom.current().nextInt(0, allNodes.size()));
        scheduleTask(node, ImmutableMultimap.of());
    }

    private Set<RemoteTask> finalizeTaskCreationIfNecessary()
    {
        // only lock down tasks if there is a sub stage that could block waiting for this stage to create all tasks
        if (stageExecution.getFragment().isLeaf()) {
            return ImmutableSet.of();
        }

        splitPlacementPolicy.lockDownNodes();

        Set<RemoteTask> newTasks = splitPlacementPolicy.allNodes().stream()
                .filter(node -> !scheduledTasks.containsKey(node))
                .map(node -> scheduleTask(node, ImmutableMultimap.of()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stageExecution.transitionToSchedulingSplits();

        return newTasks;
    }

    private Optional<RemoteTask> scheduleTask(InternalNode node, Multimap<PlanNodeId, Split> initialSplits)
    {
        Optional<RemoteTask> remoteTask = stageExecution.scheduleTask(node, partitionIdAllocator.getNextId(), initialSplits);
        remoteTask.ifPresent(task -> scheduledTasks.put(node, task));
        return remoteTask;
    }
}
