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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import io.trino.spi.SplitWeight;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableWriterNode;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static com.google.common.collect.Sets.union;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMinTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskInputSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantPreserveInputPartitionsInWriteStage;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

/**
 * Deprecated in favor of {@link EventDrivenTaskSourceFactory}
 */
@Deprecated
public class StageTaskSourceFactory
        implements TaskSourceFactory
{
    private static final Logger log = Logger.get(StageTaskSourceFactory.class);

    private final SplitSourceFactory splitSourceFactory;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final int splitBatchSize;
    private final Executor executor;
    private final InternalNodeManager nodeManager;

    @Inject
    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            QueryManagerConfig queryManagerConfig,
            @ForQueryExecution ExecutorService executor,
            InternalNodeManager nodeManager)
    {
        this(
                splitSourceFactory,
                tableExecuteContextManager,
                queryManagerConfig.getScheduleSplitBatchSize(),
                executor,
                nodeManager);
    }

    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            int splitBatchSize,
            ExecutorService executor,
            InternalNodeManager nodeManager)
    {
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitBatchSize = splitBatchSize;
        this.executor = requireNonNull(executor, "executor is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public TaskSource create(
            Session session,
            PlanFragment fragment,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
            LongConsumer getSplitTimeRecorder,
            FaultTolerantPartitioningScheme sourcePartitioningScheme)
    {
        PartitioningHandle partitioning = fragment.getPartitioning();

        if (partitioning.getConnectorHandle() instanceof MergePartitioningHandle mergeHandle) {
            return mergeHandle.getTaskSource(handle -> create(session, fragment.withPartitioning(handle), exchangeSourceHandles, getSplitTimeRecorder, sourcePartitioningScheme));
        }

        if (partitioning.equals(SINGLE_DISTRIBUTION) || partitioning.equals(COORDINATOR_DISTRIBUTION)) {
            return SingleDistributionTaskSource.create(fragment, exchangeSourceHandles, nodeManager, partitioning.equals(COORDINATOR_DISTRIBUTION));
        }
        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION)) {
            return ArbitraryDistributionTaskSource.create(
                    fragment,
                    exchangeSourceHandles,
                    getFaultTolerantExecutionTargetTaskInputSize(session));
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent() ||
                (partitioning.getConnectorHandle() instanceof MergePartitioningHandle)) {
            return HashDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    exchangeSourceHandles,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    sourcePartitioningScheme,
                    getFaultTolerantExecutionTargetTaskSplitCount(session) * SplitWeight.standard().getRawValue(),
                    getFaultTolerantExecutionTargetTaskInputSize(session),
                    getFaultTolerantPreserveInputPartitionsInWriteStage(session),
                    executor);
        }
        if (partitioning.equals(SOURCE_DISTRIBUTION)) {
            return SourceDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    exchangeSourceHandles,
                    tableExecuteContextManager,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    getFaultTolerantExecutionMinTaskSplitCount(session),
                    getFaultTolerantExecutionTargetTaskSplitCount(session) * SplitWeight.standard().getRawValue(),
                    getFaultTolerantExecutionMaxTaskSplitCount(session),
                    executor);
        }

        // other partitioning handles are not expected to be set as a fragment partitioning
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }

    public static class SingleDistributionTaskSource
            implements TaskSource
    {
        private final ListMultimap<PlanNodeId, Split> splits;
        private final InternalNodeManager nodeManager;
        private final boolean coordinatorOnly;

        private boolean finished;

        public static SingleDistributionTaskSource create(
                PlanFragment fragment,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                InternalNodeManager nodeManager,
                boolean coordinatorOnly)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            return new SingleDistributionTaskSource(
                    createRemoteSplits(getInputsForRemoteSources(fragment.getRemoteSourceNodes(), exchangeSourceHandles)),
                    nodeManager,
                    coordinatorOnly);
        }

        @VisibleForTesting
        SingleDistributionTaskSource(ListMultimap<PlanNodeId, Split> splits, InternalNodeManager nodeManager, boolean coordinatorOnly)
        {
            this.splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
            this.nodeManager = requireNonNull(nodeManager, "nodeManager");
            this.coordinatorOnly = coordinatorOnly;
        }

        @Override
        public ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished) {
                return immediateFuture(ImmutableList.of());
            }
            ImmutableSet<HostAddress> hostRequirement = ImmutableSet.of();
            if (coordinatorOnly) {
                Node currentNode = nodeManager.getCurrentNode();
                verify(currentNode.isCoordinator(), "current node is expected to be a coordinator");
                hostRequirement = ImmutableSet.of(currentNode.getHostAndPort());
            }
            List<TaskDescriptor> result = ImmutableList.of(new TaskDescriptor(
                    0,
                    splits,
                    new NodeRequirements(Optional.empty(), hostRequirement)));
            finished = true;
            return immediateFuture(result);
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public void close()
        {
        }
    }

    public static class ArbitraryDistributionTaskSource
            implements TaskSource
    {
        private final Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;
        private final long targetPartitionSizeInBytes;

        private boolean finished;

        public static ArbitraryDistributionTaskSource create(
                PlanFragment fragment,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                DataSize targetPartitionSize)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            return new ArbitraryDistributionTaskSource(
                    getPartitionedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    targetPartitionSize);
        }

        @VisibleForTesting
        ArbitraryDistributionTaskSource(
                Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles,
                Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                DataSize targetPartitionSize)
        {
            this.partitionedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(partitionedExchangeSourceHandles, "partitionedExchangeSourceHandles is null"));
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.targetPartitionSizeInBytes = requireNonNull(targetPartitionSize, "targetPartitionSize is null").toBytes();
        }

        @Override
        public ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished) {
                return immediateFuture(ImmutableList.of());
            }
            NodeRequirements nodeRequirements = new NodeRequirements(Optional.empty(), ImmutableSet.of());

            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            int currentPartitionId = 0;

            ListMultimap<PlanNodeId, ExchangeSourceHandle> assignedExchangeSourceHandles = ArrayListMultimap.create();
            long assignedExchangeDataSize = 0;
            int assignedExchangeSourceHandleCount = 0;

            for (Map.Entry<PlanNodeId, ExchangeSourceHandle> entry : partitionedExchangeSourceHandles.entries()) {
                PlanNodeId remoteSourcePlanNodeId = entry.getKey();
                ExchangeSourceHandle handle = entry.getValue();
                long handleDataSizeInBytes = handle.getDataSizeInBytes();

                if (assignedExchangeDataSize != 0 && assignedExchangeDataSize + handleDataSizeInBytes > targetPartitionSizeInBytes) {
                    assignedExchangeSourceHandles.putAll(replicatedExchangeSourceHandles);
                    result.add(new TaskDescriptor(
                            currentPartitionId++,
                            createRemoteSplits(assignedExchangeSourceHandles),
                            nodeRequirements));
                    assignedExchangeSourceHandles.clear();
                    assignedExchangeDataSize = 0;
                    assignedExchangeSourceHandleCount = 0;
                }

                assignedExchangeSourceHandles.put(remoteSourcePlanNodeId, handle);
                assignedExchangeDataSize += handleDataSizeInBytes;
                assignedExchangeSourceHandleCount++;
            }

            if (assignedExchangeSourceHandleCount > 0) {
                assignedExchangeSourceHandles.putAll(replicatedExchangeSourceHandles);
                result.add(new TaskDescriptor(currentPartitionId, createRemoteSplits(assignedExchangeSourceHandles), nodeRequirements));
            }

            finished = true;
            return immediateFuture(result.build());
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public void close()
        {
        }
    }

    public static class HashDistributionTaskSource
            implements TaskSource
    {
        private final Map<PlanNodeId, SplitSource> splitSources;
        private final ListMultimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles;
        private final ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;

        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
        private final Optional<CatalogHandle> catalogRequirement;
        private final long targetPartitionSourceSizeInBytes; // compared data read from ExchangeSources
        private final long targetPartitionSplitWeight; // compared against splits from SplitSources
        private final Executor executor;

        @GuardedBy("this")
        private ListenableFuture<List<LoadedSplits>> loadedSplitsFuture;
        @GuardedBy("this")
        private boolean finished;
        @GuardedBy("this")
        private boolean closed;

        public static HashDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                SplitSourceFactory splitSourceFactory,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                FaultTolerantPartitioningScheme sourcePartitioningScheme,
                long targetPartitionSplitWeight,
                DataSize targetPartitionSourceSize,
                boolean preserveInputPartitionsInWriteStage,
                Executor executor)
        {
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            return new HashDistributionTaskSource(
                    splitSources,
                    getPartitionedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    sourcePartitioningScheme,
                    fragment.getPartitioning().getCatalogHandle(),
                    targetPartitionSplitWeight,
                    (preserveInputPartitionsInWriteStage && isWriteFragment(fragment)) ? DataSize.of(0, BYTE) : targetPartitionSourceSize,
                    executor);
        }

        private static boolean isWriteFragment(PlanFragment fragment)
        {
            PlanVisitor<Boolean, Void> visitor = new PlanVisitor<>()
            {
                @Override
                protected Boolean visitPlan(PlanNode node, Void context)
                {
                    for (PlanNode child : node.getSources()) {
                        if (child.accept(this, context)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Boolean visitTableWriter(TableWriterNode node, Void context)
                {
                    return true;
                }
            };

            return fragment.getRoot().accept(visitor, null);
        }

        @VisibleForTesting
        HashDistributionTaskSource(
                Map<PlanNodeId, SplitSource> splitSources,
                ListMultimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles,
                ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                FaultTolerantPartitioningScheme sourcePartitioningScheme,
                Optional<CatalogHandle> catalogRequirement,
                long targetPartitionSplitWeight,
                DataSize targetPartitionSourceSize,
                Executor executor)
        {
            this.splitSources = ImmutableMap.copyOf(requireNonNull(splitSources, "splitSources is null"));
            this.partitionedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(partitionedExchangeSourceHandles, "partitionedExchangeSourceHandles is null"));
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            this.targetPartitionSourceSizeInBytes = targetPartitionSourceSize.toBytes();
            this.targetPartitionSplitWeight = targetPartitionSplitWeight;
            this.executor = requireNonNull(executor, "executor is null");
        }

        @Override
        public synchronized ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished || closed) {
                return immediateFuture(ImmutableList.of());
            }
            checkState(loadedSplitsFuture == null, "getMoreTasks called again while splits are being loaded");

            List<ListenableFuture<LoadedSplits>> splitSourceCompletionFutures = splitSources.entrySet().stream()
                    .map(entry -> {
                        SplitLoadingFuture future = new SplitLoadingFuture(entry.getKey(), entry.getValue(), splitBatchSize, getSplitTimeRecorder, executor);
                        future.load();
                        return future;
                    })
                    .collect(toImmutableList());

            loadedSplitsFuture = allAsList(splitSourceCompletionFutures);
            return Futures.transform(
                    loadedSplitsFuture,
                    loadedSplitsList -> {
                        synchronized (this) {
                            Map<Integer, ListMultimap<PlanNodeId, Split>> partitionToSplitsMap = new HashMap<>();
                            SetMultimap<Integer, HostAddress> partitionToNodeMap = HashMultimap.create();
                            for (LoadedSplits loadedSplits : loadedSplitsList) {
                                for (Split split : loadedSplits.getSplits()) {
                                    int partition = sourcePartitioningScheme.getPartition(split);
                                    Optional<InternalNode> assignedNode = sourcePartitioningScheme.getNodeRequirement(partition);
                                    if (assignedNode.isPresent()) {
                                        HostAddress requiredAddress = assignedNode.get().getHostAndPort();
                                        Set<HostAddress> existingRequirement = partitionToNodeMap.get(partition);
                                        if (existingRequirement.isEmpty()) {
                                            existingRequirement.add(requiredAddress);
                                        }
                                        else {
                                            checkState(
                                                    existingRequirement.contains(requiredAddress),
                                                    "Unable to satisfy host requirement for partition %s. Existing requirement %s; Current split requirement: %s;",
                                                    partition,
                                                    existingRequirement,
                                                    requiredAddress);
                                            existingRequirement.removeIf(host -> !host.equals(requiredAddress));
                                        }
                                    }

                                    if (!split.isRemotelyAccessible()) {
                                        Set<HostAddress> requiredAddresses = ImmutableSet.copyOf(split.getAddresses());
                                        verify(!requiredAddresses.isEmpty(), "split is not remotely accessible but the list of addresses is empty: %s", split);
                                        Set<HostAddress> existingRequirement = partitionToNodeMap.get(partition);
                                        if (existingRequirement.isEmpty()) {
                                            existingRequirement.addAll(requiredAddresses);
                                        }
                                        else {
                                            Set<HostAddress> intersection = Sets.intersection(requiredAddresses, existingRequirement);
                                            checkState(
                                                    !intersection.isEmpty(),
                                                    "Unable to satisfy host requirement for partition %s. Existing requirement %s; Current split requirement: %s;",
                                                    partition,
                                                    existingRequirement,
                                                    requiredAddresses);
                                            partitionToNodeMap.replaceValues(partition, ImmutableSet.copyOf(intersection));
                                        }
                                    }

                                    Multimap<PlanNodeId, Split> partitionSplits = partitionToSplitsMap.computeIfAbsent(partition, (p) -> ArrayListMultimap.create());
                                    partitionSplits.put(loadedSplits.getPlanNodeId(), split);
                                }
                            }

                            Map<Integer, ListMultimap<PlanNodeId, ExchangeSourceHandle>> partitionToExchangeSourceHandlesMap = new HashMap<>();
                            for (Map.Entry<PlanNodeId, ExchangeSourceHandle> entry : partitionedExchangeSourceHandles.entries()) {
                                PlanNodeId planNodeId = entry.getKey();
                                ExchangeSourceHandle handle = entry.getValue();
                                int partition = handle.getPartitionId();
                                Multimap<PlanNodeId, ExchangeSourceHandle> partitionSourceHandles = partitionToExchangeSourceHandlesMap.computeIfAbsent(partition, (p) -> ArrayListMultimap.create());
                                partitionSourceHandles.put(planNodeId, handle);
                            }

                            int taskPartitionId = 0;
                            ImmutableList.Builder<TaskDescriptor> partitionTasks = ImmutableList.builder();
                            for (Integer partition : union(partitionToSplitsMap.keySet(), partitionToExchangeSourceHandlesMap.keySet())) {
                                ImmutableListMultimap.Builder<PlanNodeId, Split> splits = ImmutableListMultimap.builder();
                                splits.putAll(partitionToSplitsMap.getOrDefault(partition, ImmutableListMultimap.of()));
                                // replicated exchange source will be added in postprocessTasks below
                                splits.putAll(createRemoteSplits(partitionToExchangeSourceHandlesMap.getOrDefault(partition, ImmutableListMultimap.of())));
                                Set<HostAddress> hostRequirement = partitionToNodeMap.get(partition);
                                partitionTasks.add(new TaskDescriptor(taskPartitionId++, splits.build(), new NodeRequirements(catalogRequirement, hostRequirement)));
                            }

                            List<TaskDescriptor> result = postprocessTasks(partitionTasks.build());
                            finished = true;
                            return result;
                        }
                    },
                    executor);
        }

        private List<TaskDescriptor> postprocessTasks(List<TaskDescriptor> tasks)
        {
            ListMultimap<NodeRequirements, TaskDescriptor> taskGroups = groupCompatibleTasks(tasks);
            ImmutableList.Builder<TaskDescriptor> joinedTasks = ImmutableList.builder();
            long replicatedExchangeSourcesSize = replicatedExchangeSourceHandles.values().stream().mapToLong(ExchangeSourceHandle::getDataSizeInBytes).sum();
            int taskPartitionId = 0;
            for (Map.Entry<NodeRequirements, Collection<TaskDescriptor>> taskGroup : taskGroups.asMap().entrySet()) {
                NodeRequirements groupNodeRequirements = taskGroup.getKey();
                Collection<TaskDescriptor> groupTasks = taskGroup.getValue();

                ImmutableListMultimap.Builder<PlanNodeId, Split> splits = ImmutableListMultimap.builder();
                long splitsWeight = 0;
                long exchangeSourcesSize = 0;

                for (TaskDescriptor task : groupTasks) {
                    ListMultimap<PlanNodeId, Split> taskSplits = task.getSplits();
                    long taskSplitWeight = 0;
                    long taskExchangeSourcesSize = 0;
                    for (Split split : taskSplits.values()) {
                        if (split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
                            RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
                            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
                            taskExchangeSourcesSize += exchangeInput.getExchangeSourceHandles().stream().mapToLong(ExchangeSourceHandle::getDataSizeInBytes).sum();
                        }
                        else {
                            taskSplitWeight += split.getSplitWeight().getRawValue();
                        }
                    }

                    if ((splitsWeight > 0 || exchangeSourcesSize > 0)
                            && ((splitsWeight + taskSplitWeight) > targetPartitionSplitWeight || (exchangeSourcesSize + taskExchangeSourcesSize + replicatedExchangeSourcesSize) > targetPartitionSourceSizeInBytes)) {
                        splits.putAll(createRemoteSplits(replicatedExchangeSourceHandles)); // add replicated exchanges
                        joinedTasks.add(new TaskDescriptor(taskPartitionId++, splits.build(), groupNodeRequirements));
                        splits = ImmutableListMultimap.builder();
                        splitsWeight = 0;
                        exchangeSourcesSize = 0;
                    }

                    splits.putAll(taskSplits);
                    splitsWeight += taskSplitWeight;
                    exchangeSourcesSize += taskExchangeSourcesSize;
                }

                ImmutableListMultimap<PlanNodeId, Split> remainderSplits = splits.build();
                if (!remainderSplits.isEmpty()) {
                    joinedTasks.add(new TaskDescriptor(
                            taskPartitionId++,
                            ImmutableListMultimap.<PlanNodeId, Split>builder()
                                    .putAll(remainderSplits)
                                    // add replicated exchanges
                                    .putAll(createRemoteSplits(replicatedExchangeSourceHandles))
                                    .build(),
                            groupNodeRequirements));
                }
            }
            return joinedTasks.build();
        }

        private ListMultimap<NodeRequirements, TaskDescriptor> groupCompatibleTasks(List<TaskDescriptor> tasks)
        {
            return Multimaps.index(tasks, TaskDescriptor::getNodeRequirements);
        }

        @Override
        public synchronized boolean isFinished()
        {
            return finished;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            for (SplitSource splitSource : splitSources.values()) {
                try {
                    splitSource.close();
                }
                catch (RuntimeException e) {
                    log.error(e, "Error closing split source");
                }
            }
        }
    }

    public static class SourceDistributionTaskSource
            implements TaskSource
    {
        private final QueryId queryId;
        private final PlanNodeId partitionedSourceNodeId;
        private final TableExecuteContextManager tableExecuteContextManager;
        private final SplitSource splitSource;
        private final ListMultimap<PlanNodeId, Split> replicatedSplits;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Optional<CatalogHandle> catalogRequirement;
        private final int minPartitionSplitCount;
        private final long targetPartitionSplitWeight;
        private final int maxPartitionSplitCount;
        private final Executor executor;

        @GuardedBy("this")
        private final Set<Split> remotelyAccessibleSplitBuffer = newIdentityHashSet();
        @GuardedBy("this")
        private final Map<HostAddress, Set<Split>> locallyAccessibleSplitBuffer = new HashMap<>();

        @GuardedBy("this")
        private int currentPartitionId;
        @GuardedBy("this")
        private boolean finished;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private ListenableFuture<SplitBatch> currentSplitBatchFuture = immediateFuture(null);

        public static SourceDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                SplitSourceFactory splitSourceFactory,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                TableExecuteContextManager tableExecuteContextManager,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int minPartitionSplitCount,
                long targetPartitionSplitWeight,
                int maxPartitionSplitCount,
                Executor executor)
        {
            checkArgument(fragment.getPartitionedSources().size() == 1, "single partitioned source is expected, got: %s", fragment.getPartitionedSources());

            List<RemoteSourceNode> remoteSourceNodes = fragment.getRemoteSourceNodes();
            checkArgument(remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE), "only replicated exchanges are expected in source distributed stage, got: %s", remoteSourceNodes);

            PlanNodeId partitionedSourceNodeId = getOnlyElement(fragment.getPartitionedSources());
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            SplitSource splitSource = splitSources.get(partitionedSourceNodeId);

            Optional<CatalogHandle> catalogName = Optional.of(splitSource.getCatalogHandle())
                    .filter(catalog -> !catalog.getType().isInternal());

            return new SourceDistributionTaskSource(
                    session.getQueryId(),
                    partitionedSourceNodeId,
                    tableExecuteContextManager,
                    splitSource,
                    createRemoteSplits(getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles)),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    catalogName,
                    minPartitionSplitCount,
                    targetPartitionSplitWeight,
                    maxPartitionSplitCount,
                    executor);
        }

        @VisibleForTesting
        SourceDistributionTaskSource(
                QueryId queryId,
                PlanNodeId partitionedSourceNodeId,
                TableExecuteContextManager tableExecuteContextManager,
                SplitSource splitSource,
                ListMultimap<PlanNodeId, Split> replicatedSplits,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<CatalogHandle> catalogRequirement,
                int minPartitionSplitCount,
                long targetPartitionSplitWeight,
                int maxPartitionSplitCount,
                Executor executor)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.partitionedSourceNodeId = requireNonNull(partitionedSourceNodeId, "partitionedSourceNodeId is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.replicatedSplits = ImmutableListMultimap.copyOf(requireNonNull(replicatedSplits, "replicatedSplits is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            checkArgument(targetPartitionSplitWeight > 0, "targetPartitionSplitCount must be greater than 0: %s", targetPartitionSplitWeight);
            this.targetPartitionSplitWeight = targetPartitionSplitWeight;
            checkArgument(minPartitionSplitCount >= 0, "minPartitionSplitCount must be greater than or equal to 0: %s", minPartitionSplitCount);
            this.minPartitionSplitCount = minPartitionSplitCount;
            checkArgument(maxPartitionSplitCount > 0, "maxPartitionSplitCount must be greater than 0: %s", maxPartitionSplitCount);
            checkArgument(maxPartitionSplitCount >= minPartitionSplitCount,
                    "maxPartitionSplitCount(%s) must be greater than or equal to minPartitionSplitCount(%s)",
                    maxPartitionSplitCount,
                    minPartitionSplitCount);
            this.maxPartitionSplitCount = maxPartitionSplitCount;
            this.executor = requireNonNull(executor, "executor is null");
        }

        @Override
        public synchronized ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished || closed) {
                return immediateFuture(ImmutableList.of());
            }

            checkState(currentSplitBatchFuture.isDone(), "getMoreTasks called again before the previous batch of splits was ready");
            currentSplitBatchFuture = splitSource.getNextBatch(splitBatchSize);

            long start = System.nanoTime();
            addSuccessCallback(currentSplitBatchFuture, () -> getSplitTimeRecorder.accept(start));

            return Futures.transform(
                    currentSplitBatchFuture,
                    splitBatch -> {
                        synchronized (this) {
                            for (Split split : splitBatch.getSplits()) {
                                if (split.isRemotelyAccessible()) {
                                    remotelyAccessibleSplitBuffer.add(split);
                                }
                                else {
                                    List<HostAddress> addresses = split.getAddresses();
                                    checkArgument(!addresses.isEmpty(), "split is not remotely accessible but the list of addresses is empty");
                                    for (HostAddress hostAddress : addresses) {
                                        locallyAccessibleSplitBuffer.computeIfAbsent(hostAddress, key -> newIdentityHashSet()).add(split);
                                    }
                                }
                            }

                            ImmutableList.Builder<TaskDescriptor> readyTasksBuilder = ImmutableList.builder();
                            boolean isLastBatch = splitBatch.isLastBatch();
                            readyTasksBuilder.addAll(getReadyTasks(
                                    remotelyAccessibleSplitBuffer,
                                    ImmutableList.of(),
                                    new NodeRequirements(catalogRequirement, ImmutableSet.of()),
                                    isLastBatch));
                            for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                                readyTasksBuilder.addAll(getReadyTasks(
                                        locallyAccessibleSplitBuffer.get(remoteHost),
                                        locallyAccessibleSplitBuffer.entrySet().stream()
                                                .filter(entry -> !entry.getKey().equals(remoteHost))
                                                .map(Map.Entry::getValue)
                                                .collect(toImmutableList()),
                                        new NodeRequirements(catalogRequirement, ImmutableSet.of(remoteHost)),
                                        isLastBatch));
                            }
                            List<TaskDescriptor> readyTasks = readyTasksBuilder.build();

                            if (isLastBatch) {
                                Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();

                                // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
                                tableExecuteSplitsInfo.ifPresent(info -> {
                                    TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
                                    tableExecuteContext.setSplitsInfo(info);
                                });

                                try {
                                    splitSource.close();
                                }
                                catch (RuntimeException e) {
                                    log.error(e, "Error closing split source");
                                }
                                finished = true;
                            }

                            return readyTasks;
                        }
                    },
                    executor);
        }

        private List<TaskDescriptor> getReadyTasks(Set<Split> splits, List<Set<Split>> otherSplitSets, NodeRequirements nodeRequirements, boolean includeRemainder)
        {
            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            while (true) {
                Optional<TaskDescriptor> readyTask = getReadyTask(splits, otherSplitSets, nodeRequirements);
                if (readyTask.isEmpty()) {
                    break;
                }
                result.add(readyTask.get());
            }

            if (includeRemainder && !splits.isEmpty()) {
                result.add(buildTaskDescriptor(splits, nodeRequirements));
                for (Set<Split> otherSplits : otherSplitSets) {
                    otherSplits.removeAll(splits);
                }
                splits.clear();
            }
            return result.build();
        }

        private Optional<TaskDescriptor> getReadyTask(Set<Split> splits, List<Set<Split>> otherSplitSets, NodeRequirements nodeRequirements)
        {
            ImmutableList.Builder<Split> chosenSplitsBuilder = ImmutableList.builder();
            int splitCount = 0;
            int totalSplitWeight = 0;
            for (Split split : splits) {
                totalSplitWeight += split.getSplitWeight().getRawValue();
                splitCount++;
                chosenSplitsBuilder.add(split);

                if (splitCount >= minPartitionSplitCount && (totalSplitWeight >= targetPartitionSplitWeight || splitCount >= maxPartitionSplitCount)) {
                    ImmutableList<Split> chosenSplits = chosenSplitsBuilder.build();
                    for (Set<Split> otherSplits : otherSplitSets) {
                        chosenSplits.forEach(otherSplits::remove);
                    }
                    chosenSplits.forEach(splits::remove);
                    return Optional.of(buildTaskDescriptor(chosenSplits, nodeRequirements));
                }
            }
            return Optional.empty();
        }

        private synchronized TaskDescriptor buildTaskDescriptor(Collection<Split> splits, NodeRequirements nodeRequirements)
        {
            return new TaskDescriptor(
                    currentPartitionId++,
                    ImmutableListMultimap.<PlanNodeId, Split>builder()
                            .putAll(partitionedSourceNodeId, splits)
                            .putAll(replicatedSplits)
                            .build(),
                    nodeRequirements);
        }

        @Override
        public synchronized boolean isFinished()
        {
            return finished;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            splitSource.close();
        }
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> getReplicatedExchangeSourceHandles(PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> handles)
    {
        return getInputsForRemoteSources(
                fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSource -> remoteSource.getExchangeType() == REPLICATE)
                        .collect(toImmutableList()),
                handles);
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> getPartitionedExchangeSourceHandles(PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> handles)
    {
        return getInputsForRemoteSources(
                fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSource -> remoteSource.getExchangeType() != REPLICATE)
                        .collect(toImmutableList()),
                handles);
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> getInputsForRemoteSources(
            List<RemoteSourceNode> remoteSources,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> result = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : remoteSources) {
            for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                Collection<ExchangeSourceHandle> handles = requireNonNull(exchangeSourceHandles.get(fragmentId), () -> "exchange source handle is missing for fragment: " + fragmentId);
                result.putAll(remoteSource.getId(), handles);
            }
        }
        return result.build();
    }

    @VisibleForTesting
    static ListMultimap<PlanNodeId, Split> createRemoteSplits(ListMultimap<PlanNodeId, ExchangeSourceHandle> handles)
    {
        return Multimaps.asMap(handles).entrySet().stream()
                .collect(toImmutableListMultimap(Map.Entry::getKey, entry -> createRemoteSplit(entry.getValue())));
    }

    @VisibleForTesting
    static Split createRemoteSplit(Collection<ExchangeSourceHandle> exchangeSourceHandles)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.copyOf(exchangeSourceHandles), Optional.empty())));
    }

    private static class LoadedSplits
    {
        private final PlanNodeId planNodeId;
        private final List<Split> splits;

        private LoadedSplits(PlanNodeId planNodeId, List<Split> splits)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public List<Split> getSplits()
        {
            return splits;
        }
    }

    private static class SplitLoadingFuture
            extends AbstractFuture<LoadedSplits>
    {
        private final PlanNodeId planNodeId;
        private final SplitSource splitSource;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Executor executor;
        @GuardedBy("this")
        private final List<Split> loadedSplits = new ArrayList<>();
        @GuardedBy("this")
        private ListenableFuture<SplitBatch> currentSplitBatch = immediateFuture(null);

        SplitLoadingFuture(PlanNodeId planNodeId, SplitSource splitSource, int splitBatchSize, LongConsumer getSplitTimeRecorder, Executor executor)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.executor = requireNonNull(executor, "executor is null");
        }

        // Called to initiate loading and to load next batch if not finished
        public synchronized void load()
        {
            if (currentSplitBatch == null) {
                checkState(isCancelled(), "SplitLoadingFuture should be in cancelled state");
                return;
            }
            checkState(currentSplitBatch.isDone(), "next batch of splits requested before previous batch is done");
            currentSplitBatch = splitSource.getNextBatch(splitBatchSize);

            long start = System.nanoTime();
            addCallback(
                    currentSplitBatch,
                    new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(SplitBatch splitBatch)
                        {
                            getSplitTimeRecorder.accept(start);
                            synchronized (SplitLoadingFuture.this) {
                                loadedSplits.addAll(splitBatch.getSplits());

                                if (splitBatch.isLastBatch()) {
                                    set(new LoadedSplits(planNodeId, loadedSplits));
                                    try {
                                        splitSource.close();
                                    }
                                    catch (RuntimeException e) {
                                        log.error(e, "Error closing split source");
                                    }
                                }
                                else {
                                    load();
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable)
                        {
                            setException(throwable);
                        }
                    },
                    executor);
        }

        @Override
        protected synchronized void interruptTask()
        {
            if (currentSplitBatch != null) {
                currentSplitBatch.cancel(true);
                currentSplitBatch = null;
            }
        }
    }
}
