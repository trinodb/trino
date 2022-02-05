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
import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
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
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.SplitWeight;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceSplitter;
import io.trino.spi.exchange.ExchangeSourceStatistics;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
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
import java.util.IdentityHashMap;
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
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static com.google.common.collect.Sets.union;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMinTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskInputSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantPreserveInputPartitionsInWriteStage;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

public class StageTaskSourceFactory
        implements TaskSourceFactory
{
    private static final Logger log = Logger.get(StageTaskSourceFactory.class);

    private final SplitSourceFactory splitSourceFactory;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final int splitBatchSize;
    private final Executor executor;

    @Inject
    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            QueryManagerConfig queryManagerConfig,
            @ForQueryExecution ExecutorService executor)
    {
        this(
                splitSourceFactory,
                tableExecuteContextManager,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize(),
                executor);
    }

    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            int splitBatchSize,
            ExecutorService executor)
    {
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitBatchSize = splitBatchSize;
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public TaskSource create(
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
            LongConsumer getSplitTimeRecorder,
            Optional<int[]> bucketToPartitionMap,
            Optional<BucketNodeMap> bucketNodeMap)
    {
        PartitioningHandle partitioning = fragment.getPartitioning();

        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            return SingleDistributionTaskSource.create(session, fragment, exchangeSourceHandles);
        }
        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_DISTRIBUTION)) {
            return ArbitraryDistributionTaskSource.create(
                    session,
                    fragment,
                    sourceExchanges,
                    exchangeSourceHandles,
                    getFaultTolerantExecutionTargetTaskInputSize(session));
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent()) {
            return HashDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    sourceExchanges,
                    exchangeSourceHandles,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap.orElseThrow(() -> new IllegalArgumentException("bucketToPartitionMap is expected to be present for hash distributed stages")),
                    bucketNodeMap,
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
        private final ListMultimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles;

        private boolean finished;
        private DataSize taskMemory;

        public static SingleDistributionTaskSource create(Session session, PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            return new SingleDistributionTaskSource(getInputsForRemoteSources(fragment.getRemoteSourceNodes(), exchangeSourceHandles), getFaultTolerantExecutionDefaultTaskMemory(session));
        }

        @VisibleForTesting
        SingleDistributionTaskSource(ListMultimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles, DataSize taskMemory)
        {
            this.exchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
            this.taskMemory = requireNonNull(taskMemory, "taskMemory is null");
        }

        @Override
        public ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished) {
                return immediateFuture(ImmutableList.of());
            }
            List<TaskDescriptor> result = ImmutableList.of(new TaskDescriptor(
                    0,
                    ImmutableListMultimap.of(),
                    exchangeSourceHandles,
                    new NodeRequirements(Optional.empty(), ImmutableSet.of(), taskMemory)));
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
        private final IdentityHashMap<ExchangeSourceHandle, Exchange> sourceExchanges;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;
        private final long targetPartitionSizeInBytes;
        private DataSize taskMemory;

        private boolean finished;

        public static ArbitraryDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                Map<PlanFragmentId, Exchange> sourceExchanges,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                DataSize targetPartitionSize)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            IdentityHashMap<ExchangeSourceHandle, Exchange> exchangeForHandleMap = getExchangeForHandleMap(sourceExchanges, exchangeSourceHandles);

            return new ArbitraryDistributionTaskSource(
                    exchangeForHandleMap,
                    getPartitionedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    targetPartitionSize,
                    getFaultTolerantExecutionDefaultTaskMemory(session));
        }

        @VisibleForTesting
        ArbitraryDistributionTaskSource(
                IdentityHashMap<ExchangeSourceHandle, Exchange> sourceExchanges,
                Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles,
                Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                DataSize targetPartitionSize,
                DataSize taskMemory)
        {
            this.sourceExchanges = new IdentityHashMap<>(requireNonNull(sourceExchanges, "sourceExchanges is null"));
            this.partitionedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(partitionedExchangeSourceHandles, "partitionedExchangeSourceHandles is null"));
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.taskMemory = requireNonNull(taskMemory, "taskMemory is null");
            checkArgument(
                    sourceExchanges.keySet().containsAll(partitionedExchangeSourceHandles.values()),
                    "Unexpected entries in partitionedExchangeSourceHandles map: %s; allowed keys: %s",
                    partitionedExchangeSourceHandles.values(),
                    sourceExchanges.keySet());
            checkArgument(
                    sourceExchanges.keySet().containsAll(replicatedExchangeSourceHandles.values()),
                    "Unexpected entries in replicatedExchangeSourceHandles map: %s; allowed keys: %s",
                    replicatedExchangeSourceHandles.values(),
                    sourceExchanges.keySet());
            this.targetPartitionSizeInBytes = requireNonNull(targetPartitionSize, "targetPartitionSize is null").toBytes();
        }

        @Override
        public ListenableFuture<List<TaskDescriptor>> getMoreTasks()
        {
            if (finished) {
                return immediateFuture(ImmutableList.of());
            }
            NodeRequirements nodeRequirements = new NodeRequirements(Optional.empty(), ImmutableSet.of(), taskMemory);

            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            int currentPartitionId = 0;

            ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> assignedExchangeSourceHandles = ImmutableListMultimap.builder();
            long assignedExchangeDataSize = 0;
            int assignedExchangeSourceHandleCount = 0;

            for (Map.Entry<PlanNodeId, ExchangeSourceHandle> entry : partitionedExchangeSourceHandles.entries()) {
                PlanNodeId remoteSourcePlanNodeId = entry.getKey();
                ExchangeSourceHandle originalExchangeSourceHandle = entry.getValue();
                Exchange sourceExchange = sourceExchanges.get(originalExchangeSourceHandle);

                ExchangeSourceSplitter splitter = sourceExchange.split(originalExchangeSourceHandle, targetPartitionSizeInBytes);
                ImmutableList.Builder<ExchangeSourceHandle> sourceHandles = ImmutableList.builder();
                while (true) {
                    checkState(splitter.isBlocked().isDone(), "not supported");
                    Optional<ExchangeSourceHandle> next = splitter.getNext();
                    if (next.isEmpty()) {
                        break;
                    }
                    sourceHandles.add(next.get());
                }

                for (ExchangeSourceHandle handle : sourceHandles.build()) {
                    ExchangeSourceStatistics statistics = sourceExchange.getExchangeSourceStatistics(handle);
                    if (assignedExchangeDataSize != 0 && assignedExchangeDataSize + statistics.getSizeInBytes() > targetPartitionSizeInBytes) {
                        assignedExchangeSourceHandles.putAll(replicatedExchangeSourceHandles);
                        result.add(new TaskDescriptor(currentPartitionId++, ImmutableListMultimap.of(), assignedExchangeSourceHandles.build(), nodeRequirements));
                        assignedExchangeSourceHandles = ImmutableListMultimap.builder();
                        assignedExchangeDataSize = 0;
                        assignedExchangeSourceHandleCount = 0;
                    }

                    assignedExchangeSourceHandles.put(remoteSourcePlanNodeId, handle);
                    assignedExchangeDataSize += statistics.getSizeInBytes();
                    assignedExchangeSourceHandleCount++;
                }
            }

            if (assignedExchangeSourceHandleCount > 0) {
                assignedExchangeSourceHandles.putAll(replicatedExchangeSourceHandles);
                result.add(new TaskDescriptor(currentPartitionId, ImmutableListMultimap.of(), assignedExchangeSourceHandles.build(), nodeRequirements));
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
        private final IdentityHashMap<ExchangeSourceHandle, Exchange> exchangeForHandle;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;

        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final int[] bucketToPartitionMap;
        private final Optional<BucketNodeMap> bucketNodeMap;
        private final DataSize taskMemory;
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
                Map<PlanFragmentId, Exchange> sourceExchanges,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int[] bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap,
                long targetPartitionSplitWeight,
                DataSize targetPartitionSourceSize,
                boolean preserveInputPartitionsInWriteStage,
                Executor executor)
        {
            checkArgument(bucketNodeMap.isPresent() || fragment.getPartitionedSources().isEmpty(), "bucketNodeMap is expected to be set when the fragment reads partitioned sources (tables)");
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);

            return new HashDistributionTaskSource(
                    splitSources,
                    getExchangeForHandleMap(sourceExchanges, exchangeSourceHandles),
                    getPartitionedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap,
                    bucketNodeMap,
                    fragment.getPartitioning().getCatalogHandle(),
                    targetPartitionSplitWeight,
                    (preserveInputPartitionsInWriteStage && isWriteFragment(fragment)) ? DataSize.of(0, BYTE) : targetPartitionSourceSize,
                    getFaultTolerantExecutionDefaultTaskMemory(session),
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
                IdentityHashMap<ExchangeSourceHandle, Exchange> exchangeForHandle,
                Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles,
                Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int[] bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap,
                Optional<CatalogHandle> catalogRequirement,
                long targetPartitionSplitWeight,
                DataSize targetPartitionSourceSize,
                DataSize taskMemory,
                Executor executor)
        {
            this.splitSources = ImmutableMap.copyOf(requireNonNull(splitSources, "splitSources is null"));
            this.exchangeForHandle = new IdentityHashMap<>();
            this.exchangeForHandle.putAll(exchangeForHandle);
            this.partitionedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(partitionedExchangeSourceHandles, "partitionedExchangeSourceHandles is null"));
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            this.taskMemory = requireNonNull(taskMemory, "taskMemory is null");
            checkArgument(bucketNodeMap.isPresent() || splitSources.isEmpty(), "bucketNodeMap is expected to be set when the fragment reads partitioned sources (tables)");
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            this.targetPartitionSourceSizeInBytes = requireNonNull(targetPartitionSourceSize, "targetPartitionSourceSize is null").toBytes();
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
                                BucketNodeMap bucketNodeMap = this.bucketNodeMap
                                        .orElseThrow(() -> new VerifyException("bucket to node map is expected to be present"));
                                for (Split split : loadedSplits.getSplits()) {
                                    int bucket = bucketNodeMap.getBucket(split);
                                    int partition = getPartitionForBucket(bucket);

                                    if (!bucketNodeMap.isDynamic()) {
                                        HostAddress requiredAddress = bucketNodeMap.getAssignedNode(split).getHostAndPort();
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

                            Map<Integer, Multimap<PlanNodeId, ExchangeSourceHandle>> partitionToExchangeSourceHandlesMap = new HashMap<>();
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
                                ListMultimap<PlanNodeId, Split> splits = partitionToSplitsMap.getOrDefault(partition, ImmutableListMultimap.of());
                                ListMultimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                                        .putAll(partitionToExchangeSourceHandlesMap.getOrDefault(partition, ImmutableMultimap.of()))
                                        // replicated exchange source will be added in postprocessTasks below
                                        .build();
                                Set<HostAddress> hostRequirement = partitionToNodeMap.get(partition);
                                partitionTasks.add(new TaskDescriptor(taskPartitionId++, splits, exchangeSourceHandles, new NodeRequirements(catalogRequirement, hostRequirement, taskMemory)));
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
            long replicatedExchangeSourcesSize = replicatedExchangeSourceHandles.values().stream().mapToLong(this::sourceHandleSize).sum();
            int taskPartitionId = 0;
            for (Map.Entry<NodeRequirements, Collection<TaskDescriptor>> taskGroup : taskGroups.asMap().entrySet()) {
                NodeRequirements groupNodeRequirements = taskGroup.getKey();
                Collection<TaskDescriptor> groupTasks = taskGroup.getValue();

                ImmutableListMultimap.Builder<PlanNodeId, Split> splits = ImmutableListMultimap.builder();
                ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> exchangeSources = ImmutableListMultimap.builder();
                long splitsWeight = 0;
                long exchangeSourcesSize = 0;

                for (TaskDescriptor task : groupTasks) {
                    ListMultimap<PlanNodeId, Split> taskSplits = task.getSplits();
                    ListMultimap<PlanNodeId, ExchangeSourceHandle> taskExchangeSources = task.getExchangeSourceHandles();
                    long taskSplitWeight = taskSplits.values().stream().mapToLong(split -> split.getSplitWeight().getRawValue()).sum();
                    long taskExchangeSourcesSize = taskExchangeSources.values().stream().mapToLong(this::sourceHandleSize).sum();

                    if ((splitsWeight > 0 || exchangeSourcesSize > 0)
                            && ((splitsWeight + taskSplitWeight) > targetPartitionSplitWeight || (exchangeSourcesSize + taskExchangeSourcesSize + replicatedExchangeSourcesSize) > targetPartitionSourceSizeInBytes)) {
                        exchangeSources.putAll(replicatedExchangeSourceHandles); // add replicated exchanges
                        joinedTasks.add(new TaskDescriptor(taskPartitionId++, splits.build(), exchangeSources.build(), groupNodeRequirements));
                        splits = ImmutableListMultimap.builder();
                        exchangeSources = ImmutableListMultimap.builder();
                        splitsWeight = 0;
                        exchangeSourcesSize = 0;
                    }

                    splits.putAll(taskSplits);
                    exchangeSources.putAll(taskExchangeSources);
                    splitsWeight += taskSplitWeight;
                    exchangeSourcesSize += taskExchangeSourcesSize;
                }

                ImmutableListMultimap<PlanNodeId, Split> remainderSplits = splits.build();
                ImmutableListMultimap<PlanNodeId, ExchangeSourceHandle> remainderExchangeSources = exchangeSources.build();
                if (!remainderSplits.isEmpty() || !remainderExchangeSources.isEmpty()) {
                    remainderExchangeSources = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                            .putAll(remainderExchangeSources)
                            .putAll(replicatedExchangeSourceHandles) // add replicated exchanges
                            .build();
                    joinedTasks.add(new TaskDescriptor(taskPartitionId++, remainderSplits, remainderExchangeSources, groupNodeRequirements));
                }
            }
            return joinedTasks.build();
        }

        private long sourceHandleSize(ExchangeSourceHandle handle)
        {
            Exchange exchange = exchangeForHandle.get(handle);
            ExchangeSourceStatistics exchangeSourceStatistics = exchange.getExchangeSourceStatistics(handle);
            return exchangeSourceStatistics.getSizeInBytes();
        }

        private ListMultimap<NodeRequirements, TaskDescriptor> groupCompatibleTasks(List<TaskDescriptor> tasks)
        {
            return Multimaps.index(tasks, TaskDescriptor::getNodeRequirements);
        }

        private int getPartitionForBucket(int bucket)
        {
            return bucketToPartitionMap[bucket];
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
        private final ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Optional<CatalogHandle> catalogRequirement;
        private final int minPartitionSplitCount;
        private final long targetPartitionSplitWeight;
        private final int maxPartitionSplitCount;
        private final DataSize taskMemory;
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
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    catalogName,
                    minPartitionSplitCount,
                    targetPartitionSplitWeight,
                    maxPartitionSplitCount,
                    getFaultTolerantExecutionDefaultTaskMemory(session),
                    executor);
        }

        @VisibleForTesting
        SourceDistributionTaskSource(
                QueryId queryId,
                PlanNodeId partitionedSourceNodeId,
                TableExecuteContextManager tableExecuteContextManager,
                SplitSource splitSource,
                ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<CatalogHandle> catalogRequirement,
                int minPartitionSplitCount,
                long targetPartitionSplitWeight,
                int maxPartitionSplitCount,
                DataSize taskMemory,
                Executor executor)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.partitionedSourceNodeId = requireNonNull(partitionedSourceNodeId, "partitionedSourceNodeId is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
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
            this.taskMemory = requireNonNull(taskMemory, "taskMemory is null");
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
                                    new NodeRequirements(catalogRequirement, ImmutableSet.of(), taskMemory),
                                    isLastBatch));
                            for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                                readyTasksBuilder.addAll(getReadyTasks(
                                        locallyAccessibleSplitBuffer.get(remoteHost),
                                        locallyAccessibleSplitBuffer.entrySet().stream()
                                                .filter(entry -> !entry.getKey().equals(remoteHost))
                                                .map(Map.Entry::getValue)
                                                .collect(toImmutableList()),
                                        new NodeRequirements(catalogRequirement, ImmutableSet.of(remoteHost), taskMemory),
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
                    ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits).build(),
                    replicatedExchangeSourceHandles,
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

    private static IdentityHashMap<ExchangeSourceHandle, Exchange> getExchangeForHandleMap(
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
    {
        IdentityHashMap<ExchangeSourceHandle, Exchange> exchangeForHandle = new IdentityHashMap<>();
        for (Map.Entry<PlanFragmentId, ExchangeSourceHandle> entry : exchangeSourceHandles.entries()) {
            PlanFragmentId fragmentId = entry.getKey();
            ExchangeSourceHandle handle = entry.getValue();
            Exchange exchange = sourceExchanges.get(fragmentId);
            requireNonNull(exchange, "Exchange not found for fragment " + fragmentId);
            exchangeForHandle.put(handle, exchange);
        }
        return exchangeForHandle;
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

    private static Map<PlanFragmentId, PlanNodeId> getSourceFragmentToRemoteSourceNodeIdMap(List<RemoteSourceNode> remoteSourceNodes)
    {
        ImmutableMap.Builder<PlanFragmentId, PlanNodeId> result = ImmutableMap.builder();
        for (RemoteSourceNode node : remoteSourceNodes) {
            for (PlanFragmentId sourceFragmentId : node.getSourceFragmentIds()) {
                result.put(sourceFragmentId, node.getId());
            }
        }
        return result.buildOrThrow();
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> getInputsForRemoteSources(
            List<RemoteSourceNode> remoteSources,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> result = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : remoteSources) {
            for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                Collection<ExchangeSourceHandle> handles = requireNonNull(exchangeSourceHandles.get(fragmentId), () -> "exchange source handle is missing for fragment: " + fragmentId);
                if (remoteSource.getExchangeType() == GATHER || remoteSource.getExchangeType() == REPLICATE) {
                    checkArgument(handles.size() <= 1, "at most 1 exchange source handle is expected, got: %s", handles);
                }
                result.putAll(remoteSource.getId(), handles);
            }
        }
        return result.build();
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
