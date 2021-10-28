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

import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
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
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static com.google.common.collect.Sets.union;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskInputSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskSplitCount;
import static io.trino.connector.CatalogName.isInternalSystemConnector;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
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

    @Inject
    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                splitSourceFactory,
                tableExecuteContextManager,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public StageTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            TableExecuteContextManager tableExecuteContextManager,
            int splitBatchSize)
    {
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitBatchSize = splitBatchSize;
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
            return SingleDistributionTaskSource.create(fragment, exchangeSourceHandles);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_DISTRIBUTION)) {
            return ArbitraryDistributionTaskSource.create(
                    fragment,
                    sourceExchanges,
                    exchangeSourceHandles,
                    getFaultTolerantExecutionTargetTaskInputSize(session));
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            return HashDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    exchangeSourceHandles,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap.orElseThrow(() -> new IllegalArgumentException("bucketToPartitionMap is expected to be present for hash distributed stages")),
                    bucketNodeMap);
        }
        else if (partitioning.equals(SOURCE_DISTRIBUTION)) {
            return SourceDistributionTaskSource.create(
                    session,
                    fragment,
                    splitSourceFactory,
                    exchangeSourceHandles,
                    tableExecuteContextManager,
                    splitBatchSize,
                    getSplitTimeRecorder,
                    getFaultTolerantExecutionTargetTaskSplitCount(session));
        }

        // other partitioning handles are not expected to be set as a fragment partitioning
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }

    public static class SingleDistributionTaskSource
            implements TaskSource
    {
        private final Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles;

        private boolean finished;

        public static SingleDistributionTaskSource create(PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            return new SingleDistributionTaskSource(getInputsForRemoteSources(fragment.getRemoteSourceNodes(), exchangeSourceHandles));
        }

        public SingleDistributionTaskSource(Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles)
        {
            this.exchangeSourceHandles = ImmutableMultimap.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        }

        @Override
        public List<TaskDescriptor> getMoreTasks()
        {
            List<TaskDescriptor> result = ImmutableList.of(new TaskDescriptor(
                    0,
                    ImmutableMultimap.of(),
                    exchangeSourceHandles,
                    new NodeRequirements(Optional.empty(), ImmutableSet.of())));
            finished = true;
            return result;
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
        private final Map<PlanFragmentId, PlanNodeId> sourceFragmentToRemoteSourceNodeIdMap;
        private final Map<PlanFragmentId, Exchange> sourceExchanges;
        private final Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles;
        private final long targetPartitionSizeInBytes;

        private boolean finished;

        public static ArbitraryDistributionTaskSource create(
                PlanFragment fragment,
                Map<PlanFragmentId, Exchange> sourceExchanges,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                DataSize targetPartitionSize)
        {
            checkArgument(fragment.getPartitionedSources().isEmpty(), "no partitioned sources (table scans) expected, got: %s", fragment.getPartitionedSources());
            checkArgument(fragment.getRemoteSourceNodes().stream().noneMatch(node -> node.getExchangeType() == REPLICATE), "replicated exchanges are not expected in source distributed stage, got: %s", fragment.getRemoteSourceNodes());

            return new ArbitraryDistributionTaskSource(
                    getSourceFragmentToRemoteSourceNodeIdMap(fragment.getRemoteSourceNodes()),
                    sourceExchanges,
                    exchangeSourceHandles,
                    targetPartitionSize);
        }

        public ArbitraryDistributionTaskSource(
                Map<PlanFragmentId, PlanNodeId> sourceFragmentToRemoteSourceNodeIdMap,
                Map<PlanFragmentId, Exchange> sourceExchanges,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                DataSize targetPartitionSize)
        {
            this.sourceFragmentToRemoteSourceNodeIdMap = ImmutableMap.copyOf(requireNonNull(sourceFragmentToRemoteSourceNodeIdMap, "sourceFragmentToRemoteSourceNodeIdMap is null"));
            this.sourceExchanges = ImmutableMap.copyOf(requireNonNull(sourceExchanges, "sourceExchanges is null"));
            checkArgument(
                    sourceFragmentToRemoteSourceNodeIdMap.keySet().equals(sourceExchanges.keySet()),
                    "sourceFragmentToRemoteSourceNodeIdMap and sourceExchanges are expected to have the same set of keys: %s != %s",
                    sourceFragmentToRemoteSourceNodeIdMap.keySet(),
                    sourceExchanges.keySet());
            this.exchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
            checkArgument(
                    sourceExchanges.keySet().containsAll(exchangeSourceHandles.keySet()),
                    "Unexpected keys in exchangeSourceHandles map: %s; allowed keys: %s",
                    exchangeSourceHandles.keySet(),
                    sourceExchanges.keySet());
            this.targetPartitionSizeInBytes = requireNonNull(targetPartitionSize, "targetPartitionSize is null").toBytes();
        }

        @Override
        public List<TaskDescriptor> getMoreTasks()
        {
            NodeRequirements nodeRequirements = new NodeRequirements(Optional.empty(), ImmutableSet.of());

            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            int currentPartitionId = 0;

            ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> assignedExchangeSourceHandles = ImmutableListMultimap.builder();
            long assignedExchangeDataSize = 0;
            int assignedExchangeSourceHandleCount = 0;

            for (Map.Entry<PlanFragmentId, ExchangeSourceHandle> entry : exchangeSourceHandles.entries()) {
                PlanFragmentId sourceFragmentId = entry.getKey();
                PlanNodeId remoteSourcePlanNodeId = sourceFragmentToRemoteSourceNodeIdMap.get(sourceFragmentId);
                ExchangeSourceHandle originalExchangeSourceHandle = entry.getValue();
                Exchange sourceExchange = sourceExchanges.get(sourceFragmentId);

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
                result.add(new TaskDescriptor(currentPartitionId, ImmutableListMultimap.of(), assignedExchangeSourceHandles.build(), nodeRequirements));
            }

            finished = true;
            return result.build();
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
        private final Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles;
        private final Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final int[] bucketToPartitionMap;
        private final Optional<BucketNodeMap> bucketNodeMap;
        private final Optional<CatalogName> catalogRequirement;

        private boolean finished;
        private boolean closed;

        public static HashDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                SplitSourceFactory splitSourceFactory,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int[] bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap)
        {
            checkArgument(bucketNodeMap.isPresent() || fragment.getPartitionedSources().isEmpty(), "bucketNodeMap is expected to be set when the fragment reads partitioned sources (tables)");
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            return new HashDistributionTaskSource(
                    splitSources,
                    getPartitionedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    bucketToPartitionMap,
                    bucketNodeMap,
                    fragment.getPartitioning().getConnectorId());
        }

        public HashDistributionTaskSource(
                Map<PlanNodeId, SplitSource> splitSources,
                Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSourceHandles,
                Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int[] bucketToPartitionMap,
                Optional<BucketNodeMap> bucketNodeMap,
                Optional<CatalogName> catalogRequirement)
        {
            this.splitSources = ImmutableMap.copyOf(requireNonNull(splitSources, "splitSources is null"));
            this.partitionedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(partitionedExchangeSourceHandles, "partitionedExchangeSourceHandles is null"));
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            checkArgument(bucketNodeMap.isPresent() || splitSources.isEmpty(), "bucketNodeMap is expected to be set when the fragment reads partitioned sources (tables)");
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
        }

        @Override
        public List<TaskDescriptor> getMoreTasks()
        {
            if (finished || closed) {
                return ImmutableList.of();
            }

            Map<Integer, Multimap<PlanNodeId, Split>> partitionToSplitsMap = new HashMap<>();
            Map<Integer, HostAddress> partitionToNodeMap = new HashMap<>();
            for (Map.Entry<PlanNodeId, SplitSource> entry : splitSources.entrySet()) {
                SplitSource splitSource = entry.getValue();
                BucketNodeMap bucketNodeMap = this.bucketNodeMap
                        .orElseThrow(() -> new VerifyException("bucket to node map is expected to be present"));
                while (!splitSource.isFinished()) {
                    ListenableFuture<SplitBatch> splitBatchFuture = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), splitBatchSize);

                    long start = System.nanoTime();
                    addSuccessCallback(splitBatchFuture, () -> getSplitTimeRecorder.accept(start));

                    SplitBatch splitBatch = getFutureValue(splitBatchFuture);

                    for (Split split : splitBatch.getSplits()) {
                        int bucket = bucketNodeMap.getBucket(split);
                        int partition = getPartitionForBucket(bucket);

                        if (!bucketNodeMap.isDynamic()) {
                            HostAddress existingValue = partitionToNodeMap.put(partition, bucketNodeMap.getAssignedNode(split).get().getHostAndPort());
                            checkState(existingValue == null, "host already assigned for partition %s: %s", partition, existingValue);
                        }

                        Multimap<PlanNodeId, Split> partitionSplits = partitionToSplitsMap.computeIfAbsent(partition, (p) -> ArrayListMultimap.create());
                        partitionSplits.put(entry.getKey(), split);
                    }

                    if (splitBatch.isLastBatch()) {
                        splitSource.close();
                        break;
                    }
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
            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            for (Integer partition : union(partitionToSplitsMap.keySet(), partitionToExchangeSourceHandlesMap.keySet())) {
                Multimap<PlanNodeId, Split> splits = partitionToSplitsMap.getOrDefault(partition, ImmutableMultimap.of());
                Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                        .putAll(partitionToExchangeSourceHandlesMap.getOrDefault(partition, ImmutableMultimap.of()))
                        .putAll(replicatedExchangeSourceHandles)
                        .build();
                HostAddress host = partitionToNodeMap.get(partition);
                Set<HostAddress> hostRequirement = host == null ? ImmutableSet.of() : ImmutableSet.of(host);
                result.add(new TaskDescriptor(taskPartitionId++, splits, exchangeSourceHandles, new NodeRequirements(catalogRequirement, hostRequirement)));
            }

            finished = true;
            return result.build();
        }

        private int getPartitionForBucket(int bucket)
        {
            return bucketToPartitionMap[bucket];
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public void close()
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
        private final Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;
        private final Optional<CatalogName> catalogRequirement;
        private final int targetPartitionSplitCount;

        private final Queue<Split> remotelyAccessibleSplitBuffer = new ArrayDeque<>();
        private final Map<HostAddress, Set<Split>> locallyAccessibleSplitBuffer = new HashMap<>();

        private int currentPartitionId;
        private boolean finished;
        private boolean closed;

        public static SourceDistributionTaskSource create(
                Session session,
                PlanFragment fragment,
                SplitSourceFactory splitSourceFactory,
                Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
                TableExecuteContextManager tableExecuteContextManager,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                int targetPartitionSplitCount)
        {
            checkArgument(fragment.getPartitionedSources().size() == 1, "single partitioned source is expected, got: %s", fragment.getPartitionedSources());

            List<RemoteSourceNode> remoteSourceNodes = fragment.getRemoteSourceNodes();
            checkArgument(remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE), "only replicated exchanges are expected in source distributed stage, got: %s", remoteSourceNodes);

            PlanNodeId partitionedSourceNodeId = getOnlyElement(fragment.getPartitionedSources());
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            SplitSource splitSource = splitSources.get(partitionedSourceNodeId);

            Optional<CatalogName> catalogName = Optional.of(splitSource.getCatalogName())
                    .filter(catalog -> !isInternalSystemConnector(catalog));

            return new SourceDistributionTaskSource(
                    session.getQueryId(),
                    partitionedSourceNodeId,
                    tableExecuteContextManager,
                    splitSource,
                    getReplicatedExchangeSourceHandles(fragment, exchangeSourceHandles),
                    splitBatchSize,
                    getSplitTimeRecorder,
                    catalogName,
                    targetPartitionSplitCount);
        }

        public SourceDistributionTaskSource(
                QueryId queryId,
                PlanNodeId partitionedSourceNodeId,
                TableExecuteContextManager tableExecuteContextManager,
                SplitSource splitSource,
                Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSourceHandles,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder,
                Optional<CatalogName> catalogRequirement,
                int targetPartitionSplitCount)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.partitionedSourceNodeId = requireNonNull(partitionedSourceNodeId, "partitionedSourceNodeId is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.replicatedExchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(replicatedExchangeSourceHandles, "replicatedExchangeSourceHandles is null"));
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            checkArgument(targetPartitionSplitCount > 0, "targetPartitionSplitCount must be positive: %s", targetPartitionSplitCount);
            this.targetPartitionSplitCount = targetPartitionSplitCount;
        }

        @Override
        public List<TaskDescriptor> getMoreTasks()
        {
            if (finished || closed) {
                return ImmutableList.of();
            }

            while (true) {
                if (remotelyAccessibleSplitBuffer.size() >= targetPartitionSplitCount) {
                    ImmutableList.Builder<Split> splits = ImmutableList.builder();
                    for (int i = 0; i < targetPartitionSplitCount; i++) {
                        splits.add(remotelyAccessibleSplitBuffer.poll());
                    }
                    return ImmutableList.of(
                            new TaskDescriptor(
                                    currentPartitionId++,
                                    ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits.build()).build(),
                                    replicatedExchangeSourceHandles,
                                    new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                }
                for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                    Set<Split> hostSplits = locallyAccessibleSplitBuffer.get(remoteHost);
                    if (hostSplits.size() >= targetPartitionSplitCount) {
                        List<Split> splits = removeN(hostSplits, targetPartitionSplitCount);
                        locallyAccessibleSplitBuffer.values().forEach(otherHostSplits -> splits.forEach(otherHostSplits::remove));
                        return ImmutableList.of(
                                new TaskDescriptor(
                                        currentPartitionId++,
                                        ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits).build(),
                                        replicatedExchangeSourceHandles,
                                        new NodeRequirements(catalogRequirement, ImmutableSet.of(remoteHost))));
                    }
                }

                if (splitSource.isFinished()) {
                    break;
                }

                ListenableFuture<SplitBatch> splitBatchFuture = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), splitBatchSize);

                long start = System.nanoTime();
                addSuccessCallback(splitBatchFuture, () -> getSplitTimeRecorder.accept(start));

                List<Split> splits = getFutureValue(splitBatchFuture).getSplits();

                for (Split split : splits) {
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
            }

            Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();

            // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
            tableExecuteSplitsInfo.ifPresent(info -> {
                TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
                tableExecuteContext.setSplitsInfo(info);
            });

            finished = true;
            splitSource.close();

            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();

            if (!remotelyAccessibleSplitBuffer.isEmpty()) {
                result.add(new TaskDescriptor(
                        currentPartitionId++,
                        ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, ImmutableList.copyOf(remotelyAccessibleSplitBuffer)).build(),
                        replicatedExchangeSourceHandles,
                        new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                remotelyAccessibleSplitBuffer.clear();
            }

            if (!locallyAccessibleSplitBuffer.isEmpty()) {
                for (HostAddress remoteHost : locallyAccessibleSplitBuffer.keySet()) {
                    List<Split> splits = ImmutableList.copyOf(locallyAccessibleSplitBuffer.get(remoteHost));
                    if (!splits.isEmpty()) {
                        locallyAccessibleSplitBuffer.values().forEach(otherHostSplits -> splits.forEach(otherHostSplits::remove));
                        result.add(new TaskDescriptor(
                                currentPartitionId++,
                                ImmutableListMultimap.<PlanNodeId, Split>builder().putAll(partitionedSourceNodeId, splits).build(),
                                replicatedExchangeSourceHandles,
                                new NodeRequirements(catalogRequirement, ImmutableSet.of(remoteHost))));
                    }
                }
                locallyAccessibleSplitBuffer.clear();
            }

            return result.build();
        }

        private static <T> List<T> removeN(Collection<T> collection, int n)
        {
            ImmutableList.Builder<T> result = ImmutableList.builder();
            Iterator<T> iterator = collection.iterator();
            for (int i = 0; i < n && iterator.hasNext(); i++) {
                T item = iterator.next();
                iterator.remove();
                result.add(item);
            }
            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            splitSource.close();
        }
    }

    private static Multimap<PlanNodeId, ExchangeSourceHandle> getReplicatedExchangeSourceHandles(PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> handles)
    {
        return getInputsForRemoteSources(
                fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSource -> remoteSource.getExchangeType() == REPLICATE)
                        .collect(toImmutableList()),
                handles);
    }

    private static Multimap<PlanNodeId, ExchangeSourceHandle> getPartitionedExchangeSourceHandles(PlanFragment fragment, Multimap<PlanFragmentId, ExchangeSourceHandle> handles)
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
        return result.build();
    }

    private static Multimap<PlanNodeId, ExchangeSourceHandle> getInputsForRemoteSources(
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
}
