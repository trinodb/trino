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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.exchange.Exchange;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionHashDistributionComputeTaskTargetSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionHashDistributionWriteTaskTargetSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionStandardSplitSize;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.lang.Math.round;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class EventDrivenTaskSourceFactory
{
    private final SplitSourceFactory splitSourceFactory;
    private final Executor executor;
    private final InternalNodeManager nodeManager;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final int splitBatchSize;

    @Inject
    public EventDrivenTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            @ForQueryExecution ExecutorService executor,
            InternalNodeManager nodeManager,
            TableExecuteContextManager tableExecuteContextManager,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                splitSourceFactory,
                executor,
                nodeManager,
                tableExecuteContextManager,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public EventDrivenTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            Executor executor,
            InternalNodeManager nodeManager,
            TableExecuteContextManager tableExecuteContextManager,
            int splitBatchSize)
    {
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitBatchSize = splitBatchSize;
    }

    public EventDrivenTaskSource create(
            Session session,
            Span stageSpan,
            PlanFragment fragment,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            LongConsumer getSplitTimeRecorder,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates)
    {
        ImmutableSetMultimap.Builder<PlanNodeId, PlanFragmentId> remoteSources = ImmutableSetMultimap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId sourceFragment : remoteSource.getSourceFragmentIds()) {
                remoteSources.put(remoteSource.getId(), sourceFragment);
            }
        }
        long standardSplitSizeInBytes = getFaultTolerantExecutionStandardSplitSize(session).toBytes();
        int maxTaskSplitCount = getFaultTolerantExecutionMaxTaskSplitCount(session);
        return new EventDrivenTaskSource(
                session.getQueryId(),
                tableExecuteContextManager,
                sourceExchanges,
                remoteSources.build(),
                () -> splitSourceFactory.createSplitSources(session, stageSpan, fragment),
                createSplitAssigner(
                        session,
                        fragment,
                        outputDataSizeEstimates,
                        sourcePartitioningScheme,
                        standardSplitSizeInBytes,
                        maxTaskSplitCount),
                executor,
                splitBatchSize,
                standardSplitSizeInBytes,
                sourcePartitioningScheme,
                getSplitTimeRecorder);
    }

    private SplitAssigner createSplitAssigner(
            Session session,
            PlanFragment fragment,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            long standardSplitSizeInBytes,
            int maxArbitraryDistributionTaskSplitCount)
    {
        PartitioningHandle partitioning = fragment.getPartitioning();

        Set<PlanNodeId> partitionedRemoteSources = fragment.getRemoteSourceNodes().stream()
                .filter(node -> node.getExchangeType() != REPLICATE)
                .map(PlanNode::getId)
                .collect(toImmutableSet());
        Set<PlanNodeId> partitionedSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedRemoteSources)
                .addAll(fragment.getPartitionedSources())
                .build();
        Set<PlanNodeId> replicatedSources = fragment.getRemoteSourceNodes().stream()
                .filter(node -> node.getExchangeType() == REPLICATE)
                .map(PlanNode::getId)
                .collect(toImmutableSet());

        boolean coordinatorOnly = partitioning.equals(COORDINATOR_DISTRIBUTION);
        if (partitioning.equals(SINGLE_DISTRIBUTION) || coordinatorOnly) {
            ImmutableSet<HostAddress> hostRequirement = ImmutableSet.of();
            if (coordinatorOnly) {
                Node currentNode = nodeManager.getCurrentNode();
                verify(currentNode.isCoordinator(), "current node is expected to be a coordinator");
                hostRequirement = ImmutableSet.of(currentNode.getHostAndPort());
            }
            return new SingleDistributionSplitAssigner(
                    hostRequirement,
                    ImmutableSet.<PlanNodeId>builder()
                            .addAll(partitionedSources)
                            .addAll(replicatedSources)
                            .build());
        }

        int arbitraryDistributionComputeTaskTargetSizeGrowthPeriod = getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthPeriod(session);
        double arbitraryDistributionComputeTaskTargetSizeGrowthFactor = getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeGrowthFactor(session);
        long arbitraryDistributionComputeTaskTargetSizeInBytesMin = getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(session).toBytes();
        long arbitraryDistributionComputeTaskTargetSizeInBytesMax = getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMax(session).toBytes();
        checkArgument(arbitraryDistributionComputeTaskTargetSizeInBytesMax >= arbitraryDistributionComputeTaskTargetSizeInBytesMin,
                "arbitraryDistributionComputeTaskTargetSizeInBytesMax %s should be no smaller than arbitraryDistributionComputeTaskTargetSizeInBytesMin %s",
                arbitraryDistributionComputeTaskTargetSizeInBytesMax, arbitraryDistributionComputeTaskTargetSizeInBytesMin);

        int arbitraryDistributionWriteTaskTargetSizeGrowthPeriod = getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthPeriod(session);
        double arbitraryDistributionWriteTaskTargetSizeGrowthFactor = getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeGrowthFactor(session);
        long arbitraryDistributionWriteTaskTargetSizeInBytesMin = getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMin(session).toBytes();
        long arbitraryDistributionWriteTaskTargetSizeInBytesMax = getFaultTolerantExecutionArbitraryDistributionWriteTaskTargetSizeMax(session).toBytes();
        checkArgument(arbitraryDistributionWriteTaskTargetSizeInBytesMax >= arbitraryDistributionWriteTaskTargetSizeInBytesMin,
                "arbitraryDistributionWriteTaskTargetSizeInBytesMax %s should be larger than arbitraryDistributionWriteTaskTargetSizeInBytesMin %s",
                arbitraryDistributionWriteTaskTargetSizeInBytesMax, arbitraryDistributionWriteTaskTargetSizeInBytesMin);

        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SOURCE_DISTRIBUTION)) {
            return new ArbitraryDistributionSplitAssigner(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    arbitraryDistributionComputeTaskTargetSizeGrowthPeriod,
                    arbitraryDistributionComputeTaskTargetSizeGrowthFactor,
                    arbitraryDistributionComputeTaskTargetSizeInBytesMin,
                    arbitraryDistributionComputeTaskTargetSizeInBytesMax,
                    standardSplitSizeInBytes,
                    maxArbitraryDistributionTaskSplitCount);
        }

        if (partitioning.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION)) {
            return new ArbitraryDistributionSplitAssigner(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    arbitraryDistributionWriteTaskTargetSizeGrowthPeriod,
                    arbitraryDistributionWriteTaskTargetSizeGrowthFactor,
                    arbitraryDistributionWriteTaskTargetSizeInBytesMin,
                    arbitraryDistributionWriteTaskTargetSizeInBytesMax,
                    standardSplitSizeInBytes,
                    maxArbitraryDistributionTaskSplitCount);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent() ||
                (partitioning.getConnectorHandle() instanceof MergePartitioningHandle)) {
            return HashDistributionSplitAssigner.create(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    sourcePartitioningScheme,
                    outputDataSizeEstimates,
                    fragment,
                    getFaultTolerantExecutionHashDistributionComputeTaskTargetSize(session).toBytes(),
                    toIntExact(round(getFaultTolerantExecutionHashDistributionComputeTasksToNodesMinRatio(session) * nodeManager.getAllNodes().getActiveNodes().size())),
                    Integer.MAX_VALUE); // compute tasks are bounded by the number of partitions anyways
        }
        if (partitioning.equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
            return HashDistributionSplitAssigner.create(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    sourcePartitioningScheme,
                    outputDataSizeEstimates,
                    fragment,
                    getFaultTolerantExecutionHashDistributionWriteTaskTargetSize(session).toBytes(),
                    toIntExact(round(getFaultTolerantExecutionHashDistributionWriteTasksToNodesMinRatio(session) * nodeManager.getAllNodes().getActiveNodes().size())),
                    getFaultTolerantExecutionHashDistributionWriteTaskTargetMaxCount(session));
        }

        // other partitioning handles are not expected to be set as a fragment partitioning
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }
}
