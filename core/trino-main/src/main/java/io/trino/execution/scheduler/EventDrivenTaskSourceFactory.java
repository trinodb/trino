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

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskInputSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskSplitCount;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
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
        long targetPartitionSizeInBytes = getFaultTolerantExecutionTargetTaskInputSize(session).toBytes();
        // TODO: refactor to define explicitly
        long standardSplitSizeInBytes = targetPartitionSizeInBytes / getFaultTolerantExecutionTargetTaskSplitCount(session);
        int maxTaskSplitCount = getFaultTolerantExecutionMaxTaskSplitCount(session);
        return new EventDrivenTaskSource(
                session.getQueryId(),
                tableExecuteContextManager,
                sourceExchanges,
                remoteSources.build(),
                () -> splitSourceFactory.createSplitSources(session, fragment),
                createSplitAssigner(
                        session,
                        fragment,
                        outputDataSizeEstimates,
                        sourcePartitioningScheme,
                        targetPartitionSizeInBytes,
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
            long targetPartitionSizeInBytes,
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
        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) || partitioning.equals(SOURCE_DISTRIBUTION)) {
            return new ArbitraryDistributionSplitAssigner(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    targetPartitionSizeInBytes,
                    standardSplitSizeInBytes,
                    maxArbitraryDistributionTaskSplitCount);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent() ||
                (partitioning.getConnectorHandle() instanceof MergePartitioningHandle) ||
                partitioning.equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
            return HashDistributionSplitAssigner.create(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    sourcePartitioningScheme,
                    outputDataSizeEstimates,
                    fragment,
                    getFaultTolerantExecutionTargetTaskInputSize(session).toBytes());
        }

        // other partitioning handles are not expected to be set as a fragment partitioning
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }
}
