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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer is responsible for changing the partition count of hash partitioned fragments
 * at runtime. This uses the runtime output stats from FTE to determine estimated memory consumption
 * and changes the partition count if the estimated memory consumption is higher than the
 * runtimeAdaptivePartitioningMaxTaskSizeInBytes * current partitionCount.
 */
public class AdaptivePartitioning
        implements AdaptivePlanOptimizer
{
    private static final Logger log = Logger.get(AdaptivePartitioning.class);

    @Override
    public Result optimizeAndMarkPlanChanges(PlanNode plan, Context context)
    {
        // Skip if runtime adaptive partitioning is not enabled
        if (!isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled(context.session())) {
            return new Result(plan, ImmutableSet.of());
        }

        int maxPartitionCount = getFaultTolerantExecutionMaxPartitionCount(context.session());
        int runtimeAdaptivePartitioningPartitionCount = getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount(context.session());
        long runtimeAdaptivePartitioningMaxTaskSizeInBytes = getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize(context.session()).toBytes();
        RuntimeInfoProvider runtimeInfoProvider = context.runtimeInfoProvider();
        List<PlanFragment> fragments = runtimeInfoProvider.getAllPlanFragments();

        // Skip if there are already some fragments with the maximum partition count. This is to avoid re-planning
        // since currently we apply this rule on the entire plan. Once, we have a granular way of applying this rule,
        // we can remove this check.
        if (fragments.stream()
                .anyMatch(fragment ->
                        fragment.getPartitionCount().orElse(maxPartitionCount) >= runtimeAdaptivePartitioningPartitionCount)) {
            return new Result(plan, ImmutableSet.of());
        }

        for (PlanFragment fragment : fragments) {
            // Skip if the stage is not consuming hash partitioned input or if the runtime stats are accurate which
            // basically means that the stage can't be re-planned in the current implementation of AdaptivePlaner.
            // TODO: We need add an ability to re-plan fragment whose stats are estimated by progress.
            if (!consumesHashPartitionedInput(fragment) || runtimeInfoProvider.getRuntimeOutputStats(fragment.getId()).isAccurate()) {
                continue;
            }

            int partitionCount = fragment.getPartitionCount().orElse(maxPartitionCount);
            // calculate (estimated) input data size to determine if we want to change number of partitions at runtime
            List<Long> partitionedInputBytes = fragment.getRemoteSourceNodes().stream()
                    // skip for replicate exchange since it's assumed that broadcast join will be chosen by
                    // static optimizer only if build size is small.
                    // TODO: Fix this assumption by using runtime stats
                    .filter(remoteSourceNode -> remoteSourceNode.getExchangeType() != REPLICATE)
                    .map(remoteSourceNode -> remoteSourceNode.getSourceFragmentIds().stream()
                            .mapToLong(sourceFragmentId -> {
                                OutputStatsEstimateResult runtimeStats = runtimeInfoProvider.getRuntimeOutputStats(sourceFragmentId);
                                return runtimeStats.outputDataSizeEstimate().getTotalSizeInBytes();
                            })
                            .sum())
                    .collect(toImmutableList());

            // Currently the memory estimation is simplified:
            // if it's an aggregation, then we use the total input bytes as the memory consumption
            // if it involves multiple joins, conservatively we assume the smallest remote source will be streamed through
            // and use the sum of input bytes of other remote sources as the memory consumption
            // TODO: more accurate memory estimation based on context (https://github.com/trinodb/trino/issues/18698)
            long estimatedMemoryConsumptionInBytes = (partitionedInputBytes.size() == 1) ? partitionedInputBytes.get(0) :
                    partitionedInputBytes.stream().mapToLong(Long::longValue).sum() - Collections.min(partitionedInputBytes);

            if (estimatedMemoryConsumptionInBytes > runtimeAdaptivePartitioningMaxTaskSizeInBytes * partitionCount) {
                log.info("Stage %s has an estimated memory consumption of %s, changing partition count from %s to %s",
                        fragment.getId(), succinctBytes(estimatedMemoryConsumptionInBytes), partitionCount, runtimeAdaptivePartitioningPartitionCount);
                Rewriter rewriter = new Rewriter(runtimeAdaptivePartitioningPartitionCount, context.idAllocator(), runtimeInfoProvider);
                PlanNode planNode = rewriteWith(rewriter, plan);
                return new Result(planNode, rewriter.getChangedPlanIds());
            }
        }

        return new Result(plan, ImmutableSet.of());
    }

    public static boolean consumesHashPartitionedInput(PlanFragment fragment)
    {
        return isPartitioned(fragment.getPartitioning());
    }

    private static boolean isPartitioned(PartitioningHandle partitioningHandle)
    {
        return partitioningHandle.equals(FIXED_HASH_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_HASH_DISTRIBUTION);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final int partitionCount;
        private final PlanNodeIdAllocator idAllocator;
        private final RuntimeInfoProvider runtimeInfoProvider;
        private final Set<PlanNodeId> changedPlanIds = new HashSet<>();

        private Rewriter(int partitionCount, PlanNodeIdAllocator idAllocator, RuntimeInfoProvider runtimeInfoProvider)
        {
            this.partitionCount = partitionCount;
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            if (node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                // the result of the subtree will be broadcast, then no need to change partition count for the subtree
                // as the planner will only broadcast fragment output if it sees input data is small
                // or filter ratio is high
                return node;
            }
            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            PartitioningScheme partitioningScheme = node.getPartitioningScheme();

            // for FTE it only makes sense to set partition count for hash partitioned fragments
            if (node.getScope() == REMOTE
                    && node.getPartitioningScheme().getPartitioning().getHandle() == FIXED_HASH_DISTRIBUTION) {
                partitioningScheme = partitioningScheme.withPartitionCount(Optional.of(partitionCount));
                changedPlanIds.add(node.getId());
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    sources,
                    node.getInputs(),
                    node.getOrderingScheme());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            if (node.getExchangeType() != REPARTITION) {
                return node;
            }

            Optional<PartitioningScheme> sourcePartitioningScheme = node.getSourceFragmentIds().stream()
                    .map(runtimeInfoProvider::getPlanFragment)
                    .map(PlanFragment::getOutputPartitioningScheme)
                    .filter(scheme -> isPartitioned(scheme.getPartitioning().getHandle()))
                    .findFirst();

            if (sourcePartitioningScheme.isEmpty()) {
                return node;
            }

            PartitioningScheme newPartitioningSchema = sourcePartitioningScheme.get()
                    .withPartitionCount(Optional.of(partitionCount))
                    .withPartitioningHandle(FIXED_HASH_DISTRIBUTION);

            PlanNodeId nodeId = idAllocator.getNextId();
            changedPlanIds.add(nodeId);
            return new ExchangeNode(
                    nodeId,
                    REPARTITION,
                    REMOTE,
                    newPartitioningSchema,
                    ImmutableList.of(node),
                    ImmutableList.of(node.getOutputSymbols()),
                    node.getOrderingScheme());
        }

        public Set<PlanNodeId> getChangedPlanIds()
        {
            return ImmutableSet.copyOf(changedPlanIds);
        }
    }
}
