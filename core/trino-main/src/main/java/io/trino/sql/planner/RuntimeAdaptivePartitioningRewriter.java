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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public final class RuntimeAdaptivePartitioningRewriter
{
    private RuntimeAdaptivePartitioningRewriter() {}

    public static SubPlan overridePartitionCountRecursively(
            SubPlan subPlan,
            int oldPartitionCount,
            int newPartitionCount,
            PlanFragmentIdAllocator planFragmentIdAllocator,
            PlanNodeIdAllocator planNodeIdAllocator,
            Set<PlanFragmentId> startedFragments)
    {
        PlanFragment fragment = subPlan.getFragment();
        if (startedFragments.contains(fragment.getId())) {
            // already started, nothing to change for subPlan and its descendants
            return subPlan;
        }

        PartitioningScheme outputPartitioningScheme = fragment.getOutputPartitioningScheme();
        if (outputPartitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
            // the result of the subtree will be broadcast, then no need to change partition count for the subtree
            // as the planner will only broadcast fragment output if it sees input data is small or filter ratio is high
            return subPlan;
        }
        if (producesHashPartitionedOutput(fragment)) {
            fragment = fragment.withOutputPartitioningScheme(outputPartitioningScheme.withPartitionCount(Optional.of(newPartitionCount)));
        }

        if (consumesHashPartitionedInput(fragment)) {
            fragment = fragment.withPartitionCount(Optional.of(newPartitionCount));
        }
        else {
            // no input partitioning, then no need to insert extra exchanges to sources
            return new SubPlan(
                    fragment,
                    subPlan.getChildren().stream()
                            .map(child -> overridePartitionCountRecursively(
                                    child,
                                    oldPartitionCount,
                                    newPartitionCount,
                                    planFragmentIdAllocator,
                                    planNodeIdAllocator,
                                    startedFragments))
                            .collect(toImmutableList()));
        }

        // insert extra exchanges to sources
        ImmutableList.Builder<SubPlan> newSources = ImmutableList.builder();
        ImmutableMap.Builder<PlanFragmentId, PlanFragmentId> runtimeAdaptivePlanFragmentIdMapping = ImmutableMap.builder();
        for (SubPlan source : subPlan.getChildren()) {
            PlanFragment sourceFragment = source.getFragment();
            RemoteSourceNode sourceRemoteSourceNode = getOnlyElement(fragment.getRemoteSourceNodes().stream()
                    .filter(remoteSourceNode -> remoteSourceNode.getSourceFragmentIds().contains(sourceFragment.getId()))
                    .iterator());
            requireNonNull(sourceRemoteSourceNode, "sourceRemoteSourceNode is null");
            if (sourceRemoteSourceNode.getExchangeType() == REPLICATE) {
                // since exchange type is REPLICATE, also no need to change partition count for the subtree as the
                // planner will only broadcast fragment output if it sees input data is small or filter ratio is high
                newSources.add(source);
                continue;
            }
            if (!startedFragments.contains(sourceFragment.getId())) {
                // source not started yet, then no need to insert extra exchanges to sources
                newSources.add(overridePartitionCountRecursively(
                        source,
                        oldPartitionCount,
                        newPartitionCount,
                        planFragmentIdAllocator,
                        planNodeIdAllocator,
                        startedFragments));
                runtimeAdaptivePlanFragmentIdMapping.put(sourceFragment.getId(), sourceFragment.getId());
                continue;
            }
            RemoteSourceNode runtimeAdaptiveRemoteSourceNode = new RemoteSourceNode(
                    planNodeIdAllocator.getNextId(),
                    sourceFragment.getId(),
                    sourceFragment.getOutputPartitioningScheme().getOutputLayout(),
                    sourceRemoteSourceNode.getOrderingScheme(),
                    sourceRemoteSourceNode.getExchangeType(),
                    sourceRemoteSourceNode.getRetryPolicy());
            PlanFragment runtimeAdaptivePlanFragment = new PlanFragment(
                    planFragmentIdAllocator.getNextId(),
                    runtimeAdaptiveRemoteSourceNode,
                    sourceFragment.getSymbols(),
                    FIXED_HASH_DISTRIBUTION,
                    Optional.of(oldPartitionCount),
                    ImmutableList.of(), // partitioned sources will be empty as the fragment will only read from `runtimeAdaptiveRemoteSourceNode`
                    sourceFragment.getOutputPartitioningScheme().withPartitionCount(Optional.of(newPartitionCount)),
                    sourceFragment.getStatsAndCosts(),
                    sourceFragment.getActiveCatalogs(),
                    sourceFragment.getJsonRepresentation());
            SubPlan newSource = new SubPlan(
                    runtimeAdaptivePlanFragment,
                    ImmutableList.of(overridePartitionCountRecursively(
                            source,
                            oldPartitionCount,
                            newPartitionCount,
                            planFragmentIdAllocator,
                            planNodeIdAllocator,
                            startedFragments)));
            newSources.add(newSource);
            runtimeAdaptivePlanFragmentIdMapping.put(sourceFragment.getId(), runtimeAdaptivePlanFragment.getId());
        }

        return new SubPlan(
                fragment.withRoot(rewriteWith(
                        new UpdateRemoteSourceFragmentIdsRewriter(runtimeAdaptivePlanFragmentIdMapping.buildOrThrow()),
                        fragment.getRoot())),
                newSources.build());
    }

    public static boolean consumesHashPartitionedInput(PlanFragment fragment)
    {
        return isPartitioned(fragment.getPartitioning());
    }

    public static boolean producesHashPartitionedOutput(PlanFragment fragment)
    {
        return isPartitioned(fragment.getOutputPartitioningScheme().getPartitioning().getHandle());
    }

    public static int getMaxPlanFragmentId(List<SubPlan> subPlans)
    {
        return subPlans.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .mapToInt(fragmentId -> Integer.parseInt(fragmentId.toString()))
                .max()
                .orElseThrow();
    }

    public static int getMaxPlanId(List<SubPlan> subPlans)
    {
        return subPlans.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getRoot)
                .mapToInt(root -> traverse(root)
                        .map(PlanNode::getId)
                        .mapToInt(planNodeId -> Integer.parseInt(planNodeId.toString()))
                        .max()
                        .orElseThrow())
                .max()
                .orElseThrow();
    }

    private static boolean isPartitioned(PartitioningHandle partitioningHandle)
    {
        return partitioningHandle.equals(FIXED_HASH_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_HASH_DISTRIBUTION);
    }

    private static Stream<PlanNode> traverse(PlanNode node)
    {
        Iterable<PlanNode> iterable = Traverser.forTree(PlanNode::getSources).depthFirstPreOrder(node);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private static class UpdateRemoteSourceFragmentIdsRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Map<PlanFragmentId, PlanFragmentId> runtimeAdaptivePlanFragmentIdMapping;

        public UpdateRemoteSourceFragmentIdsRewriter(Map<PlanFragmentId, PlanFragmentId> runtimeAdaptivePlanFragmentIdMapping)
        {
            this.runtimeAdaptivePlanFragmentIdMapping = requireNonNull(runtimeAdaptivePlanFragmentIdMapping, "runtimeAdaptivePlanFragmentIdMapping is null");
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            if (node.getExchangeType() == REPLICATE) {
                return node;
            }
            return node.withSourceFragmentIds(node.getSourceFragmentIds().stream()
                    .map(runtimeAdaptivePlanFragmentIdMapping::get)
                    .collect(toImmutableList()));
        }
    }
}
