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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class HashDistributionSplitAssigner
        implements SplitAssigner
{
    private final Optional<CatalogHandle> catalogRequirement;
    private final Set<PlanNodeId> replicatedSources;
    private final Set<PlanNodeId> allSources;
    private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
    private final Map<Integer, TaskPartition> outputPartitionToTaskPartition;

    private final Set<Integer> createdTaskPartitions = new HashSet<>();
    private final Set<PlanNodeId> completedSources = new HashSet<>();
    private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();

    private int nextTaskPartitionId;

    public static HashDistributionSplitAssigner create(
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            PlanFragment fragment,
            long targetPartitionSizeInBytes)
    {
        if (fragment.getPartitioning().equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
            verify(

                    fragment.getPartitionedSources().isEmpty() && fragment.getRemoteSourceNodes().size() == 1,
                    "SCALED_WRITER_HASH_DISTRIBUTION fragments are expected to have exactly one remote source and no table scans");
        }
        return new HashDistributionSplitAssigner(
                catalogRequirement,
                partitionedSources,
                replicatedSources,
                sourcePartitioningScheme,
                createOutputPartitionToTaskPartition(
                        sourcePartitioningScheme,
                        partitionedSources,
                        outputDataSizeEstimates,
                        targetPartitionSizeInBytes,
                        sourceId -> fragment.getPartitioning().equals(SCALED_WRITER_HASH_DISTRIBUTION),
                        // never merge partitions for table write to avoid running into the maximum writers limit per task
                        !isWriteFragment(fragment)));
    }

    @VisibleForTesting
    HashDistributionSplitAssigner(
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Map<Integer, TaskPartition> outputPartitionToTaskPartition)
    {
        this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
        this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
        allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        this.outputPartitionToTaskPartition = ImmutableMap.copyOf(requireNonNull(outputPartitionToTaskPartition, "outputPartitionToTaskPartition is null"));
    }

    @Override
    public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();

        if (replicatedSources.contains(planNodeId)) {
            replicatedSplits.putAll(planNodeId, splits.values());
            for (Integer partitionId : createdTaskPartitions) {
                assignment.updatePartition(new PartitionUpdate(partitionId, planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits));
            }
        }
        else {
            splits.forEach((outputPartitionId, split) -> {
                TaskPartition taskPartition = outputPartitionToTaskPartition.get(outputPartitionId);
                verify(taskPartition != null, "taskPartition not found for outputPartitionId: %s", outputPartitionId);

                List<SubPartition> subPartitions;
                if (taskPartition.getSplitBy().isPresent() && taskPartition.getSplitBy().get().equals(planNodeId)) {
                    subPartitions = ImmutableList.of(taskPartition.getNextSubPartition());
                }
                else {
                    subPartitions = taskPartition.getSubPartitions();
                }

                for (SubPartition subPartition : subPartitions) {
                    if (!subPartition.isIdAssigned()) {
                        int taskPartitionId = nextTaskPartitionId++;
                        // Assigns lazily to ensure task ids are incremental and with no gaps.
                        // Gaps can occur when scanning over a bucketed table as some buckets may contain no data.
                        subPartition.assignId(taskPartitionId);
                        Set<HostAddress> hostRequirement = sourcePartitioningScheme.getNodeRequirement(outputPartitionId)
                                .map(InternalNode::getHostAndPort)
                                .map(ImmutableSet::of)
                                .orElse(ImmutableSet.of());
                        assignment.addPartition(new Partition(
                                taskPartitionId,
                                new NodeRequirements(catalogRequirement, hostRequirement)));
                        for (PlanNodeId replicatedSource : replicatedSplits.keySet()) {
                            assignment.updatePartition(new PartitionUpdate(taskPartitionId, replicatedSource, replicatedSplits.get(replicatedSource), completedSources.contains(replicatedSource)));
                        }
                        for (PlanNodeId completedSource : completedSources) {
                            assignment.updatePartition(new PartitionUpdate(taskPartitionId, completedSource, ImmutableList.of(), true));
                        }
                        createdTaskPartitions.add(taskPartitionId);
                    }

                    assignment.updatePartition(new PartitionUpdate(subPartition.getId(), planNodeId, ImmutableList.of(split), false));
                }
            });
        }

        if (noMoreSplits) {
            completedSources.add(planNodeId);
            for (Integer taskPartition : createdTaskPartitions) {
                assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, ImmutableList.of(), true));
            }
            if (completedSources.containsAll(allSources)) {
                if (createdTaskPartitions.isEmpty()) {
                    assignment.addPartition(new Partition(
                            0,
                            new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                    for (PlanNodeId replicatedSource : replicatedSplits.keySet()) {
                        assignment.updatePartition(new PartitionUpdate(0, replicatedSource, replicatedSplits.get(replicatedSource), true));
                    }
                    for (PlanNodeId completedSource : completedSources) {
                        assignment.updatePartition(new PartitionUpdate(0, completedSource, ImmutableList.of(), true));
                    }
                    createdTaskPartitions.add(0);
                }
                for (Integer taskPartition : createdTaskPartitions) {
                    assignment.sealPartition(taskPartition);
                }
                assignment.setNoMorePartitions();
                replicatedSplits.clear();
            }
        }

        return assignment.build();
    }

    @Override
    public AssignmentResult finish()
    {
        checkState(!createdTaskPartitions.isEmpty(), "createdTaskPartitions is not expected to be empty");
        return AssignmentResult.builder().build();
    }

    @VisibleForTesting
    static Map<Integer, TaskPartition> createOutputPartitionToTaskPartition(
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Set<PlanNodeId> partitionedSources,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            long targetPartitionSizeInBytes,
            Predicate<PlanNodeId> canSplit,
            boolean canMerge)
    {
        int partitionCount = sourcePartitioningScheme.getPartitionCount();
        if (sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent() ||
                partitionedSources.isEmpty() ||
                !outputDataSizeEstimates.keySet().containsAll(partitionedSources)) {
            // if bucket scheme is set explicitly or if estimates are missing create one task partition per output partition
            return IntStream.range(0, partitionCount)
                    .boxed()
                    .collect(toImmutableMap(Function.identity(), (key) -> new TaskPartition(1, Optional.empty())));
        }

        List<OutputDataSizeEstimate> partitionedSourcesEstimates = outputDataSizeEstimates.entrySet().stream()
                .filter(entry -> partitionedSources.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        OutputDataSizeEstimate mergedEstimate = OutputDataSizeEstimate.merge(partitionedSourcesEstimates);
        ImmutableMap.Builder<Integer, TaskPartition> result = ImmutableMap.builder();
        PriorityQueue<PartitionAssignment> assignments = new PriorityQueue<>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            long partitionSizeInBytes = mergedEstimate.getPartitionSizeInBytes(partitionId);
            if (assignments.isEmpty() || assignments.peek().assignedDataSizeInBytes() + partitionSizeInBytes > targetPartitionSizeInBytes || !canMerge) {
                TaskPartition taskPartition = createTaskPartition(
                        partitionSizeInBytes,
                        targetPartitionSizeInBytes,
                        partitionedSources,
                        outputDataSizeEstimates,
                        partitionId,
                        canSplit);
                result.put(partitionId, taskPartition);
                assignments.add(new PartitionAssignment(taskPartition, partitionSizeInBytes));
            }
            else {
                PartitionAssignment assignment = assignments.poll();
                result.put(partitionId, assignment.taskPartition());
                assignments.add(new PartitionAssignment(assignment.taskPartition(), assignment.assignedDataSizeInBytes() + partitionSizeInBytes));
            }
        }
        return result.buildOrThrow();
    }

    private static TaskPartition createTaskPartition(
            long partitionSizeInBytes,
            long targetPartitionSizeInBytes,
            Set<PlanNodeId> partitionedSources,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            int partitionId,
            Predicate<PlanNodeId> canSplit)
    {
        if (partitionSizeInBytes > targetPartitionSizeInBytes) {
            // try to assign multiple sub-partitions if possible
            Map<PlanNodeId, Long> sourceSizes = getSourceSizes(partitionedSources, outputDataSizeEstimates, partitionId);
            PlanNodeId largestSource = sourceSizes.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElseThrow();
            long largestSourceSizeInBytes = sourceSizes.get(largestSource);
            long remainingSourcesSizeInBytes = partitionSizeInBytes - largestSourceSizeInBytes;
            if (remainingSourcesSizeInBytes <= targetPartitionSizeInBytes / 4 && canSplit.test(largestSource)) {
                long targetLargestSourceSizeInBytes = targetPartitionSizeInBytes - remainingSourcesSizeInBytes;
                return new TaskPartition(toIntExact(largestSourceSizeInBytes / targetLargestSourceSizeInBytes) + 1, Optional.of(largestSource));
            }
        }
        return new TaskPartition(1, Optional.empty());
    }

    private static Map<PlanNodeId, Long> getSourceSizes(Set<PlanNodeId> partitionedSources, Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates, int partitionId)
    {
        return partitionedSources.stream()
                .collect(toImmutableMap(Function.identity(), source -> outputDataSizeEstimates.get(source).getPartitionSizeInBytes(partitionId)));
    }

    private record PartitionAssignment(TaskPartition taskPartition, long assignedDataSizeInBytes)
            implements Comparable<PartitionAssignment>
    {
        public PartitionAssignment(TaskPartition taskPartition, long assignedDataSizeInBytes)
        {
            this.taskPartition = requireNonNull(taskPartition, "taskPartition is null");
            this.assignedDataSizeInBytes = assignedDataSizeInBytes;
        }

        @Override
        public int compareTo(PartitionAssignment other)
        {
            return Long.compare(assignedDataSizeInBytes, other.assignedDataSizeInBytes);
        }
    }

    @VisibleForTesting
    static class TaskPartition
    {
        private final List<SubPartition> subPartitions;
        private final Optional<PlanNodeId> splitBy;

        private int nextSubPartition;

        private TaskPartition(int subPartitionCount, Optional<PlanNodeId> splitBy)
        {
            checkArgument(subPartitionCount > 0, "subPartitionCount is expected to be greater than zero");
            subPartitions = IntStream.range(0, subPartitionCount)
                    .mapToObj(i -> new SubPartition())
                    .collect(toImmutableList());
            checkArgument(subPartitionCount == 1 || splitBy.isPresent(), "splitBy is expected to be present when subPartitionCount is greater than 1");
            this.splitBy = requireNonNull(splitBy, "splitBy is null");
        }

        public SubPartition getNextSubPartition()
        {
            SubPartition result = subPartitions.get(nextSubPartition);
            nextSubPartition = (nextSubPartition + 1) % subPartitions.size();
            return result;
        }

        public List<SubPartition> getSubPartitions()
        {
            return subPartitions;
        }

        public Optional<PlanNodeId> getSplitBy()
        {
            return splitBy;
        }
    }

    @VisibleForTesting
    static class SubPartition
    {
        private OptionalInt id = OptionalInt.empty();

        public void assignId(int id)
        {
            checkState(this.id.isEmpty(), "id is already assigned");
            this.id = OptionalInt.of(id);
        }

        public boolean isIdAssigned()
        {
            return id.isPresent();
        }

        public int getId()
        {
            checkState(id.isPresent(), "id is expected to be assigned");
            return id.getAsInt();
        }
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
}
