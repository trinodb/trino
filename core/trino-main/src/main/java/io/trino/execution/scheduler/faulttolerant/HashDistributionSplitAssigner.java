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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.execution.scheduler.OutputDataSizeEstimate;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class HashDistributionSplitAssigner
        implements SplitAssigner
{
    private final PlanFragmentId fragmentId;
    private final Optional<CatalogHandle> catalogRequirement;
    private final Set<PlanNodeId> replicatedSources;
    private final Set<PlanNodeId> allSources;
    private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
    private final Map<Integer, TaskPartition> sourcePartitionToTaskPartition;

    private final Set<Integer> createdTaskPartitions = new HashSet<>();
    private final Set<PlanNodeId> completedSources = new HashSet<>();

    private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();

    private boolean allTaskPartitionsCreated;

    public static HashDistributionSplitAssigner create(
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates,
            PlanFragment fragment,
            long targetPartitionSizeInBytes,
            int targetMinTaskCount,
            int targetMaxTaskCount)
    {
        if (fragment.getPartitioning().isScaleWriters()) {
            verify(fragment.getPartitionedSources().isEmpty() && fragment.getRemoteSourceNodes().size() == 1,
                    "fragments using scale-writers partitioning are expected to have exactly one remote source and no table scans");
        }
        return new HashDistributionSplitAssigner(
                fragment.getId(),
                catalogRequirement,
                partitionedSources,
                replicatedSources,
                sourcePartitioningScheme,
                createSourcePartitionToTaskPartition(
                        sourcePartitioningScheme,
                        partitionedSources,
                        sourceDataSizeEstimates,
                        targetPartitionSizeInBytes,
                        targetMinTaskCount,
                        targetMaxTaskCount,
                        sourceId -> fragment.getPartitioning().isScaleWriters(),
                        // never merge partitions for table write to avoid running into the maximum writers limit per task
                        !isWriteFragment(fragment)));
    }

    @VisibleForTesting
    HashDistributionSplitAssigner(
            PlanFragmentId fragmentId,
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Map<Integer, TaskPartition> sourcePartitionToTaskPartition)
    {
        this.fragmentId = requireNonNull(fragmentId, "fragmentId is null");
        this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
        this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
        this.allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        this.sourcePartitionToTaskPartition = ImmutableMap.copyOf(requireNonNull(sourcePartitionToTaskPartition, "sourcePartitionToTaskPartition is null"));
    }

    @Override
    public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();

        if (!allTaskPartitionsCreated) {
            // create tasks all at once
            int nextTaskPartitionId = 0;
            for (int sourcePartitionId = 0; sourcePartitionId < sourcePartitioningScheme.getPartitionCount(); sourcePartitionId++) {
                TaskPartition taskPartition = sourcePartitionToTaskPartition.get(sourcePartitionId);
                verify(taskPartition != null, "taskPartition not found for fragment %s plan node %s for sourcePartitionId %s", fragmentId, planNodeId, sourcePartitionId);

                for (SubPartition subPartition : taskPartition.getSubPartitions()) {
                    if (!subPartition.isIdAssigned()) {
                        int taskPartitionId = nextTaskPartitionId++;
                        subPartition.assignId(taskPartitionId);
                        Optional<HostAddress> hostRequirement = sourcePartitioningScheme.getNodeRequirement(sourcePartitionId)
                                .map(InternalNode::getHostAndPort);
                        assignment.addPartition(new Partition(
                                taskPartitionId,
                                new NodeRequirements(catalogRequirement, hostRequirement, hostRequirement.isEmpty())));
                        createdTaskPartitions.add(taskPartitionId);
                    }
                }
            }
            assignment.setNoMorePartitions();

            allTaskPartitionsCreated = true;
        }

        if (replicatedSources.contains(planNodeId)) {
            replicatedSplits.putAll(planNodeId, splits.values());
            for (Integer partitionId : createdTaskPartitions) {
                assignment.updatePartition(new PartitionUpdate(partitionId, planNodeId, false, replicatedSourcePartition(ImmutableList.copyOf(splits.values())), noMoreSplits));
            }
        }
        else {
            splits.forEach((sourcePartitionId, split) -> {
                TaskPartition taskPartition = sourcePartitionToTaskPartition.get(sourcePartitionId);
                verify(taskPartition != null, "taskPartition not found for fragment %s plan node %s for sourcePartitionId %s", fragmentId, planNodeId, sourcePartitionId);

                List<SubPartition> subPartitions;
                if (taskPartition.getSplitBy().isPresent() && taskPartition.getSplitBy().get().equals(planNodeId)) {
                    subPartitions = ImmutableList.of(taskPartition.getNextSubPartition());
                }
                else {
                    subPartitions = taskPartition.getSubPartitions();
                }

                for (SubPartition subPartition : subPartitions) {
                    // todo see if having lots of PartitionUpdates is not a problem; should we merge
                    assignment.updatePartition(new PartitionUpdate(subPartition.getId(), planNodeId, true, ImmutableListMultimap.of(sourcePartitionId, split), false));
                }
            });
        }

        if (noMoreSplits) {
            completedSources.add(planNodeId);
            for (Integer taskPartition : createdTaskPartitions) {
                assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, false, ImmutableListMultimap.of(), true));
            }

            if (completedSources.containsAll(allSources)) {
                for (Integer taskPartition : createdTaskPartitions) {
                    assignment.sealPartition(taskPartition);
                }
                replicatedSplits.clear();
            }
        }

        return assignment.build();
    }

    public static ListMultimap<Integer, Split> replicatedSourcePartition(List<Split> splits)
    {
        ImmutableListMultimap.Builder<Integer, Split> builder = ImmutableListMultimap.builder();
        builder.putAll(SINGLE_SOURCE_PARTITION_ID, splits);
        return builder.build();
    }

    @Override
    public AssignmentResult finish()
    {
        checkState(!createdTaskPartitions.isEmpty(), "createdTaskPartitions is not expected to be empty");
        return AssignmentResult.builder().build();
    }

    @VisibleForTesting
    static Map<Integer, TaskPartition> createSourcePartitionToTaskPartition(
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Set<PlanNodeId> partitionedSources,
            Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates,
            long targetPartitionSizeInBytes,
            int targetMinTaskCount,
            int targetMaxTaskCount,
            Predicate<PlanNodeId> canSplit,
            boolean canMerge)
    {
        int partitionCount = sourcePartitioningScheme.getPartitionCount();
        if (sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent() ||
                partitionedSources.isEmpty() ||
                !sourceDataSizeEstimates.keySet().containsAll(partitionedSources)) {
            // if bucket scheme is set explicitly or if estimates are missing create one task partition per output partition
            return IntStream.range(0, partitionCount)
                    .boxed()
                    .collect(toImmutableMap(Function.identity(), (key) -> new TaskPartition(1, Optional.empty())));
        }

        List<OutputDataSizeEstimate> partitionedSourcesEstimates = sourceDataSizeEstimates.entrySet().stream()
                .filter(entry -> partitionedSources.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        OutputDataSizeEstimate mergedEstimate = OutputDataSizeEstimate.merge(partitionedSourcesEstimates);

        // adjust targetPartitionSizeInBytes based on total input bytes
        if (targetMaxTaskCount != Integer.MAX_VALUE || targetMinTaskCount != 0) {
            long totalBytes = mergedEstimate.getTotalSizeInBytes();

            if (totalBytes / targetPartitionSizeInBytes > targetMaxTaskCount) {
                // targetMaxTaskCount is only used to adjust targetPartitionSizeInBytes to avoid excessive number
                // of tasks; actual number of tasks depend on the data size distribution and may exceed its value
                targetPartitionSizeInBytes = (totalBytes + targetMaxTaskCount - 1) / targetMaxTaskCount;
            }

            if (totalBytes / targetPartitionSizeInBytes < targetMinTaskCount) {
                targetPartitionSizeInBytes = Math.max(totalBytes / targetMinTaskCount, 1);
            }
        }

        ImmutableMap.Builder<Integer, TaskPartition> result = ImmutableMap.builder();
        PriorityQueue<PartitionAssignment> assignments = new PriorityQueue<>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            long partitionSizeInBytes = mergedEstimate.getPartitionSizeInBytes(partitionId);
            if (assignments.isEmpty() || assignments.peek().assignedDataSizeInBytes() + partitionSizeInBytes > targetPartitionSizeInBytes || !canMerge) {
                TaskPartition taskPartition = createTaskPartition(
                        partitionSizeInBytes,
                        targetPartitionSizeInBytes,
                        partitionedSources,
                        sourceDataSizeEstimates,
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
            Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates,
            int partitionId,
            Predicate<PlanNodeId> canSplit)
    {
        if (partitionSizeInBytes > targetPartitionSizeInBytes) {
            // try to assign multiple sub-partitions if possible
            Map<PlanNodeId, Long> sourceSizes = getSourceSizes(partitionedSources, sourceDataSizeEstimates, partitionId);
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

    private static Map<PlanNodeId, Long> getSourceSizes(Set<PlanNodeId> partitionedSources, Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates, int partitionId)
    {
        return partitionedSources.stream()
                .collect(toImmutableMap(Function.identity(), source -> sourceDataSizeEstimates.get(source).getPartitionSizeInBytes(partitionId)));
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

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("subPartitions", subPartitions)
                    .add("splitBy", splitBy)
                    .add("nextSubPartition", nextSubPartition)
                    .toString();
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

        @Override
        public String toString()
        {
            return id.toString();
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogRequirement", catalogRequirement)
                .add("replicatedSources", replicatedSources)
                .add("allSources", allSources)
                .add("sourcePartitioningScheme", sourcePartitioningScheme)
                .add("sourcePartitionToTaskPartition", sourcePartitionToTaskPartition)
                .add("createdTaskPartitions", createdTaskPartitions)
                .add("completedSources", completedSources)
                .add("replicatedSplits.size()", replicatedSplits.size())
                .add("allTaskPartitionsCreated", allTaskPartitionsCreated)
                .toString();
    }
}
