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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.connector.CatalogHandle;
import io.trino.execution.scheduler.EventDrivenTaskSource.Partition;
import io.trino.execution.scheduler.EventDrivenTaskSource.PartitionUpdate;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
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

    HashDistributionSplitAssigner(
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            long targetPartitionSizeInBytes,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            boolean preserveOutputPartitioning)
    {
        this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
        this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
        allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        outputPartitionToTaskPartition = createOutputPartitionToTaskPartition(
                sourcePartitioningScheme,
                partitionedSources,
                outputDataSizeEstimates,
                preserveOutputPartitioning,
                targetPartitionSizeInBytes);
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
            for (Integer outputPartitionId : splits.keySet()) {
                TaskPartition taskPartition = outputPartitionToTaskPartition.get(outputPartitionId);
                verify(taskPartition != null, "taskPartition not found for outputPartitionId: %s", outputPartitionId);
                if (!taskPartition.isIdAssigned()) {
                    // Assigns lazily to ensure task ids are incremental and with no gaps.
                    // Gaps can occur when scanning over a bucketed table as some buckets may contain no data.
                    taskPartition.assignId(nextTaskPartitionId++);
                }
                int taskPartitionId = taskPartition.getId();
                if (!createdTaskPartitions.contains(taskPartitionId)) {
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
                assignment.updatePartition(new PartitionUpdate(taskPartitionId, planNodeId, splits.get(outputPartitionId), false));
            }
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

    private static Map<Integer, TaskPartition> createOutputPartitionToTaskPartition(
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Set<PlanNodeId> partitionedSources,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            boolean preserveOutputPartitioning,
            long targetPartitionSizeInBytes)
    {
        int partitionCount = sourcePartitioningScheme.getPartitionCount();
        if (sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent() ||
                partitionedSources.isEmpty() ||
                !outputDataSizeEstimates.keySet().containsAll(partitionedSources) ||
                preserveOutputPartitioning) {
            // if bucket scheme is set explicitly or if estimates are missing create one task partition per output partition
            return IntStream.range(0, partitionCount)
                    .boxed()
                    .collect(toImmutableMap(Function.identity(), (key) -> new TaskPartition()));
        }

        List<OutputDataSizeEstimate> partitionedSourcesEstimates = outputDataSizeEstimates.entrySet().stream()
                .filter(entry -> partitionedSources.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        OutputDataSizeEstimate mergedEstimate = OutputDataSizeEstimate.merge(partitionedSourcesEstimates);
        ImmutableMap.Builder<Integer, TaskPartition> result = ImmutableMap.builder();
        PriorityQueue<PartitionAssignment> assignments = new PriorityQueue<>();
        assignments.add(new PartitionAssignment(new TaskPartition(), 0));
        for (int outputPartitionId = 0; outputPartitionId < partitionCount; outputPartitionId++) {
            long outputPartitionSize = mergedEstimate.getPartitionSizeInBytes(outputPartitionId);
            if (assignments.peek().assignedDataSizeInBytes() + outputPartitionSize > targetPartitionSizeInBytes
                    && assignments.size() < partitionCount) {
                assignments.add(new PartitionAssignment(new TaskPartition(), 0));
            }
            PartitionAssignment assignment = assignments.poll();
            result.put(outputPartitionId, assignment.taskPartition());
            assignments.add(new PartitionAssignment(assignment.taskPartition(), assignment.assignedDataSizeInBytes() + outputPartitionSize));
        }
        return result.buildOrThrow();
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

    private static class TaskPartition
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
}
