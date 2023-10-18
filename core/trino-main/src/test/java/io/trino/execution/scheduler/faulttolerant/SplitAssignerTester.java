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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.trino.execution.scheduler.faulttolerant.SplitAssigner.AssignmentResult;
import io.trino.execution.scheduler.faulttolerant.SplitAssigner.Partition;
import io.trino.execution.scheduler.faulttolerant.SplitAssigner.PartitionUpdate;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

class SplitAssignerTester
{
    private final Map<Integer, NodeRequirements> nodeRequirements = new HashMap<>();
    private final Map<Integer, ListMultimap<PlanNodeId, Split>> splits = new HashMap<>();
    private final SetMultimap<Integer, PlanNodeId> noMoreSplits = HashMultimap.create();
    private final Set<Integer> sealedTaskPartitions = new HashSet<>();
    private boolean noMoreTaskPartitions;
    private Optional<List<TaskDescriptor>> taskDescriptors = Optional.empty();

    public Optional<List<TaskDescriptor>> getTaskDescriptors()
    {
        return taskDescriptors;
    }

    public synchronized int getTaskPartitionCount()
    {
        return nodeRequirements.size();
    }

    public synchronized NodeRequirements getNodeRequirements(int taskPartition)
    {
        NodeRequirements result = nodeRequirements.get(taskPartition);
        checkArgument(result != null, "task partition not found: %s", taskPartition);
        return result;
    }

    public synchronized Set<Integer> getSplitIds(int taskPartition, PlanNodeId planNodeId)
    {
        ListMultimap<PlanNodeId, Split> taskPartitionSplits = splits.getOrDefault(taskPartition, ImmutableListMultimap.of());
        return taskPartitionSplits.get(planNodeId).stream()
                .map(split -> (TestingConnectorSplit) split.getConnectorSplit())
                .map(TestingConnectorSplit::getId)
                .collect(toImmutableSet());
    }

    public synchronized boolean isNoMoreSplits(int taskPartition, PlanNodeId planNodeId)
    {
        return noMoreSplits.get(taskPartition).contains(planNodeId);
    }

    public synchronized boolean isSealed(int taskPartition)
    {
        return sealedTaskPartitions.contains(taskPartition);
    }

    public synchronized boolean isNoMoreTaskPartitions()
    {
        return noMoreTaskPartitions;
    }

    public void checkContainsSplits(PlanNodeId planNodeId, Collection<Split> splits, boolean replicated)
    {
        Set<Integer> expectedSplitIds = splits.stream()
                .map(TestingConnectorSplit::getSplitId)
                .collect(Collectors.toSet());
        for (int taskPartitionId = 0; taskPartitionId < getTaskPartitionCount(); taskPartitionId++) {
            Set<Integer> taskPartitionSplitIds = getSplitIds(taskPartitionId, planNodeId);
            if (replicated) {
                assertThat(taskPartitionSplitIds).containsAll(expectedSplitIds);
            }
            else {
                expectedSplitIds.removeAll(taskPartitionSplitIds);
            }
        }
        if (!replicated) {
            assertThat(expectedSplitIds).isEmpty();
        }
    }

    public void update(AssignmentResult assignment)
    {
        for (Partition taskPartition : assignment.partitionsAdded()) {
            verify(!noMoreTaskPartitions, "noMoreTaskPartitions is set");
            verify(nodeRequirements.put(taskPartition.partitionId(), taskPartition.nodeRequirements()) == null, "task partition already exist: %s", taskPartition.partitionId());
        }
        for (PartitionUpdate taskPartitionUpdate : assignment.partitionUpdates()) {
            int taskPartitionId = taskPartitionUpdate.partitionId();
            verify(nodeRequirements.get(taskPartitionId) != null, "task partition does not exist: %s", taskPartitionId);
            verify(!sealedTaskPartitions.contains(taskPartitionId), "task partition is sealed: %s", taskPartitionId);
            PlanNodeId planNodeId = taskPartitionUpdate.planNodeId();
            if (!taskPartitionUpdate.splits().isEmpty()) {
                verify(!noMoreSplits.get(taskPartitionId).contains(planNodeId), "noMoreSplits is set for task partition %s and plan node %s", taskPartitionId, planNodeId);
                splits.computeIfAbsent(taskPartitionId, (key) -> ArrayListMultimap.create()).putAll(planNodeId, taskPartitionUpdate.splits());
            }
            if (taskPartitionUpdate.noMoreSplits()) {
                noMoreSplits.put(taskPartitionId, planNodeId);
            }
        }
        assignment.sealedPartitions().forEach(sealedTaskPartitions::add);
        if (assignment.noMorePartitions()) {
            noMoreTaskPartitions = true;
        }
        checkFinished();
    }

    private synchronized void checkFinished()
    {
        if (noMoreTaskPartitions && sealedTaskPartitions.containsAll(nodeRequirements.keySet())) {
            verify(sealedTaskPartitions.equals(nodeRequirements.keySet()), "unknown sealed partitions: %s", Sets.difference(sealedTaskPartitions, nodeRequirements.keySet()));
            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            for (Integer taskPartitionId : sealedTaskPartitions) {
                ListMultimap<PlanNodeId, Split> taskSplits = splits.getOrDefault(taskPartitionId, ImmutableListMultimap.of());
                verify(
                        noMoreSplits.get(taskPartitionId).containsAll(taskSplits.keySet()),
                        "no more split is missing for task partition %s: %s",
                        taskPartitionId,
                        Sets.difference(taskSplits.keySet(), noMoreSplits.get(taskPartitionId)));
                result.add(new TaskDescriptor(
                        taskPartitionId,
                        taskSplits,
                        nodeRequirements.get(taskPartitionId)));
            }
            taskDescriptors = Optional.of(result.build());
        }
    }
}
