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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class SingleDistributionSplitAssigner
        implements SplitAssigner
{
    private final Optional<HostAddress> hostRequirement;
    private final Set<PlanNodeId> allSources;

    private boolean partitionAdded;
    private final Set<PlanNodeId> completedSources = new HashSet<>();

    SingleDistributionSplitAssigner(Optional<HostAddress> hostRequirement, Set<PlanNodeId> allSources)
    {
        requireNonNull(hostRequirement, "hostRequirement is null");
        this.hostRequirement = hostRequirement;
        this.allSources = ImmutableSet.copyOf(requireNonNull(allSources, "allSources is null"));
    }

    @Override
    public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();
        if (!partitionAdded) {
            partitionAdded = true;
            assignment.addPartition(new Partition(0, new NodeRequirements(Optional.empty(), hostRequirement, hostRequirement.isEmpty())));
            assignment.setNoMorePartitions();
        }
        if (!splits.isEmpty()) {
            checkState(!completedSources.contains(planNodeId), "source is finished: %s", planNodeId);
            assignment.updatePartition(new PartitionUpdate(
                    0,
                    planNodeId,
                    true,
                    ImmutableListMultimap.copyOf(splits),
                    false));
        }
        if (noMoreSplits) {
            assignment.updatePartition(new PartitionUpdate(
                    0,
                    planNodeId,
                    false,
                    ImmutableListMultimap.of(),
                    true));
            completedSources.add(planNodeId);
        }
        if (completedSources.containsAll(allSources)) {
            assignment.sealPartition(0);
        }
        return assignment.build();
    }

    @Override
    public AssignmentResult finish()
    {
        AssignmentResult.Builder result = AssignmentResult.builder();
        if (!partitionAdded) {
            partitionAdded = true;
            result
                    .addPartition(new Partition(0, new NodeRequirements(Optional.empty(), hostRequirement, hostRequirement.isEmpty())))
                    .sealPartition(0)
                    .setNoMorePartitions();
        }
        return result.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hostRequirement", hostRequirement)
                .add("allSources", allSources)
                .add("partitionAdded", partitionAdded)
                .add("completedSources", completedSources)
                .toString();
    }
}
