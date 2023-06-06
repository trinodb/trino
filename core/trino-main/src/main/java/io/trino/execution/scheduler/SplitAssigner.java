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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.primitives.ImmutableIntArray;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * An implementation is not required to be thread safe
 */
@NotThreadSafe
interface SplitAssigner
{
    AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits);

    AssignmentResult finish();

    record Partition(int partitionId, NodeRequirements nodeRequirements)
    {
        public Partition
        {
            requireNonNull(nodeRequirements, "nodeRequirements is null");
        }
    }

    record PartitionUpdate(
            int partitionId,
            PlanNodeId planNodeId,
            boolean readyForScheduling,
            List<Split> splits,
            boolean noMoreSplits)
    {
        public PartitionUpdate
        {
            requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(!(readyForScheduling && splits.isEmpty()), "partition update with empty splits marked as ready for scheduling");
            splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        }
    }

    record AssignmentResult(
            List<Partition> partitionsAdded,
            boolean noMorePartitions,
            List<PartitionUpdate> partitionUpdates,
            ImmutableIntArray sealedPartitions)
    {
        public AssignmentResult
        {
            partitionsAdded = ImmutableList.copyOf(requireNonNull(partitionsAdded, "partitionsAdded is null"));
            partitionUpdates = ImmutableList.copyOf(requireNonNull(partitionUpdates, "partitionUpdates is null"));
        }

        public static AssignmentResult.Builder builder()
        {
            return new AssignmentResult.Builder();
        }

        public static class Builder
        {
            private final ImmutableList.Builder<Partition> partitionsAdded = ImmutableList.builder();
            private boolean noMorePartitions;
            private final ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
            private final ImmutableIntArray.Builder sealedPartitions = ImmutableIntArray.builder();

            @CanIgnoreReturnValue
            public AssignmentResult.Builder addPartition(Partition partition)
            {
                partitionsAdded.add(partition);
                return this;
            }

            @CanIgnoreReturnValue
            public AssignmentResult.Builder setNoMorePartitions()
            {
                this.noMorePartitions = true;
                return this;
            }

            @CanIgnoreReturnValue
            public AssignmentResult.Builder updatePartition(PartitionUpdate partitionUpdate)
            {
                partitionUpdates.add(partitionUpdate);
                return this;
            }

            @CanIgnoreReturnValue
            public AssignmentResult.Builder sealPartition(int partitionId)
            {
                sealedPartitions.add(partitionId);
                return this;
            }

            public AssignmentResult build()
            {
                return new AssignmentResult(
                        partitionsAdded.build(),
                        noMorePartitions,
                        partitionUpdates.build(),
                        sealedPartitions.build());
            }
        }
    }
}
