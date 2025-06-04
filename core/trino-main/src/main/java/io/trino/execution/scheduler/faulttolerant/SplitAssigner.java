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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.primitives.ImmutableIntArray;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.annotation.NotThreadSafe;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * An implementation is not required to be thread safe
 */
@NotThreadSafe
interface SplitAssigner
{
    // marker source partition id for data which is not hash distributed
    int SINGLE_SOURCE_PARTITION_ID = 0;

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
            ListMultimap<Integer, Split> splits, // sourcePartition -> splits
            boolean noMoreSplits)
    {
        public PartitionUpdate
        {
            requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(!(readyForScheduling && splits.isEmpty()), "partition update with empty splits marked as ready for scheduling");
            splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
        }

        public String debugInfo()
        {
            // toString is too verbose
            return toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("planNodeId", planNodeId)
                    .add("readyForScheduling", readyForScheduling)
                    .add("splits", splits.asMap().entrySet().stream().collect(toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().size())))
                    .add("noMoreSplits", noMoreSplits)
                    .toString();
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

        boolean isEmpty()
        {
            return partitionsAdded.isEmpty() && !noMorePartitions && partitionUpdates.isEmpty() && sealedPartitions.isEmpty();
        }

        public String debugInfo()
        {
            // toString is too verbose
            return toStringHelper(this)
                    .add("partitionsAdded", partitionsAdded)
                    .add("noMorePartitions", noMorePartitions)
                    .add("partitionUpdates", partitionUpdates.stream().map(PartitionUpdate::debugInfo).collect(toList()))
                    .add("sealedPartitions", sealedPartitions)
                    .toString();
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
