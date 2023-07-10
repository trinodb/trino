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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Multimaps.asMap;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TaskDescriptor
{
    private static final int INSTANCE_SIZE = instanceSize(TaskDescriptor.class);

    private final int partitionId;
    private final ListMultimap<PlanNodeId, Split> splits;
    private final NodeRequirements nodeRequirements;

    private transient volatile long retainedSizeInBytes;

    public TaskDescriptor(
            int partitionId,
            ListMultimap<PlanNodeId, Split> splits,
            NodeRequirements nodeRequirements)
    {
        this.partitionId = partitionId;
        this.splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
        this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public ListMultimap<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    public NodeRequirements getNodeRequirements()
    {
        return nodeRequirements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskDescriptor that = (TaskDescriptor) o;
        return partitionId == that.partitionId && Objects.equals(splits, that.splits) && Objects.equals(nodeRequirements, that.nodeRequirements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, splits, nodeRequirements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("splits", splits)
                .add("nodeRequirements", nodeRequirements)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        long result = retainedSizeInBytes;
        if (result == 0) {
            result = INSTANCE_SIZE
                    + estimatedSizeOf(asMap(splits), PlanNodeId::getRetainedSizeInBytes, splits -> estimatedSizeOf(splits, Split::getRetainedSizeInBytes))
                    + nodeRequirements.getRetainedSizeInBytes();
            retainedSizeInBytes = result;
        }
        return result;
    }
}
