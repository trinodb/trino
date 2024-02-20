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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TaskDescriptor
{
    private static final int INSTANCE_SIZE = instanceSize(TaskDescriptor.class);

    private final int partitionId;
    private final SplitsMapping splits;
    private final NodeRequirements nodeRequirements;

    private transient volatile long retainedSizeInBytes;

    public TaskDescriptor(
            int partitionId,
            SplitsMapping splitsMapping,
            NodeRequirements nodeRequirements)
    {
        this.partitionId = partitionId;
        this.splits = requireNonNull(splitsMapping, "splitsMapping is null");
        this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public SplitsMapping getSplits()
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
                    + splits.getRetainedSizeInBytes()
                    + nodeRequirements.getRetainedSizeInBytes();
            retainedSizeInBytes = result;
        }
        return result;
    }
}
