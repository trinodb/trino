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
import com.google.common.collect.ImmutableList;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FaultTolerantPartitioningScheme
{
    private final int partitionCount;
    private final Optional<int[]> bucketToPartitionMap;
    private final Optional<ToIntFunction<Split>> splitToBucketFunction;
    private final Optional<List<InternalNode>> partitionToNodeMap;

    @VisibleForTesting
    FaultTolerantPartitioningScheme(
            int partitionCount,
            Optional<int[]> bucketToPartitionMap,
            Optional<ToIntFunction<Split>> splitToBucketFunction,
            Optional<List<InternalNode>> partitionToNodeMap)
    {
        checkArgument(partitionCount > 0, "partitionCount must be greater than zero");
        this.partitionCount = partitionCount;
        this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
        this.splitToBucketFunction = requireNonNull(splitToBucketFunction, "splitToBucketFunction is null");
        requireNonNull(partitionToNodeMap, "partitionToNodeMap is null");
        partitionToNodeMap.ifPresent(map -> checkArgument(
                map.size() == partitionCount,
                "partitionToNodeMap size (%s) must be equal to partitionCount (%s)",
                map.size(),
                partitionCount));
        this.partitionToNodeMap = partitionToNodeMap.map(ImmutableList::copyOf);
    }

    public int getPartitionCount()
    {
        return partitionCount;
    }

    public Optional<int[]> getBucketToPartitionMap()
    {
        return bucketToPartitionMap;
    }

    public int getPartition(Split split)
    {
        if (splitToBucketFunction.isPresent()) {
            checkState(bucketToPartitionMap.isPresent(), "bucketToPartitionMap is expected to be present");
            int bucket = splitToBucketFunction.get().applyAsInt(split);
            checkState(
                    bucketToPartitionMap.get().length > bucket,
                    "invalid bucketToPartitionMap size (%s), bucket to partition mapping not found for bucket %s",
                    bucketToPartitionMap.get().length,
                    bucket);
            return bucketToPartitionMap.get()[bucket];
        }
        checkState(partitionCount == 1, "partitionCount is expected to be set to 1: %s", partitionCount);
        return 0;
    }

    public boolean isExplicitPartitionToNodeMappingPresent()
    {
        return partitionToNodeMap.isPresent();
    }

    public Optional<InternalNode> getNodeRequirement(int partition)
    {
        checkArgument(partition < partitionCount, "partition is expected to be less than %s", partitionCount);
        return partitionToNodeMap.map(map -> map.get(partition));
    }

    public Optional<List<InternalNode>> getPartitionToNodeMap()
    {
        return partitionToNodeMap;
    }
}
