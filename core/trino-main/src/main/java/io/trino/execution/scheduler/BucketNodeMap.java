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
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;

import java.util.List;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public final class BucketNodeMap
{
    private final List<InternalNode> bucketToNode;
    private final ToIntFunction<Split> splitToBucket;

    public BucketNodeMap(ToIntFunction<Split> splitToBucket, List<InternalNode> bucketToNode)
    {
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
        this.bucketToNode = ImmutableList.copyOf(requireNonNull(bucketToNode, "bucketToNode is null"));
    }

    public int getBucketCount()
    {
        return bucketToNode.size();
    }

    public int getBucket(Split split)
    {
        return splitToBucket.applyAsInt(split);
    }

    public InternalNode getAssignedNode(int bucketId)
    {
        return bucketToNode.get(bucketId);
    }

    public InternalNode getAssignedNode(Split split)
    {
        return getAssignedNode(getBucket(split));
    }

    public ToIntFunction<Split> getSplitToBucketFunction()
    {
        return splitToBucket;
    }
}
