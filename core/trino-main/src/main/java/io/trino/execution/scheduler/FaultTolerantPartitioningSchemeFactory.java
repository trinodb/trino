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
import io.trino.Session;
import io.trino.metadata.InternalNode;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class FaultTolerantPartitioningSchemeFactory
{
    private final NodePartitioningManager nodePartitioningManager;
    private final Session session;
    private final int partitionCount;

    private final Map<PartitioningHandle, FaultTolerantPartitioningScheme> cache = new HashMap<>();

    public FaultTolerantPartitioningSchemeFactory(NodePartitioningManager nodePartitioningManager, Session session, int partitionCount)
    {
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.session = requireNonNull(session, "session is null");
        this.partitionCount = partitionCount;
    }

    public FaultTolerantPartitioningScheme get(PartitioningHandle handle)
    {
        return cache.computeIfAbsent(handle, this::create);
    }

    private FaultTolerantPartitioningScheme create(PartitioningHandle partitioningHandle)
    {
        if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
            return new FaultTolerantPartitioningScheme(
                    partitionCount,
                    Optional.of(IntStream.range(0, partitionCount).toArray()),
                    Optional.empty(),
                    Optional.empty());
        }
        if (partitioningHandle.getCatalogHandle().isPresent() ||
                (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle)) {
            // TODO This caps the number of partitions to the number of available nodes. Perhaps a better approach is required for fault tolerant execution.
            BucketNodeMap bucketNodeMap = nodePartitioningManager.getNodePartitioningMap(session, partitioningHandle).asBucketNodeMap();
            int bucketCount = bucketNodeMap.getBucketCount();
            int[] bucketToPartition = new int[bucketCount];
            // make sure all buckets mapped to the same node map to the same partition, such that locality requirements are respected in scheduling
            Map<InternalNode, Integer> nodeToPartition = new HashMap<>();
            List<InternalNode> partitionToNodeMap = new ArrayList<>();
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                InternalNode node = bucketNodeMap.getAssignedNode(bucket);
                Integer partitionId = nodeToPartition.get(node);
                if (partitionId == null) {
                    partitionId = partitionToNodeMap.size();
                    nodeToPartition.put(node, partitionId);
                    partitionToNodeMap.add(node);
                }
                bucketToPartition[bucket] = partitionId;
            }
            return new FaultTolerantPartitioningScheme(
                    partitionToNodeMap.size(),
                    Optional.of(bucketToPartition),
                    Optional.of(bucketNodeMap.getSplitToBucketFunction()),
                    Optional.of(ImmutableList.copyOf(partitionToNodeMap)));
        }
        return new FaultTolerantPartitioningScheme(1, Optional.empty(), Optional.empty(), Optional.empty());
    }
}
