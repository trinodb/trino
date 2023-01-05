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
import io.trino.metadata.Split;
import io.trino.spi.Node;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
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
        FaultTolerantPartitioningScheme result = cache.get(handle);
        if (result == null) {
            // Avoid using computeIfAbsent as the "get" method is called recursively from the "create" method
            result = create(handle);
            cache.put(handle, result);
        }
        return result;
    }

    private FaultTolerantPartitioningScheme create(PartitioningHandle partitioningHandle)
    {
        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle mergePartitioningHandle) {
            return mergePartitioningHandle.getFaultTolerantPartitioningScheme(this::get);
        }
        if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
            return createSystemSchema(partitionCount);
        }
        if (partitioningHandle.getCatalogHandle().isPresent()) {
            Optional<ConnectorBucketNodeMap> connectorBucketNodeMap = nodePartitioningManager.getConnectorBucketNodeMap(session, partitioningHandle);
            if (connectorBucketNodeMap.isEmpty()) {
                return createSystemSchema(partitionCount);
            }
            ToIntFunction<Split> splitToBucket = nodePartitioningManager.getSplitToBucket(session, partitioningHandle);
            return createConnectorSpecificSchema(partitionCount, connectorBucketNodeMap.get(), splitToBucket);
        }
        return new FaultTolerantPartitioningScheme(1, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static FaultTolerantPartitioningScheme createSystemSchema(int partitionCount)
    {
        return new FaultTolerantPartitioningScheme(
                partitionCount,
                Optional.of(IntStream.range(0, partitionCount).toArray()),
                Optional.empty(),
                Optional.empty());
    }

    private static FaultTolerantPartitioningScheme createConnectorSpecificSchema(int partitionCount, ConnectorBucketNodeMap bucketNodeMap, ToIntFunction<Split> splitToBucket)
    {
        if (bucketNodeMap.hasFixedMapping()) {
            return createFixedConnectorSpecificSchema(bucketNodeMap.getFixedMapping(), splitToBucket);
        }
        return createArbitraryConnectorSpecificSchema(partitionCount, bucketNodeMap.getBucketCount(), splitToBucket);
    }

    private static FaultTolerantPartitioningScheme createFixedConnectorSpecificSchema(List<Node> fixedMapping, ToIntFunction<Split> splitToBucket)
    {
        int bucketCount = fixedMapping.size();
        int[] bucketToPartition = new int[bucketCount];
        // make sure all buckets mapped to the same node map to the same partition, such that locality requirements are respected in scheduling
        Map<InternalNode, Integer> nodeToPartition = new HashMap<>();
        List<InternalNode> partitionToNodeMap = new ArrayList<>();
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            InternalNode node = (InternalNode) fixedMapping.get(bucket);
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
                Optional.of(splitToBucket),
                Optional.of(ImmutableList.copyOf(partitionToNodeMap)));
    }

    private static FaultTolerantPartitioningScheme createArbitraryConnectorSpecificSchema(int partitionCount, int bucketCount, ToIntFunction<Split> splitToBucket)
    {
        int[] bucketToPartition = new int[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            bucketToPartition[bucket] = bucket % partitionCount;
        }
        return new FaultTolerantPartitioningScheme(
                // TODO: It may be possible to set the number of partitions to the number of buckets when it is known that a
                // TODO: stage contains no remote sources and the engine doesn't have to partition any data
                partitionCount,
                Optional.of(bucketToPartition),
                Optional.of(splitToBucket),
                Optional.empty());
    }
}
