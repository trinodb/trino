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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class HiveNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private static final int PARTITIONED_BUCKETS_PER_NODE = 32;

    private final NodeManager nodeManager;
    private final TypeOperators typeOperators;

    @Inject
    public HiveNodePartitioningProvider(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeOperators = requireNonNull(typeManager, "typeManager is null").getTypeOperators();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        List<HiveType> hiveBucketTypes = handle.getHiveTypes();
        if (!handle.isUsePartitionedBucketingForWrites()) {
            return new HiveBucketFunction(
                    handle.getBucketingVersion(),
                    bucketCount,
                    hiveBucketTypes,
                    partitionChannelTypes.subList(0, partitionChannelTypes.size() - hiveBucketTypes.size()),
                    handle.getPartitions());
        }
        return new HivePartitionHashBucketFunction(
                handle.getBucketingVersion(),
                handle.getBucketCount(),
                hiveBucketTypes,
                partitionChannelTypes.subList(hiveBucketTypes.size(), partitionChannelTypes.size()),
                typeOperators,
                bucketCount);
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        if (!handle.isUsePartitionedBucketingForWrites()) {
            return createBucketNodeMap(handle.getBucketCount() * Integer.max(1, handle.getPartitions().size()));
        }

        // Create a bucket to node mapping. Consecutive buckets are assigned
        // to shuffled nodes (e.g "1 -> node2, 2 -> node1, 3 -> node2, 4 -> node1, ...").
        // Hash function generates consecutive bucket numbers within a partition
        // (e.g "(part1, bucket1) -> 1234, (part1, bucket2) -> 1235, ...").
        // Thus single partition insert will be distributed across all worker nodes
        // (if number of workers is greater or equal to number of buckets within a partition).
        // We can write to (number of partitions P) * (number of buckets B) in parallel.
        // However, number of partitions is not known here
        // If number of workers < ( P * B), we need multiple writers per node to fully
        // parallelize the write within a worker
        return createBucketNodeMap(createArbitraryBucketToNode(
                ImmutableList.copyOf(nodeManager.getRequiredWorkerNodes()),
                nodeManager.getRequiredWorkerNodes().size() * PARTITIONED_BUCKETS_PER_NODE));
    }

    private static List<Node> createArbitraryBucketToNode(List<Node> nodes, int bucketCount)
    {
        return cyclingShuffledStream(nodes)
                .limit(bucketCount)
                .collect(toImmutableList());
    }

    private static <T> Stream<T> cyclingShuffledStream(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Stream.generate(() -> list).flatMap(List::stream);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        final HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        if (handle.getPartitions().isEmpty()) {
            return value -> ((HiveSplit) value).getReadBucketNumber()
                    .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
        }
        List<String> partitions = handle.getPartitions();
        ImmutableMap.Builder<String, Integer> partitionToIndexBuilder = ImmutableMap.builder();
        for (int i = 0; i < handle.getPartitions().size(); i++) {
            partitionToIndexBuilder.put(partitions.get(i), i);
        }
        final ImmutableMap<String, Integer> partitionToIndex = partitionToIndexBuilder.buildOrThrow();
        return value -> {
            HiveSplit split = (HiveSplit) value;
            int partitionIndex = partitionToIndex.getOrDefault(split.getPartitionName(), 0);
            return split.getReadBucketNumber().orElse(0) + (handle.getBucketCount() * partitionIndex);
        };
    }
}
