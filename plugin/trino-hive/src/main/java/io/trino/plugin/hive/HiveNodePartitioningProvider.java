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

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

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
        this.typeOperators = typeManager.getTypeOperators();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        if (partitioningHandle instanceof HiveUpdateHandle) {
            return new HiveUpdateBucketFunction(bucketCount);
        }
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        List<HiveType> hiveBucketTypes = handle.getHiveTypes();
        if (!handle.isUsePartitionedBucketing()) {
            return new HiveBucketFunction(handle.getBucketingVersion(), bucketCount, hiveBucketTypes);
        }
        return new HivePartitionedBucketFunction(
                handle.getBucketingVersion(),
                handle.getBucketCount(),
                hiveBucketTypes,
                partitionChannelTypes.subList(hiveBucketTypes.size(), partitionChannelTypes.size()),
                typeOperators,
                bucketCount);
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        if (!handle.isUsePartitionedBucketing()) {
            return Optional.of(createBucketNodeMap(handle.getBucketCount()));
        }

        // Allocate a fixed number of buckets. Trino will assign consecutive buckets
        // to shuffled nodes (e.g. "1 -> node2, 2 -> node1, 3 -> node2, 4 -> node1, ...").
        // Hash function generates consecutive bucket numbers within a partition
        // (e.g. "(part1, bucket1) -> 1234, (part1, bucket2) -> 1235, ...").
        // Thus single partition insert will be distributed across all worker nodes
        // (if number of workers is greater or equal to number of buckets within a partition).
        // We can write to (number of partitions P) * (number of buckets B) in parallel.
        // However, number of partitions is not known here
        // If number of workers < ( P * B), we need multiple writers per node to fully
        // parallelize the write within a worker
        return Optional.of(createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size() * PARTITIONED_BUCKETS_PER_NODE));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((HiveSplit) value).getReadBucketNumber()
                .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
    }
}
