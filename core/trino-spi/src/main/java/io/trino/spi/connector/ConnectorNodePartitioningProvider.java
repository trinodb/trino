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
package io.trino.spi.connector;

import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

public interface ConnectorNodePartitioningProvider
{
    /**
     * Get the mapping from bucket to nodes for the specified partitioning handle. The returned mapping may
     * be fixed or dynamic. If the mapping is fixed, the bucket will be assigned to an exact node; otherwise,
     * the bucket will be assigned to a node chosen by the system.  The ConnectorPartitionHandle is declared
     * in ConnectorTablePartitioning property of ConnectorTableProperties.
     * <p>
     * If the partitioning handle is not supported, this method must return an empty optional.
     */
    default Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return Optional.empty();
    }

    /**
     * Gets a function that maps a split to a bucket number. The returned function must be deterministic, and must
     * be consistent with getBucketNodeMapping. That means all rows in a split must be assigned to the same bucket.
     * The bucket number must be in the range [0, bucketCount).
     */
    default ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            int bucketCount)
    {
        return _ -> {
            throw new UnsupportedOperationException();
        };
    }

    /**
     * Get the function that maps a partition to a bucket number. The returned function must be deterministic, and
     * must be consistent with getBucketNodeMapping. The result must be in the range [0, bucketCount).
     */
    BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount);
}
