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

import com.google.inject.Inject;
import io.trino.metastore.HiveType;
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

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;

public class HiveNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final TypeOperators typeOperators;

    @Inject
    public HiveNodePartitioningProvider(TypeManager typeManager)
    {
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
            return Optional.of(createBucketNodeMap(handle.getBucketCount()).withCacheKeyHint(handle.getCacheKeyHint()));
        }
        return Optional.empty();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            int bucketCount)
    {
        return value -> ((HiveSplit) value).getReadBucketNumber()
                .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
    }
}
