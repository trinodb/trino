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
package io.trino.plugin.iceberg;

import com.google.inject.Inject;
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

import static io.trino.plugin.iceberg.IcebergPartitionFunction.Transform.BUCKET;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final TypeOperators typeOperators;

    @Inject
    public IcebergNodePartitioningProvider(TypeManager typeManager)
    {
        this.typeOperators = typeManager.getTypeOperators();
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;

        List<IcebergPartitionFunction> partitionFunctions = handle.partitionFunctions();
        // when there is a single bucket partition function, inform the engine there is a limit on the number of buckets
        // TODO: when there are multiple bucket partition functions, we could compute the product of bucket counts, but this causes the engine to create too many writers
        if (partitionFunctions.size() == 1 && partitionFunctions.getFirst().transform() == BUCKET) {
            return Optional.of(createBucketNodeMap(partitionFunctions.getFirst().size().orElseThrow()).withCacheKeyHint(handle.getCacheKeyHint()));
        }
        return Optional.empty();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;
        if (handle.update()) {
            return new IcebergUpdateBucketFunction(bucketCount);
        }

        return new IcebergBucketFunction(handle, typeOperators, bucketCount);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            int bucketCount)
    {
        return new IcebergBucketFunction((IcebergPartitioningHandle) partitioningHandle, typeOperators, bucketCount);
    }
}
