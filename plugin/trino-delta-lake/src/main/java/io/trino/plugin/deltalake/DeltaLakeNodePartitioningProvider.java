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
package io.trino.plugin.deltalake;

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
import java.util.function.ToIntFunction;

import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class DeltaLakeNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final TypeOperators typeOperators;
    private final NodeManager nodeManager;

    @Inject
    public DeltaLakeNodePartitioningProvider(TypeManager typeManager, NodeManager nodeManager)
    {
        this.typeOperators = requireNonNull(typeManager, "typeManager is null").getTypeOperators();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return split -> {
            throw new UnsupportedOperationException();
        };
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        DeltaLakePartitioningHandle handle = (DeltaLakePartitioningHandle) partitioningHandle;
        return new DeltaLakeBucketFunction(typeOperators, handle.getPartitioningColumns(), bucketCount);
    }
}
