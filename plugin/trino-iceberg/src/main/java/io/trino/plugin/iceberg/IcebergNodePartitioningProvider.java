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

import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final TypeOperators typeOperators;

    @Inject
    public IcebergNodePartitioningProvider(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeOperators = requireNonNull(typeManager, "typeManager is null").getTypeOperators();
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        if (partitioningHandle instanceof IcebergUpdateHandle) {
            return Optional.empty();
        }

        return Optional.of(createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size()));
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        if (partitioningHandle instanceof IcebergUpdateHandle) {
            return new IcebergUpdateBucketFunction(bucketCount);
        }

        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;
        Schema schema = schemaFromHandles(handle.getPartitioningColumns());
        return new IcebergBucketFunction(
                typeOperators,
                parsePartitionFields(schema, handle.getPartitioning()),
                handle.getPartitioningColumns(),
                bucketCount);
    }
}
