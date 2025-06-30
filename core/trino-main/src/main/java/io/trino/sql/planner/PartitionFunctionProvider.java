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
package io.trino.sql.planner;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionFunctionProvider
{
    private final TypeOperators typeOperators;
    private final CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider;

    @Inject
    public PartitionFunctionProvider(TypeOperators typeOperators, CatalogServiceProvider<ConnectorNodePartitioningProvider> partitioningProvider)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
    }

    public PartitionFunction getPartitionFunction(Session session, PartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int[] bucketToPartition)
    {
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle handle) {
            return handle.getPartitionFunction(partitionChannelTypes, bucketToPartition, typeOperators);
        }

        if (partitioningHandle.getConnectorHandle() instanceof MergePartitioningHandle handle) {
            return handle.getPartitionFunction(
                    (scheme, types) -> getPartitionFunction(session, scheme.getPartitioning().getHandle(), types, bucketToPartition),
                    partitionChannelTypes,
                    bucketToPartition);
        }

        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioningHandle);

        BucketFunction bucketFunction = partitioningProvider.getBucketFunction(
                partitioningHandle.getTransactionHandle().orElseThrow(),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle(),
                partitionChannelTypes,
                bucketToPartition.length);
        checkArgument(bucketFunction != null, "No bucket function for partitioning: %s", partitioningHandle);
        return new BucketPartitionFunction(bucketFunction, bucketToPartition);
    }

    // NOTE: Do not access any function other than getBucketFunction as the other functions are not usable on workers
    private ConnectorNodePartitioningProvider getPartitioningProvider(PartitioningHandle partitioningHandle)
    {
        CatalogHandle catalogHandle = partitioningHandle.getCatalogHandle().orElseThrow(() ->
                new IllegalStateException("No catalog handle for partitioning handle: " + partitioningHandle));
        return partitioningProvider.getService(requireNonNull(catalogHandle, "catalogHandle is null"));
    }
}
