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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeNodePartitioningProvider;
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeUpdateHandle;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.iceberg.IcebergNodePartitioningProvider;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public class LakehouseNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final HiveNodePartitioningProvider hiveNodePartitioningProvider;
    private final IcebergNodePartitioningProvider icebergNodePartitioningProvider;
    private final DeltaLakeNodePartitioningProvider deltaNodePartitioningProvider;

    @Inject
    public LakehouseNodePartitioningProvider(
            HiveNodePartitioningProvider hiveNodePartitioningProvider,
            IcebergNodePartitioningProvider icebergNodePartitioningProvider,
            DeltaLakeNodePartitioningProvider deltaNodePartitioningProvider)
    {
        this.hiveNodePartitioningProvider = requireNonNull(hiveNodePartitioningProvider, "hiveNodePartitioningProvider is null");
        this.icebergNodePartitioningProvider = requireNonNull(icebergNodePartitioningProvider, "icebergNodePartitioningProvider is null");
        this.deltaNodePartitioningProvider = requireNonNull(deltaNodePartitioningProvider, "deltaNodePartitioningProvider is null");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return forHandle(partitioningHandle).getBucketNodeMapping(transactionHandle, session, partitioningHandle);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, int bucketCount)
    {
        return forHandle(partitioningHandle).getSplitBucketFunction(transactionHandle, session, partitioningHandle, bucketCount);
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        return forHandle(partitioningHandle).getBucketFunction(transactionHandle, session, partitioningHandle, partitionChannelTypes, bucketCount);
    }

    private ConnectorNodePartitioningProvider forHandle(ConnectorPartitioningHandle handle)
    {
        return switch (handle) {
            case HivePartitioningHandle _ -> hiveNodePartitioningProvider;
            case IcebergPartitioningHandle _ -> icebergNodePartitioningProvider;
            case DeltaLakePartitioningHandle _, DeltaLakeUpdateHandle _ -> deltaNodePartitioningProvider;
            default -> throw new UnsupportedOperationException("Unsupported partitioning handle " + handle.getClass());
        };
    }
}
