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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketAssignerNodes;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.dynamicBucketAssignerParallelism;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.keyDynamicAssignerNodes;
import static io.trino.plugin.paimon.PaimonDynamicBucketUtils.keyDynamicAssignerParallelism;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class PaimonNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;

    @Inject
    public PaimonNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        PaimonPartitioningHandle paimonPartitioningHandle = getPartitioningHandle(partitioningHandle);
        if (paimonPartitioningHandle.isSingleNode()) {
            return Optional.of(createBucketNodeMap(1));
        }
        TableSchema schema = paimonPartitioningHandle.getOriginalSchema();
        if (bucketMode(schema) == BucketMode.HASH_DYNAMIC) {
            if (paimonPartitioningHandle.dynamicBucketAssignerParallelism().isPresent()) {
                return Optional.of(createBucketNodeMap(dynamicBucketAssignerNodes(
                        nodeManager,
                        paimonPartitioningHandle.dynamicBucketAssignerParallelism().orElseThrow())));
            }
            return Optional.of(createBucketNodeMap(dynamicBucketAssignerNodes(
                    nodeManager,
                    new CoreOptions(schema.options()))));
        }
        if (bucketMode(schema) == BucketMode.KEY_DYNAMIC) {
            if (paimonPartitioningHandle.dynamicBucketAssignerParallelism().isPresent()) {
                return Optional.of(createBucketNodeMap(dynamicBucketAssignerNodes(
                        nodeManager,
                        paimonPartitioningHandle.dynamicBucketAssignerParallelism().orElseThrow())));
            }
            return Optional.of(createBucketNodeMap(keyDynamicAssignerNodes(
                    nodeManager,
                    new CoreOptions(schema.options()))));
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
        PaimonPartitioningHandle paimonPartitioningHandle = getPartitioningHandle(partitioningHandle);
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        partitionChannelTypes.forEach(type -> requireNonNull(type, "partitionChannelTypes contains null type"));
        checkArgument(bucketCount > 0, "bucketCount must be positive: %s", bucketCount);
        if (paimonPartitioningHandle.isSingleNode()) {
            return (_, _) -> 0;
        }
        TableSchema schema = paimonPartitioningHandle.getOriginalSchema();
        if (bucketMode(schema) == BucketMode.HASH_DYNAMIC) {
            int assignerCount = paimonPartitioningHandle.dynamicBucketAssignerParallelism()
                    .orElseGet(() -> dynamicBucketAssignerParallelism(new CoreOptions(schema.options()), bucketCount));
            checkArgument(assignerCount <= bucketCount,
                    "Paimon HASH_DYNAMIC assigner parallelism %s exceeds Trino bucket count %s",
                    assignerCount,
                    bucketCount);
            return new DynamicBucketTableShuffleFunction(partitionChannelTypes, paimonPartitioningHandle, assignerCount);
        }
        if (bucketMode(schema) == BucketMode.KEY_DYNAMIC) {
            int assignerCount = paimonPartitioningHandle.dynamicBucketAssignerParallelism()
                    .orElseGet(() -> keyDynamicAssignerParallelism(new CoreOptions(schema.options()), bucketCount));
            checkArgument(assignerCount <= bucketCount,
                    "Paimon KEY_DYNAMIC assigner parallelism %s exceeds Trino bucket count %s",
                    assignerCount,
                    bucketCount);
            return new DynamicBucketTableShuffleFunction(partitionChannelTypes, paimonPartitioningHandle, assignerCount);
        }
        return new FixedBucketTableShuffleFunction(partitionChannelTypes, paimonPartitioningHandle, bucketCount);
    }

    static PaimonPartitioningHandle getPartitioningHandle(ConnectorPartitioningHandle partitioningHandle)
    {
        if (!(requireNonNull(partitioningHandle, "partitioningHandle is null") instanceof PaimonPartitioningHandle paimonPartitioningHandle)) {
            throw new IllegalStateException("Paimon node partitioning requires PaimonPartitioningHandle, got: "
                    + partitioningHandle.getClass().getName());
        }
        return paimonPartitioningHandle;
    }

    private static BucketMode bucketMode(TableSchema schema)
    {
        requireNonNull(schema, "schema is null");
        int bucket = CoreOptions.fromMap(schema.options()).bucket();
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            return BucketMode.POSTPONE_MODE;
        }
        if (bucket != -1) {
            return BucketMode.HASH_FIXED;
        }
        if (schema.primaryKeys().isEmpty()) {
            return BucketMode.BUCKET_UNAWARE;
        }
        return schema.crossPartitionUpdate() ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
    }
}
