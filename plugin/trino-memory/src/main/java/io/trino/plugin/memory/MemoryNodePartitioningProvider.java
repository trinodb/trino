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
package io.trino.plugin.memory;

import com.google.inject.Inject;
import io.trino.spi.NodeManager;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.memory.MemoryNodesUtil.getSortedWorkerNodes;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class MemoryNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;

    @Inject
    public MemoryNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return Optional.of(createBucketNodeMap(getSortedWorkerNodes(nodeManager)));
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        MemoryPartitioningHandle handle = (MemoryPartitioningHandle) partitioningHandle;

        Map<String, Integer> nodeToBucket = handle.nodeToBucket();

        return (page, position) -> {
            Block block = page.getBlock(0);
            List<Block> rowIdBlocks = RowBlock.getRowFieldsFromBlock(block);
            checkState(rowIdBlocks.size() == 2, "Invalid partition channel count: %s", rowIdBlocks.size());
            Block storageBlock = rowIdBlocks.get(1);
            String storageNode = VARCHAR.getSlice(storageBlock, position).toStringUtf8();
            checkState(nodeToBucket.containsKey(storageNode), "Can't find node for storage node: %s", storageNode);
            return nodeToBucket.get(storageNode);
        };
    }
}
