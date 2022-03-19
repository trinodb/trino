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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Comparator.comparing;

public class TpchNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchNodePartitioningProvider(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No TPCH nodes available");
        // sort to ensure the assignment is consistent with TpchSplitManager
        List<Node> sortedNodes = nodes.stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList());
        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();
        for (Node node : sortedNodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                bucketToNode.add(node);
            }
        }
        return createBucketNodeMap(bucketToNode.build());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((TpchSplit) value).getPartNumber();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        long totalRows = ((TpchPartitioningHandle) partitioningHandle).getTotalRows();
        long rowsPerBucket = totalRows / bucketCount;
        checkArgument(partitionChannelTypes.equals(ImmutableList.of(BIGINT)), "Expected one BIGINT parameter");
        return new TpchBucketFunction(bucketCount, rowsPerBucket);
    }
}
