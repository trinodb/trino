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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.client.OpenSearchNode;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class NodesSystemTable
        implements SystemTable
{
    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName("system", "nodes"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("trino_node_id", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("trino_node_address", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("opensearch_node_id", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("opensearch_node_address", createUnboundedVarcharType()))
                    .build());

    private final OpenSearchClient client;
    private final Node currentNode;

    @Inject
    public NodesSystemTable(NodeManager nodeManager, OpenSearchClient client)
    {
        requireNonNull(nodeManager, "nodeManager is null");

        this.client = requireNonNull(client, "client is null");
        currentNode = nodeManager.getCurrentNode();
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transaction, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Set<OpenSearchNode> nodes = client.getNodes();

        BlockBuilder nodeId = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder trinoAddress = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder opensearchNodeId = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder opensearchAddress = VARCHAR.createBlockBuilder(null, nodes.size());

        for (OpenSearchNode node : nodes) {
            VARCHAR.writeString(nodeId, currentNode.getNodeIdentifier());
            VARCHAR.writeString(trinoAddress, currentNode.getHostAndPort().toString());
            VARCHAR.writeString(opensearchNodeId, node.id());

            if (node.address().isPresent()) {
                VARCHAR.writeString(opensearchAddress, node.address().get());
            }
            else {
                opensearchAddress.appendNull();
            }
        }

        return new FixedPageSource(ImmutableList.of(new Page(
                nodeId.build(),
                trinoAddress.build(),
                opensearchNodeId.build(),
                opensearchAddress.build())));
    }
}
