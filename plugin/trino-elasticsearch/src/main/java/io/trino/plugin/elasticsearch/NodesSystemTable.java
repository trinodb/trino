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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.ElasticsearchNode;
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
                    .add(new ColumnMetadata("presto_node_id", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("presto_node_address", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("elasticsearch_node_id", createUnboundedVarcharType()))
                    .add(new ColumnMetadata("elasticsearch_node_address", createUnboundedVarcharType()))
                    .build());

    private final ElasticsearchClient client;
    private final Node currentNode;

    @Inject
    public NodesSystemTable(NodeManager nodeManager, ElasticsearchClient client)
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
        Set<ElasticsearchNode> nodes = client.getNodes();

        BlockBuilder nodeId = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder prestoAddress = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder elasticsearchNodeId = VARCHAR.createBlockBuilder(null, nodes.size());
        BlockBuilder elasticsearchAddress = VARCHAR.createBlockBuilder(null, nodes.size());

        for (ElasticsearchNode node : nodes) {
            VARCHAR.writeString(nodeId, currentNode.getNodeIdentifier());
            VARCHAR.writeString(prestoAddress, currentNode.getHostAndPort().toString());
            VARCHAR.writeString(elasticsearchNodeId, node.getId());

            if (node.getAddress().isPresent()) {
                VARCHAR.writeString(elasticsearchAddress, node.getAddress().get());
            }
            else {
                elasticsearchAddress.appendNull();
            }
        }

        return new FixedPageSource(ImmutableList.of(new Page(
                nodeId.build(),
                prestoAddress.build(),
                elasticsearchNodeId.build(),
                elasticsearchAddress.build())));
    }
}
