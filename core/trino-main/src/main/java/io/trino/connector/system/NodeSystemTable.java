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
package io.trino.connector.system;

import com.google.inject.Inject;
import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.Locale;
import java.util.Set;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class NodeSystemTable
        implements SystemTable
{
    public static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("runtime", "nodes");

    public static final ConnectorTableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())
            .column("http_uri", createUnboundedVarcharType())
            .column("node_version", createUnboundedVarcharType())
            .column("coordinator", BOOLEAN)
            .column("state", createUnboundedVarcharType())
            .build();

    private final InternalNodeManager nodeManager;

    @Inject
    public NodeSystemTable(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return NODES_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(NODES_TABLE);
        AllNodes allNodes = nodeManager.getAllNodes();
        addRows(table, allNodes.getActiveNodes(), ACTIVE);
        addRows(table, allNodes.getInactiveNodes(), INACTIVE);
        addRows(table, allNodes.getShuttingDownNodes(), SHUTTING_DOWN);
        return table.build().cursor();
    }

    private void addRows(Builder table, Set<InternalNode> nodes, NodeState state)
    {
        for (InternalNode node : nodes) {
            table.addRow(node.getNodeIdentifier(), node.getInternalUri().toString(), getNodeVersion(node), isCoordinator(node), state.toString().toLowerCase(Locale.ENGLISH));
        }
    }

    private static String getNodeVersion(InternalNode node)
    {
        return node.getNodeVersion().toString();
    }

    private boolean isCoordinator(InternalNode node)
    {
        return nodeManager.getCoordinators().contains(node);
    }
}
