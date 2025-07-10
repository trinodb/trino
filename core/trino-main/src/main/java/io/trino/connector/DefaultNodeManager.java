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
package io.trino.connector;

import com.google.common.collect.ImmutableSet;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Set;

import static io.trino.node.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

public class DefaultNodeManager
        implements NodeManager
{
    private final InternalNode currentNode;
    private final InternalNodeManager nodeManager;
    private final boolean schedulerIncludeCoordinator;

    public DefaultNodeManager(InternalNode currentNode, InternalNodeManager nodeManager, boolean schedulerIncludeCoordinator)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.schedulerIncludeCoordinator = schedulerIncludeCoordinator;
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return ImmutableSet.<Node>builder()
                .addAll(nodeManager.getNodes(ACTIVE))
                // append current node (before connector is registered with the node
                // in the discovery service) since current node should have connector always loaded
                .add(currentNode)
                .build();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        ImmutableSet.Builder<Node> nodes = ImmutableSet.builder();
        // getActiveConnectorNodes returns all nodes (including coordinators)
        // that have connector registered
        nodeManager.getNodes(ACTIVE).stream()
                .filter(node -> !node.isCoordinator() || schedulerIncludeCoordinator)
                .forEach(nodes::add);
        if (!currentNode.isCoordinator() || schedulerIncludeCoordinator) {
            // append current node (before connector is registered with the node
            // in discovery service) since current node should have connector always loaded
            nodes.add(currentNode);
        }
        return nodes.build();
    }

    @Override
    public Node getCurrentNode()
    {
        return currentNode;
    }
}
