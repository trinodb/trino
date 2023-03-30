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
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.CatalogHandle;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ConnectorAwareNodeManager
        implements NodeManager
{
    private final InternalNodeManager nodeManager;
    private final String environment;
    private final CatalogHandle catalogHandle;
    private final boolean schedulerIncludeCoordinator;

    public ConnectorAwareNodeManager(InternalNodeManager nodeManager, String environment, CatalogHandle catalogHandle, boolean schedulerIncludeCoordinator)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.schedulerIncludeCoordinator = schedulerIncludeCoordinator;
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return ImmutableSet.<Node>builder()
                .addAll(nodeManager.getActiveCatalogNodes(catalogHandle))
                // append current node (before connector is registered with the node
                // in the discovery service) since current node should have connector always loaded
                .add(nodeManager.getCurrentNode())
                .build();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        ImmutableSet.Builder<Node> nodes = ImmutableSet.builder();
        // getActiveConnectorNodes returns all nodes (including coordinators)
        // that have connector registered
        nodeManager.getActiveCatalogNodes(catalogHandle).stream()
                .filter(node -> !node.isCoordinator() || schedulerIncludeCoordinator)
                .forEach(nodes::add);
        if (!nodeManager.getCurrentNode().isCoordinator() || schedulerIncludeCoordinator) {
            // append current node (before connector is registered with the node
            // in discovery service) since current node should have connector always loaded
            nodes.add(getCurrentNode());
        }
        return nodes.build();
    }

    @Override
    public Node getCurrentNode()
    {
        return nodeManager.getCurrentNode();
    }

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}
