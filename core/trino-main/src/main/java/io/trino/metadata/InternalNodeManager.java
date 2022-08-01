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
package io.trino.metadata;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.connector.CatalogHandle;
import io.trino.spi.HostAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

public interface InternalNodeManager
{
    Set<InternalNode> getNodes(NodeState state);

    Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle);

    NodesSnapshot getActiveNodesSnapshot();

    InternalNode getCurrentNode();

    Set<InternalNode> getCoordinators();

    AllNodes getAllNodes();

    void refreshNodes();

    NodeMap createNodeMap(Optional<CatalogHandle> catalogHandle);

    void addNodeChangeListener(Consumer<AllNodes> listener);

    void removeNodeChangeListener(Consumer<AllNodes> listener);

    class NodesSnapshot
    {
        private final Set<InternalNode> allNodes;
        private final SetMultimap<CatalogHandle, InternalNode> connectorNodes;

        public NodesSnapshot(Set<InternalNode> allActiveNodes, SetMultimap<CatalogHandle, InternalNode> activeNodesByCatalogName)
        {
            requireNonNull(allActiveNodes, "allActiveNodes is null");
            requireNonNull(activeNodesByCatalogName, "activeNodesByCatalogName is null");
            this.allNodes = ImmutableSet.copyOf(allActiveNodes);
            this.connectorNodes = ImmutableSetMultimap.copyOf(activeNodesByCatalogName);
        }

        public Set<InternalNode> getAllNodes()
        {
            return allNodes;
        }

        public Set<InternalNode> getConnectorNodes(CatalogHandle catalogHandle)
        {
            return connectorNodes.get(catalogHandle);
        }
    }

    static NodeMap createNodeMapInternal(InternalNodeManager nodeManager, Optional<CatalogHandle> catalogHandle, BiConsumer<UnknownHostException, InternalNode> unknownHostHandler)
    {
        Set<InternalNode> nodes = catalogHandle
                .map(nodeManager::getActiveCatalogNodes)
                .orElseGet(() -> nodeManager.getNodes(ACTIVE));

        Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        ImmutableSetMultimap.Builder<HostAddress, InternalNode> byHostAndPort = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<InetAddress, InternalNode> byHost = ImmutableSetMultimap.builder();
        for (InternalNode node : nodes) {
            try {
                byHostAndPort.put(node.getHostAndPort(), node);
                byHost.put(node.getInternalAddress(), node);
            }
            catch (UnknownHostException e) {
                unknownHostHandler.accept(e, node);
            }
        }

        return new NodeMap(byHostAndPort.build(), byHost.build(), ImmutableSetMultimap.of(), coordinatorNodeIds);
    }
}
