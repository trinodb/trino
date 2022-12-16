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
import io.trino.spi.connector.CatalogHandle;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

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

    void addNodeChangeListener(Consumer<AllNodes> listener);

    void removeNodeChangeListener(Consumer<AllNodes> listener);

    class NodesSnapshot
    {
        private final Set<InternalNode> allNodes;
        private final Optional<SetMultimap<CatalogHandle, InternalNode>> connectorNodes;

        public NodesSnapshot(Set<InternalNode> allActiveNodes, Optional<SetMultimap<CatalogHandle, InternalNode>> activeNodesByCatalogName)
        {
            requireNonNull(allActiveNodes, "allActiveNodes is null");
            requireNonNull(activeNodesByCatalogName, "activeNodesByCatalogName is null");
            this.allNodes = ImmutableSet.copyOf(allActiveNodes);
            this.connectorNodes = activeNodesByCatalogName.map(ImmutableSetMultimap::copyOf);
        }

        public Set<InternalNode> getAllNodes()
        {
            return allNodes;
        }

        public Set<InternalNode> getConnectorNodes(CatalogHandle catalogHandle)
        {
            return connectorNodes
                    .map(map -> map.get(catalogHandle))
                    .orElse(allNodes);
        }
    }
}
