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
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.client.NodeVersion;
import io.trino.spi.connector.CatalogHandle;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class InMemoryNodeManager
        implements InternalNodeManager
{
    private static final InternalNode CURRENT_NODE = new InternalNode("local", URI.create("local://127.0.0.1:8080"), NodeVersion.UNKNOWN, true);
    private final Set<InternalNode> allNodes = ConcurrentHashMap.newKeySet();

    public InMemoryNodeManager(InternalNode... remoteNodes)
    {
        this(ImmutableSet.copyOf(remoteNodes));
    }

    public InMemoryNodeManager(Set<InternalNode> remoteNodes)
    {
        allNodes.add(CURRENT_NODE);
        allNodes.addAll(remoteNodes);
    }

    public void addNodes(InternalNode... internalNodes)
    {
        for (InternalNode internalNode : internalNodes) {
            allNodes.add(requireNonNull(internalNode, "internalNode is null"));
        }
    }

    public void removeNode(InternalNode internalNode)
    {
        allNodes.remove(internalNode);
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        switch (state) {
            case ACTIVE:
                return ImmutableSet.copyOf(allNodes);
            case INACTIVE:
            case SHUTTING_DOWN:
                return ImmutableSet.of();
        }
        throw new IllegalArgumentException("Unknown node state " + state);
    }

    @Override
    public Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle)
    {
        return ImmutableSet.copyOf(allNodes);
    }

    @Override
    public NodesSnapshot getActiveNodesSnapshot()
    {
        return new NodesSnapshot(ImmutableSet.copyOf(allNodes), Optional.empty());
    }

    @Override
    public AllNodes getAllNodes()
    {
        return new AllNodes(
                ImmutableSet.copyOf(allNodes),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(CURRENT_NODE));
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return CURRENT_NODE;
    }

    @Override
    public Set<InternalNode> getCoordinators()
    {
        // always use localNode as coordinator
        return ImmutableSet.of(CURRENT_NODE);
    }

    @Override
    public void refreshNodes() {}

    @Override
    public void addNodeChangeListener(Consumer<AllNodes> listener) {}

    @Override
    public void removeNodeChangeListener(Consumer<AllNodes> listener) {}
}
