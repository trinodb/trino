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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class InMemoryNodeManager
        implements InternalNodeManager
{
    private final InternalNode localNode;
    private final SetMultimap<CatalogName, InternalNode> remoteNodes = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public InMemoryNodeManager()
    {
        this(URI.create("local://127.0.0.1:8080"));
    }

    public InMemoryNodeManager(URI localUri)
    {
        localNode = new InternalNode("local", localUri, NodeVersion.UNKNOWN, true);
    }

    public void addCurrentNodeConnector(CatalogName catalogName)
    {
        addNode(catalogName, localNode);
    }

    public void addNode(CatalogName catalogName, InternalNode... nodes)
    {
        addNode(catalogName, ImmutableList.copyOf(nodes));
    }

    public void addNode(CatalogName catalogName, Iterable<InternalNode> nodes)
    {
        remoteNodes.putAll(catalogName, nodes);

        List<Consumer<AllNodes>> listeners;
        synchronized (this) {
            listeners = ImmutableList.copyOf(this.listeners);
        }
        AllNodes allNodes = getAllNodes();
        listeners.forEach(listener -> listener.accept(allNodes));
    }

    public void removeNode(InternalNode node)
    {
        for (CatalogName catalog : ImmutableSet.copyOf(remoteNodes.keySet())) {
            removeNode(catalog, node);
        }
    }

    public void removeNode(CatalogName catalogName, InternalNode node)
    {
        remoteNodes.remove(catalogName, node);
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        switch (state) {
            case ACTIVE:
                return getAllNodes().getActiveNodes();
            case INACTIVE:
                return getAllNodes().getInactiveNodes();
            case SHUTTING_DOWN:
                return getAllNodes().getShuttingDownNodes();
        }
        throw new IllegalArgumentException("Unknown node state " + state);
    }

    @Override
    public Set<InternalNode> getActiveConnectorNodes(CatalogName catalogName)
    {
        return ImmutableSet.copyOf(remoteNodes.get(catalogName));
    }

    @Override
    public NodesSnapshot getActiveNodesSnapshot()
    {
        Set<InternalNode> allActiveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(remoteNodes.values())
                .add(localNode)
                .build();
        return new NodesSnapshot(allActiveNodes, remoteNodes);
    }

    @Override
    public AllNodes getAllNodes()
    {
        return new AllNodes(ImmutableSet.<InternalNode>builder().add(localNode).addAll(remoteNodes.values()).build(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(localNode));
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return localNode;
    }

    @Override
    public Set<InternalNode> getCoordinators()
    {
        // always use localNode as coordinator
        return ImmutableSet.of(localNode);
    }

    @Override
    public void refreshNodes()
    {
        // no-op
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }
}
