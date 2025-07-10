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
package io.trino.node;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.client.NodeVersion;
import io.trino.spi.HostAddress;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@ThreadSafe
public class TestingInternalNodeManager
        implements InternalNodeManager
{
    public static final InternalNode CURRENT_NODE = new InternalNode("local", URI.create("local://127.0.0.1:8080"), NodeVersion.UNKNOWN, true);

    private final InternalNode currentNode;
    private final ExecutorService nodeStateEventExecutor;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public TestingInternalNodeManager(InternalNode currentNode)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        checkArgument(currentNode.isCoordinator(), "currentNode must be a coordinator: %s", currentNode);

        this.allNodes = new AllNodes(
                ImmutableSet.of(currentNode),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(currentNode));
        this.nodeStateEventExecutor = newSingleThreadExecutor(daemonThreadsNamed("node-state-events-%s"));
    }

    public static TestingInternalNodeManager createDefault(InternalNode... remoteNodes)
    {
        return createDefault(ImmutableSet.copyOf(remoteNodes));
    }

    public static TestingInternalNodeManager createDefault(Set<InternalNode> remoteNodes)
    {
        TestingInternalNodeManager nodeManager = new TestingInternalNodeManager(CURRENT_NODE);
        nodeManager.addNodes(remoteNodes);
        return nodeManager;
    }

    public void addNodes(InternalNode... internalNodes)
    {
        addNodes(ImmutableSet.copyOf(internalNodes));
    }

    public synchronized void addNodes(Collection<InternalNode> internalNodes)
    {
        checkArgument(internalNodes.stream().noneMatch(currentNode::equals), "Cannot add current node");
        Set<InternalNode> newActiveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(allNodes.activeNodes())
                .addAll(internalNodes)
                .build();

        setAllNodes(new AllNodes(
                newActiveNodes,
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                newActiveNodes.stream()
                        .filter(InternalNode::isCoordinator)
                        .collect(toImmutableSet())));
    }

    public synchronized void removeNode(InternalNode internalNode)
    {
        requireNonNull(internalNode, "internalNode is null");
        checkArgument(!currentNode.equals(internalNode), "Cannot remove current node");

        Set<InternalNode> newActiveNodes = new HashSet<>(allNodes.activeNodes());
        newActiveNodes.remove(internalNode);

        setAllNodes(new AllNodes(
                newActiveNodes,
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                newActiveNodes.stream()
                        .filter(InternalNode::isCoordinator)
                        .collect(toImmutableSet())));
    }

    @GuardedBy("this")
    private void setAllNodes(AllNodes newAllNodes)
    {
        // did the node set change?
        if (newAllNodes.equals(allNodes)) {
            return;
        }
        allNodes = newAllNodes;

        // notify listeners
        List<Consumer<AllNodes>> listeners = ImmutableList.copyOf(this.listeners);
        nodeStateEventExecutor.submit(() -> listeners.forEach(listener -> listener.accept(newAllNodes)));
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        return allNodes;
    }

    @Override
    public boolean isGone(HostAddress hostAddress)
    {
        return false;
    }

    @Override
    public boolean refreshNodes(boolean forceAndWait)
    {
        return true;
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        AllNodes allNodes = this.allNodes;
        nodeStateEventExecutor.submit(() -> listener.accept(allNodes));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }
}
