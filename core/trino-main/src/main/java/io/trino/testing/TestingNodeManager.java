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
package io.trino.testing;

import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static java.util.Objects.requireNonNull;

public class TestingNodeManager
        implements NodeManager
{
    private final Node localNode;
    private final Set<Node> nodes = new CopyOnWriteArraySet<>();
    private final boolean scheduleOnCoordinator;

    private TestingNodeManager(Node localNode, Collection<Node> otherNodes, boolean scheduleOnCoordinator)
    {
        this.localNode = requireNonNull(localNode, "localNode is null");
        this.scheduleOnCoordinator = scheduleOnCoordinator;
        nodes.add(localNode);
        nodes.addAll(otherNodes);
    }

    public void addNode(Node node)
    {
        nodes.add(node);
    }

    public void removeNode(Node node)
    {
        nodes.remove(node);
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return nodes;
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        if (scheduleOnCoordinator) {
            return nodes;
        }
        return nodes.stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableSet());
    }

    @Override
    public Node getCurrentNode()
    {
        return localNode;
    }

    public static TestingNodeManager create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Node localNode = CURRENT_NODE;
        private final Set<Node> otherNodes = new HashSet<>();
        private boolean scheduleOnCoordinator = true;

        public Builder localNode(Node localNode)
        {
            this.localNode = requireNonNull(localNode, "localNode is null");
            return this;
        }

        public Builder addNode(Node node)
        {
            otherNodes.add(requireNonNull(node, "node is null"));
            return this;
        }

        public Builder addNodes(Collection<Node> nodes)
        {
            otherNodes.addAll(requireNonNull(nodes, "nodes is null"));
            return this;
        }

        public Builder doNotScheduleOnCoordinator()
        {
            this.scheduleOnCoordinator = false;
            return this;
        }

        public TestingNodeManager build()
        {
            return new TestingNodeManager(localNode, otherNodes, scheduleOnCoordinator);
        }
    }
}
