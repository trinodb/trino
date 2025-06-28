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

import com.google.common.collect.ImmutableSet;
import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TestingNodeManager
        implements NodeManager
{
    private static final String TEST_ENVIRONMENT = "testenv";
    public static final InternalNode DEFAULT_CURRENT_NODE = new InternalNode("local", URI.create("local://127.0.0.1"), NodeVersion.UNKNOWN, true);

    private final String environment;
    private final Node localNode;
    private final Set<Node> nodes = new CopyOnWriteArraySet<>();
    private final boolean scheduleOnCoordinator;

    public TestingNodeManager()
    {
        this(TEST_ENVIRONMENT);
    }

    public TestingNodeManager(boolean scheduleOnCoordinator)
    {
        this(TEST_ENVIRONMENT, scheduleOnCoordinator);
    }

    public TestingNodeManager(String environment)
    {
        this(environment, true);
    }

    public TestingNodeManager(String environment, boolean scheduleOnCoordinator)
    {
        this(environment, DEFAULT_CURRENT_NODE, ImmutableSet.of(), scheduleOnCoordinator);
    }

    public TestingNodeManager(Node localNode)
    {
        this(localNode, ImmutableSet.of());
    }

    public TestingNodeManager(List<Node> allNodes)
    {
        this(allNodes.iterator().next(), allNodes);
    }

    public TestingNodeManager(Node localNode, Collection<Node> otherNodes)
    {
        this(TEST_ENVIRONMENT, localNode, otherNodes, true);
    }

    public TestingNodeManager(String environment, Node localNode, Collection<Node> otherNodes, boolean scheduleOnCoordinator)
    {
        this.environment = environment;
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

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}
