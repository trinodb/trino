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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogManagerConfig;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.server.InternalCommunicationConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.discovery.client.ServiceSelectorConfig.DEFAULT_POOL;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestDiscoveryNodeManager
{
    private final NodeInfo nodeInfo = new NodeInfo("test");
    private final InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig();
    private NodeVersion expectedVersion;
    private Set<InternalNode> activeNodes;
    private Set<InternalNode> inactiveNodes;
    private InternalNode coordinator;
    private InternalNode currentNode;
    private final TrinoNodeServiceSelector selector = new TrinoNodeServiceSelector();
    private HttpClient testHttpClient;

    @BeforeEach
    public void setup()
    {
        testHttpClient = new TestingHttpClient(input -> new TestingResponse(OK, ArrayListMultimap.create(), ACTIVE.name().getBytes(UTF_8)));

        expectedVersion = new NodeVersion("1");
        coordinator = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), expectedVersion, true);
        currentNode = new InternalNode(nodeInfo.getNodeId(), URI.create("http://192.0.1.1"), expectedVersion, false);

        activeNodes = ImmutableSet.of(
                currentNode,
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), expectedVersion, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), expectedVersion, false),
                coordinator);
        inactiveNodes = ImmutableSet.of(
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), NodeVersion.UNKNOWN, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), new NodeVersion("2"), false));

        selector.announceNodes(activeNodes, inactiveNodes);
    }

    @AfterEach
    public void tearDown()
    {
        testHttpClient.close();
        testHttpClient = null;
    }

    @Test
    public void testGetAllNodes()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                selector,
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                testHttpClient,
                internalCommunicationConfig,
                new CatalogManagerConfig());
        try {
            AllNodes allNodes = manager.getAllNodes();

            Set<InternalNode> connectorNodes = manager.getActiveCatalogNodes(GlobalSystemConnector.CATALOG_HANDLE);
            assertThat(connectorNodes.size()).isEqualTo(4);
            assertThat(connectorNodes.stream().anyMatch(InternalNode::isCoordinator)).isTrue();

            Set<InternalNode> activeNodes = allNodes.getActiveNodes();
            assertEqualsIgnoreOrder(activeNodes, this.activeNodes);

            for (InternalNode actual : activeNodes) {
                for (InternalNode expected : this.activeNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertEqualsIgnoreOrder(activeNodes, manager.getNodes(ACTIVE));

            Set<InternalNode> inactiveNodes = allNodes.getInactiveNodes();
            assertEqualsIgnoreOrder(inactiveNodes, this.inactiveNodes);

            for (InternalNode actual : inactiveNodes) {
                for (InternalNode expected : this.inactiveNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertEqualsIgnoreOrder(inactiveNodes, manager.getNodes(INACTIVE));
        }
        finally {
            manager.stop();
        }
    }

    @Test
    public void testGetCurrentNode()
    {
        NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                .setEnvironment("test")
                .setNodeId(currentNode.getNodeIdentifier()));

        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                selector,
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                testHttpClient,
                internalCommunicationConfig,
                new CatalogManagerConfig());
        try {
            assertThat(manager.getCurrentNode()).isEqualTo(currentNode);
        }
        finally {
            manager.stop();
        }
    }

    @Test
    public void testGetCoordinators()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                selector,
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                testHttpClient,
                internalCommunicationConfig,
                new CatalogManagerConfig());
        try {
            assertThat(manager.getCoordinators()).isEqualTo(ImmutableSet.of(coordinator));
        }
        finally {
            manager.stop();
        }
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test
    public void testGetCurrentNodeRequired()
    {
        assertThatThrownBy(() -> new DiscoveryNodeManager(
                selector,
                new NodeInfo("test"),
                new NoOpFailureDetector(),
                expectedVersion,
                testHttpClient,
                internalCommunicationConfig,
                new CatalogManagerConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("current node not returned");
    }

    @Test
    @Timeout(60)
    public void testNodeChangeListener()
            throws Exception
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                selector,
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                testHttpClient,
                internalCommunicationConfig,
                new CatalogManagerConfig());
        try {
            manager.startPollingNodeStates();

            BlockingQueue<AllNodes> notifications = new ArrayBlockingQueue<>(100);
            manager.addNodeChangeListener(notifications::add);
            AllNodes allNodes = notifications.take();
            assertThat(allNodes.getActiveNodes()).isEqualTo(activeNodes);
            assertThat(allNodes.getInactiveNodes()).isEqualTo(inactiveNodes);

            selector.announceNodes(ImmutableSet.of(currentNode), ImmutableSet.of(coordinator));
            allNodes = notifications.take();
            assertThat(allNodes.getActiveNodes()).isEqualTo(ImmutableSet.of(currentNode, coordinator));
            assertThat(allNodes.getActiveCoordinators()).isEqualTo(ImmutableSet.of(coordinator));

            selector.announceNodes(activeNodes, inactiveNodes);
            allNodes = notifications.take();
            assertThat(allNodes.getActiveNodes()).isEqualTo(activeNodes);
            assertThat(allNodes.getInactiveNodes()).isEqualTo(inactiveNodes);
        }
        finally {
            manager.stop();
        }
    }

    public static class TrinoNodeServiceSelector
            implements ServiceSelector
    {
        @GuardedBy("this")
        private List<ServiceDescriptor> descriptors = ImmutableList.of();

        private synchronized void announceNodes(Set<InternalNode> activeNodes, Set<InternalNode> inactiveNodes)
        {
            ImmutableList.Builder<ServiceDescriptor> descriptors = ImmutableList.builder();
            for (InternalNode node : Iterables.concat(activeNodes, inactiveNodes)) {
                descriptors.add(serviceDescriptor("trino")
                        .setNodeId(node.getNodeIdentifier())
                        .addProperty("http", node.getInternalUri().toString())
                        .addProperty("node_version", node.getNodeVersion().toString())
                        .addProperty("coordinator", String.valueOf(node.isCoordinator()))
                        .build());
            }

            this.descriptors = descriptors.build();
        }

        @Override
        public String getType()
        {
            return "trino";
        }

        @Override
        public String getPool()
        {
            return DEFAULT_POOL;
        }

        @Override
        public synchronized List<ServiceDescriptor> selectAllServices()
        {
            return descriptors;
        }

        @Override
        public ListenableFuture<List<ServiceDescriptor>> refresh()
        {
            throw new UnsupportedOperationException();
        }
    }
}
