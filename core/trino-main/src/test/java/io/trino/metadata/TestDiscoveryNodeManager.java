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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.airlift.testing.TestingTicker;
import io.trino.client.NodeVersion;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.server.InternalCommunicationConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.discovery.client.ServiceSelectorConfig.DEFAULT_POOL;
import static io.airlift.http.client.HttpStatus.OK;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDiscoveryNodeManager
{
    private static final byte[] ACTIVE_JSON = ("\"" + ACTIVE + "\"").getBytes(UTF_8);
    private static final byte[] INACTIVE_JSON = ("\"" + INACTIVE + "\"").getBytes(UTF_8);

    private final NodeInfo nodeInfo = new NodeInfo("test");
    private final InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig();
    private final NodeVersion expectedVersion;
    private final Set<InternalNode> activeNodes;
    private final Set<String> activeHosts;
    private final Set<InternalNode> inactiveNodes;
    private final InternalNode coordinator;
    private final InternalNode currentNode;
    private final TestingHttpClient httpClient;

    TestDiscoveryNodeManager()
    {
        expectedVersion = new NodeVersion("1");
        coordinator = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), expectedVersion, true);
        currentNode = new InternalNode(nodeInfo.getNodeId(), URI.create("http://192.0.1.1"), expectedVersion, false);

        activeNodes = ImmutableSet.of(
                currentNode,
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.1:8080"), expectedVersion, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("http://192.0.2.3"), expectedVersion, false),
                coordinator);
        activeHosts = activeNodes.stream()
                .map(InternalNode::getHost)
                .collect(toImmutableSet());
        inactiveNodes = ImmutableSet.of(
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.3.9"), expectedVersion, false),
                new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.4.9"), expectedVersion, false));

        httpClient = new TestingHttpClient(input -> new TestingResponse(
                OK,
                ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                activeHosts.contains(input.getUri().getHost()) ? ACTIVE_JSON : INACTIVE_JSON));
    }

    @Test
    void testGetAllNodes()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                new TrinoNodeServiceSelector(),
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                httpClient,
                internalCommunicationConfig);
        try {
            AllNodes allNodes = manager.getAllNodes();

            Set<InternalNode> connectorNodes = manager.getNodes(ACTIVE);
            assertThat(connectorNodes).hasSize(4);
            assertThat(connectorNodes.stream().anyMatch(InternalNode::isCoordinator)).isTrue();

            Set<InternalNode> activeNodes = allNodes.getActiveNodes();
            assertThat(activeNodes).containsExactlyInAnyOrderElementsOf(this.activeNodes);

            for (InternalNode actual : activeNodes) {
                for (InternalNode expected : this.activeNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertThat(activeNodes).containsExactlyInAnyOrderElementsOf(manager.getNodes(ACTIVE));

            Set<InternalNode> inactiveNodes = allNodes.getInactiveNodes();
            assertThat(inactiveNodes).containsExactlyInAnyOrderElementsOf(this.inactiveNodes);

            for (InternalNode actual : inactiveNodes) {
                for (InternalNode expected : this.inactiveNodes) {
                    assertThat(actual).isNotSameAs(expected);
                }
            }

            assertThat(inactiveNodes).containsExactlyInAnyOrderElementsOf(manager.getNodes(INACTIVE));
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testGetCurrentNode()
    {
        NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                .setEnvironment("test")
                .setNodeId(currentNode.getNodeIdentifier()));

        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                new TrinoNodeServiceSelector(),
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                httpClient,
                internalCommunicationConfig);
        try {
            assertThat(manager.getCurrentNode()).isEqualTo(currentNode);
        }
        finally {
            manager.stop();
        }
    }

    @Test
    void testGetCoordinators()
    {
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                new TrinoNodeServiceSelector(),
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                httpClient,
                internalCommunicationConfig);
        try {
            assertThat(manager.getCoordinators()).isEqualTo(ImmutableSet.of(coordinator));
        }
        finally {
            manager.stop();
        }
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test
    void testGetCurrentNodeRequired()
    {
        assertThatThrownBy(() -> new DiscoveryNodeManager(
                new TrinoNodeServiceSelector(),
                new NodeInfo("test"),
                new NoOpFailureDetector(),
                expectedVersion,
                httpClient,
                internalCommunicationConfig))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("current node not returned");
    }

    @Test
    @Timeout(60)
    void testNodeChangeListener()
            throws Exception
    {
        // initially only the current node is announced
        TrinoNodeServiceSelector selector = new TrinoNodeServiceSelector();
        selector.announceNodes(ImmutableSet.of(currentNode), ImmutableSet.of());
        TestingTicker testingTicker = new TestingTicker();
        DiscoveryNodeManager manager = new DiscoveryNodeManager(
                selector,
                nodeInfo,
                new NoOpFailureDetector(),
                expectedVersion,
                httpClient,
                internalCommunicationConfig,
                testingTicker);
        try {
            BlockingQueue<AllNodes> notifications = new ArrayBlockingQueue<>(100);
            manager.addNodeChangeListener(notifications::add);
            AllNodes allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.getActiveNodes()).containsExactly(currentNode);
            assertThat(allNodes.getInactiveNodes()).isEmpty();

            // announce all nodes
            testingTicker.increment(5, SECONDS);
            selector.announceNodes(activeNodes, inactiveNodes);
            manager.refreshNodes();
            allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.getActiveNodes()).isEqualTo(activeNodes);
            assertThat(allNodes.getActiveCoordinators()).isEqualTo(ImmutableSet.of(coordinator));

            // only announce current node and inactive nodes
            // node manager tracks all nodes until they have not been seen for a while
            testingTicker.increment(30, SECONDS);
            selector.announceNodes(ImmutableSet.of(currentNode), inactiveNodes);
            manager.refreshNodes();
            allNodes = notifications.take();
            assertThat(manager.getAllNodes()).isSameAs(allNodes);
            assertThat(allNodes.getActiveNodes()).containsExactly(currentNode);
            assertThat(allNodes.getInactiveNodes()).isEqualTo(inactiveNodes);
        }
        finally {
            manager.stop();
        }
    }

    private final class TrinoNodeServiceSelector
            implements ServiceSelector
    {
        @GuardedBy("this")
        private List<ServiceDescriptor> descriptors = ImmutableList.of();

        public TrinoNodeServiceSelector()
        {
            announceNodes(activeNodes, inactiveNodes);
        }

        @SafeVarargs
        private synchronized void announceNodes(Set<InternalNode>... nodeSets)
        {
            ImmutableList.Builder<ServiceDescriptor> descriptors = ImmutableList.builder();
            Stream.of(nodeSets)
                    .flatMap(Set::stream)
                    .map(node -> serviceDescriptor("trino")
                            .setNodeId(node.getNodeIdentifier())
                            .addProperty("http", node.getInternalUri().toString())
                            .addProperty("node_version", node.getNodeVersion().toString())
                            .addProperty("coordinator", String.valueOf(node.isCoordinator()))
                            .build())
                    .forEach(descriptors::add);

            // Also add some nodes with bad versions
            descriptors.add(serviceDescriptor("trino")
                    .setNodeId("bad_version_unknown")
                    .addProperty("http", "https://192.0.7.1")
                    .addProperty("node_version", NodeVersion.UNKNOWN.toString())
                    .addProperty("coordinator", "false")
                    .build());
            descriptors.add(serviceDescriptor("trino")
                    .setNodeId("bad_version_2")
                    .addProperty("http", "https://192.0.7.2")
                    .addProperty("node_version", "2")
                    .addProperty("coordinator", "false")
                    .build());

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
