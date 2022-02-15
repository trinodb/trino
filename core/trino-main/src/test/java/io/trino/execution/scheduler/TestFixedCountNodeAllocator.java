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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.execution.scheduler.TestingNodeSelectorFactory.TestingNodeSupplier;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

// uses mutable state
@Test(singleThreaded = true)
public class TestFixedCountNodeAllocator
{
    private static final Session SESSION = testSessionBuilder().build();

    private static final HostAddress NODE_1_ADDRESS = HostAddress.fromParts("127.0.0.1", 8080);
    private static final HostAddress NODE_2_ADDRESS = HostAddress.fromParts("127.0.0.1", 8081);
    private static final HostAddress NODE_3_ADDRESS = HostAddress.fromParts("127.0.0.1", 8082);

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://" + NODE_1_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://" + NODE_2_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://" + NODE_3_ADDRESS), NodeVersion.UNKNOWN, false);

    private static final CatalogName CATALOG_1 = new CatalogName("catalog1");
    private static final CatalogName CATALOG_2 = new CatalogName("catalog2");

    private FixedCountNodeAllocatorService nodeAllocatorService;

    private void setupNodeAllocatorService(TestingNodeSupplier testingNodeSupplier)
    {
        shutdownNodeAllocatorService(); // just in case
        nodeAllocatorService = new FixedCountNodeAllocatorService(new NodeScheduler(new TestingNodeSelectorFactory(NODE_1, testingNodeSupplier)));
    }

    @AfterMethod(alwaysRun = true)
    public void shutdownNodeAllocatorService()
    {
        if (nodeAllocatorService != null) {
            nodeAllocatorService.stop();
        }
        nodeAllocatorService = null;
    }

    @Test
    public void testSingleNode()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire2.isDone());

            nodeAllocator.release(acquire1.get());

            assertTrue(acquire2.isDone());
            assertEquals(acquire2.get().getNode(), NODE_1);
        }

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 2)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire2.isDone());
            assertEquals(acquire2.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire3.isDone());

            ListenableFuture<NodeInfo> acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire4.isDone());

            nodeAllocator.release(acquire2.get()); // NODE_1
            assertTrue(acquire3.isDone());
            assertEquals(acquire3.get().getNode(), NODE_1);

            nodeAllocator.release(acquire3.get()); // NODE_1
            assertTrue(acquire4.isDone());
            assertEquals(acquire4.get().getNode(), NODE_1);
        }
    }

    @Test
    public void testMultipleNodes()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of(), NODE_2, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire2.isDone());
            assertEquals(acquire2.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire3.isDone());

            ListenableFuture<NodeInfo> acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire4.isDone());

            ListenableFuture<NodeInfo> acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire5.isDone());

            nodeAllocator.release(acquire2.get()); // NODE_2
            assertTrue(acquire3.isDone());
            assertEquals(acquire3.get().getNode(), NODE_2);

            nodeAllocator.release(acquire1.get()); // NODE_1
            assertTrue(acquire4.isDone());
            assertEquals(acquire4.get().getNode(), NODE_1);

            nodeAllocator.release(acquire4.get()); // NODE_1
            assertTrue(acquire5.isDone());
            assertEquals(acquire5.get().getNode(), NODE_1);
        }

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 2)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire2.isDone());
            assertEquals(acquire2.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire3.isDone());
            assertEquals(acquire3.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire4.isDone());
            assertEquals(acquire4.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire5.isDone());

            ListenableFuture<NodeInfo> acquire6 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire6.isDone());

            nodeAllocator.release(acquire2.get()); // NODE_2
            assertTrue(acquire5.isDone());
            assertEquals(acquire5.get().getNode(), NODE_2);

            nodeAllocator.release(acquire1.get()); // NODE_1
            assertTrue(acquire6.isDone());
            assertEquals(acquire6.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire7 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire7.isDone());

            nodeAllocator.release(acquire3.get()); // NODE_1
            assertTrue(acquire7.isDone());
            assertEquals(acquire7.get().getNode(), NODE_1);

            nodeAllocator.release(acquire6.get()); // NODE_1
            nodeAllocator.release(acquire5.get()); // NODE_2
            nodeAllocator.release(acquire4.get()); // NODE_2

            ListenableFuture<NodeInfo> acquire8 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire8.isDone());
            assertEquals(acquire8.get().getNode(), NODE_2);
        }
    }

    @Test
    public void testCatalogRequirement()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG_1),
                NODE_2, ImmutableList.of(CATALOG_2),
                NODE_3, ImmutableList.of(CATALOG_1, CATALOG_2)));

        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> catalog1acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()));
            assertTrue(catalog1acquire1.isDone());
            assertEquals(catalog1acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> catalog1acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()));
            assertTrue(catalog1acquire2.isDone());
            assertEquals(catalog1acquire2.get().getNode(), NODE_3);

            ListenableFuture<NodeInfo> catalog1acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()));
            assertFalse(catalog1acquire3.isDone());

            ListenableFuture<NodeInfo> catalog2acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()));
            assertTrue(catalog2acquire1.isDone());
            assertEquals(catalog2acquire1.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> catalog2acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()));
            assertFalse(catalog2acquire2.isDone());

            nodeAllocator.release(catalog2acquire1.get()); // NODE_2
            assertFalse(catalog1acquire3.isDone());
            assertTrue(catalog2acquire2.isDone());
            assertEquals(catalog2acquire2.get().getNode(), NODE_2);

            nodeAllocator.release(catalog1acquire1.get()); // NODE_1
            assertTrue(catalog1acquire3.isDone());
            assertEquals(catalog1acquire3.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> catalog1acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()));
            assertFalse(catalog1acquire4.isDone());

            ListenableFuture<NodeInfo> catalog2acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()));
            assertFalse(catalog2acquire4.isDone());

            nodeAllocator.release(catalog1acquire2.get()); // NODE_3
            assertFalse(catalog2acquire4.isDone());
            assertTrue(catalog1acquire4.isDone());
            assertEquals(catalog1acquire4.get().getNode(), NODE_3);

            nodeAllocator.release(catalog1acquire4.get()); // NODE_3
            assertTrue(catalog2acquire4.isDone());
            assertEquals(catalog2acquire4.get().getNode(), NODE_3);
        }
    }

    @Test
    public void testCancellation()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire2.isDone());

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire3.isDone());

            acquire2.cancel(true);

            nodeAllocator.release(acquire1.get()); // NODE_1
            assertTrue(acquire3.isDone());
            assertEquals(acquire3.get().getNode(), NODE_1);
        }
    }

    @Test
    public void testAddNode()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire2.isDone());

            nodeSupplier.addNode(NODE_2, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            assertEquals(acquire2.get(10, SECONDS).getNode(), NODE_2);
        }
    }

    @Test
    public void testRemoveNode()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_1);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire2.isDone());

            nodeSupplier.removeNode(NODE_1);
            nodeSupplier.addNode(NODE_2, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            assertEquals(acquire2.get(10, SECONDS).getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()));
            assertFalse(acquire3.isDone());

            nodeAllocator.release(acquire1.get()); // NODE_1
            assertFalse(acquire3.isDone());
        }
    }

    @Test
    public void testAddressRequirement()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of(), NODE_2, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            ListenableFuture<NodeInfo> acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_2_ADDRESS)));
            assertTrue(acquire1.isDone());
            assertEquals(acquire1.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_2_ADDRESS)));
            assertFalse(acquire2.isDone());

            nodeAllocator.release(acquire1.get()); // NODE_2

            assertTrue(acquire2.isDone());
            assertEquals(acquire2.get().getNode(), NODE_2);

            ListenableFuture<NodeInfo> acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)));
            assertTrue(acquire3.isDone());
            assertThatThrownBy(acquire3::get)
                    .hasMessageContaining("No nodes available to run query");

            nodeSupplier.addNode(NODE_3, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            ListenableFuture<NodeInfo> acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)));
            assertTrue(acquire4.isDone());
            assertEquals(acquire4.get().getNode(), NODE_3);

            ListenableFuture<NodeInfo> acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)));
            assertFalse(acquire5.isDone());

            nodeSupplier.removeNode(NODE_3);
            nodeAllocatorService.updateNodes();

            assertTrue(acquire5.isDone());
            assertThatThrownBy(acquire5::get)
                    .hasMessageContaining("No nodes available to run query");
        }
    }

    private NodeScheduler createNodeScheduler(TestingNodeSupplier testingNodeSupplier)
    {
        return new NodeScheduler(new TestingNodeSelectorFactory(NODE_1, testingNodeSupplier));
    }
}
