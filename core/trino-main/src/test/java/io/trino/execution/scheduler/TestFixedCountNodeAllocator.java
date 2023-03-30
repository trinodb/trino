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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.scheduler.TestingNodeSelectorFactory.TestingNodeSupplier;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
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

    private static final CatalogHandle CATALOG_1 = createTestCatalogHandle("catalog1");
    private static final CatalogHandle CATALOG_2 = createTestCatalogHandle("catalog2");

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
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire2.getNode().isDone());

            acquire1.release();

            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_1);
        }

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 2)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire3.getNode().isDone());

            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire4.getNode().isDone());

            acquire2.release(); // NODE_1
            assertTrue(acquire3.getNode().isDone());
            assertEquals(acquire3.getNode().get(), NODE_1);

            acquire3.release(); // NODE_1
            assertTrue(acquire4.getNode().isDone());
            assertEquals(acquire4.getNode().get(), NODE_1);
        }
    }

    @Test
    public void testMultipleNodes()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of(), NODE_2, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_2);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire3.getNode().isDone());

            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire4.getNode().isDone());

            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire5.getNode().isDone());

            acquire2.release(); // NODE_2
            assertTrue(acquire3.getNode().isDone());
            assertEquals(acquire3.getNode().get(), NODE_2);

            acquire1.release(); // NODE_1
            assertTrue(acquire4.getNode().isDone());
            assertEquals(acquire4.getNode().get(), NODE_1);

            acquire4.release(); //NODE_1
            assertTrue(acquire5.getNode().isDone());
            assertEquals(acquire5.getNode().get(), NODE_1);
        }

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 2)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_2);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire3.getNode().isDone());
            assertEquals(acquire3.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire4.getNode().isDone());
            assertEquals(acquire4.getNode().get(), NODE_2);

            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire5.getNode().isDone());

            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire6.getNode().isDone());

            acquire4.release(); // NODE_2
            assertTrue(acquire5.getNode().isDone());
            assertEquals(acquire5.getNode().get(), NODE_2);

            acquire3.release(); // NODE_1
            assertTrue(acquire6.getNode().isDone());
            assertEquals(acquire6.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire7 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire7.getNode().isDone());

            acquire6.release(); // NODE_1
            assertTrue(acquire7.getNode().isDone());
            assertEquals(acquire7.getNode().get(), NODE_1);

            acquire7.release(); // NODE_1
            acquire5.release(); // NODE_2
            acquire2.release(); // NODE_2

            NodeAllocator.NodeLease acquire8 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire8.getNode().isDone());
            assertEquals(acquire8.getNode().get(), NODE_2);
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
            NodeAllocator.NodeLease catalog1acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(catalog1acquire1.getNode().isDone());
            assertEquals(catalog1acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease catalog1acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(catalog1acquire2.getNode().isDone());
            assertEquals(catalog1acquire2.getNode().get(), NODE_3);

            NodeAllocator.NodeLease catalog1acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(catalog1acquire3.getNode().isDone());

            NodeAllocator.NodeLease catalog2acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(catalog2acquire1.getNode().isDone());
            assertEquals(catalog2acquire1.getNode().get(), NODE_2);

            NodeAllocator.NodeLease catalog2acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(catalog2acquire2.getNode().isDone());

            catalog2acquire1.release(); // NODE_2
            assertFalse(catalog1acquire3.getNode().isDone());
            assertTrue(catalog2acquire2.getNode().isDone());
            assertEquals(catalog2acquire2.getNode().get(), NODE_2);

            catalog1acquire1.release(); // NODE_1
            assertTrue(catalog1acquire3.getNode().isDone());
            assertEquals(catalog1acquire3.getNode().get(), NODE_1);

            NodeAllocator.NodeLease catalog1acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(catalog1acquire4.getNode().isDone());

            NodeAllocator.NodeLease catalog2acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_2), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(catalog2acquire4.getNode().isDone());

            catalog1acquire2.release(); // NODE_3
            assertFalse(catalog2acquire4.getNode().isDone());
            assertTrue(catalog1acquire4.getNode().isDone());
            assertEquals(catalog1acquire4.getNode().get(), NODE_3);

            catalog1acquire4.release(); // NODE_3
            assertTrue(catalog2acquire4.getNode().isDone());
            assertEquals(catalog2acquire4.getNode().get(), NODE_3);
        }
    }

    @Test
    public void testReleaseBeforeAcquired()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire2.getNode().isDone());

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire3.getNode().isDone());

            acquire2.release();

            acquire1.release(); // NODE_1
            assertTrue(acquire3.getNode().isDone());
            assertEquals(acquire3.getNode().get(), NODE_1);
        }
    }

    @Test
    public void testAddNode()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire2.getNode().isDone());

            nodeSupplier.addNode(NODE_2, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            assertEquals(acquire2.getNode().get(10, SECONDS), NODE_2);
        }
    }

    @Test
    public void testRemoveNode()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire2.getNode().isDone());

            nodeSupplier.removeNode(NODE_1);
            nodeSupplier.addNode(NODE_2, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            assertEquals(acquire2.getNode().get(10, SECONDS), NODE_2);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of()), DataSize.of(1, GIGABYTE));
            assertFalse(acquire3.getNode().isDone());

            acquire1.release(); // NODE_1
            assertFalse(acquire3.getNode().isDone());
        }
    }

    @Test
    public void testAddressRequirement()
            throws Exception
    {
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(NODE_1, ImmutableList.of(), NODE_2, ImmutableList.of()));
        setupNodeAllocatorService(nodeSupplier);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_2_ADDRESS)), DataSize.of(1, GIGABYTE));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_2);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_2_ADDRESS)), DataSize.of(1, GIGABYTE));
            assertFalse(acquire2.getNode().isDone());

            acquire1.release(); // NODE_2

            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_2);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)), DataSize.of(1, GIGABYTE));
            assertTrue(acquire3.getNode().isDone());
            assertThatThrownBy(() -> acquire3.getNode().get())
                    .hasMessageContaining("No nodes available to run query");

            nodeSupplier.addNode(NODE_3, ImmutableList.of());
            nodeAllocatorService.updateNodes();

            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)), DataSize.of(1, GIGABYTE));
            assertTrue(acquire4.getNode().isDone());
            assertEquals(acquire4.getNode().get(), NODE_3);

            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(new NodeRequirements(Optional.empty(), ImmutableSet.of(NODE_3_ADDRESS)), DataSize.of(1, GIGABYTE));
            assertFalse(acquire5.getNode().isDone());

            nodeSupplier.removeNode(NODE_3);
            nodeAllocatorService.updateNodes();

            assertTrue(acquire5.getNode().isDone());
            assertThatThrownBy(() -> acquire5.getNode().get())
                    .hasMessageContaining("No nodes available to run query");
        }
    }
}
