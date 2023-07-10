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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.MemoryInfo;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

// uses mutable state
@Test(singleThreaded = true)
public class TestBinPackingNodeAllocator
{
    private static final Session SESSION = testSessionBuilder().build();

    private static final HostAddress NODE_1_ADDRESS = HostAddress.fromParts("127.0.0.1", 8080);
    private static final HostAddress NODE_2_ADDRESS = HostAddress.fromParts("127.0.0.1", 8081);
    private static final HostAddress NODE_3_ADDRESS = HostAddress.fromParts("127.0.0.1", 8082);
    private static final HostAddress NODE_4_ADDRESS = HostAddress.fromParts("127.0.0.1", 8083);

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://" + NODE_1_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://" + NODE_2_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://" + NODE_3_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_4 = new InternalNode("node-4", URI.create("local://" + NODE_4_ADDRESS), NodeVersion.UNKNOWN, false);

    private static final CatalogHandle CATALOG_1 = createTestCatalogHandle("catalog1");

    private static final NodeRequirements REQ_NONE = new NodeRequirements(Optional.empty(), Set.of());
    private static final NodeRequirements REQ_NODE_1 = new NodeRequirements(Optional.empty(), Set.of(NODE_1_ADDRESS));
    private static final NodeRequirements REQ_NODE_2 = new NodeRequirements(Optional.empty(), Set.of(NODE_2_ADDRESS));
    private static final NodeRequirements REQ_CATALOG_1 = new NodeRequirements(Optional.of(CATALOG_1), Set.of());

    // none of the tests should require periodic execution of routine which processes pending acquisitions
    private static final long TEST_TIMEOUT = BinPackingNodeAllocatorService.PROCESS_PENDING_ACQUIRES_DELAY_SECONDS * 1000 / 2;

    private BinPackingNodeAllocatorService nodeAllocatorService;
    private ConcurrentHashMap<String, Optional<MemoryInfo>> workerMemoryInfos;
    private final TestingTicker ticker = new TestingTicker();

    private void setupNodeAllocatorService(InMemoryNodeManager nodeManager)
    {
        setupNodeAllocatorService(nodeManager, DataSize.ofBytes(0));
    }

    private void setupNodeAllocatorService(InMemoryNodeManager nodeManager, DataSize taskRuntimeMemoryEstimationOverhead)
    {
        shutdownNodeAllocatorService(); // just in case

        workerMemoryInfos = new ConcurrentHashMap<>();
        MemoryInfo memoryInfo = buildWorkerMemoryInfo(DataSize.ofBytes(0), ImmutableMap.of());
        workerMemoryInfos.put(NODE_1.getNodeIdentifier(), Optional.of(memoryInfo));
        workerMemoryInfos.put(NODE_2.getNodeIdentifier(), Optional.of(memoryInfo));
        workerMemoryInfos.put(NODE_3.getNodeIdentifier(), Optional.of(memoryInfo));
        workerMemoryInfos.put(NODE_4.getNodeIdentifier(), Optional.of(memoryInfo));

        nodeAllocatorService = new BinPackingNodeAllocatorService(
                nodeManager,
                () -> workerMemoryInfos,
                false,
                false,
                Duration.of(1, MINUTES),
                taskRuntimeMemoryEstimationOverhead,
                ticker);
        nodeAllocatorService.start();
    }

    private void updateWorkerUsedMemory(InternalNode node, DataSize usedMemory, Map<TaskId, DataSize> taskMemoryUsage)
    {
        workerMemoryInfos.put(node.getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(usedMemory, taskMemoryUsage)));
    }

    private MemoryInfo buildWorkerMemoryInfo(DataSize usedMemory, Map<TaskId, DataSize> taskMemoryUsage)
    {
        return new MemoryInfo(
                4,
                new MemoryPoolInfo(
                        DataSize.of(64, GIGABYTE).toBytes(),
                        usedMemory.toBytes(),
                        0,
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        taskMemoryUsage.entrySet().stream()
                                .collect(toImmutableMap(
                                        entry -> entry.getKey().toString(),
                                        entry -> entry.getValue().toBytes())),
                        ImmutableMap.of()));
    }

    @AfterMethod(alwaysRun = true)
    public void shutdownNodeAllocatorService()
    {
        if (nodeAllocatorService != null) {
            nodeAllocatorService.stop();
        }
        nodeAllocatorService = null;
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateSimple()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first two allocations should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);

            // same for subsequent two allocation (each task requires 32GB and we have 2 nodes with 64GB each)
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire3, NODE_1);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire4, NODE_2);

            // 5th allocation should block
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire5);

            // release acquire2 which uses
            acquire2.release();
            assertEventually(() -> {
                // we need to wait as pending acquires are processed asynchronously
                assertAcquired(acquire5);
                assertEquals(acquire5.getNode().get(), NODE_2);
            });

            // try to acquire one more node (should block)
            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire6);

            // add new node
            nodeManager.addNodes(NODE_3);
            // TODO: make BinPackingNodeAllocatorService react on new node added automatically
            nodeAllocatorService.processPendingAcquires();

            // new node should be assigned
            assertEventually(() -> {
                assertAcquired(acquire6);
                assertEquals(acquire6.getNode().get(), NODE_3);
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateDifferentSizes()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire3, NODE_1);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire4, NODE_2);
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire5, NODE_1);
            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire6, NODE_2);
            // each of the nodes is filled in with 32+16+16

            // try allocate 32 and 16
            NodeAllocator.NodeLease acquire7 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire7);

            NodeAllocator.NodeLease acquire8 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertNotAcquired(acquire8);

            // free 16MB on NODE_1;
            acquire3.release();
            // none of the pending allocations should be unblocked as NODE_1 is reserved for 32MB allocation which came first
            assertNotAcquired(acquire7);
            assertNotAcquired(acquire8);

            // release 16MB on NODE_2
            acquire4.release();
            // pending 16MB should be unblocked now
            assertAcquired(acquire8);

            // unblock another 16MB on NODE_1
            acquire5.release();
            // pending 32MB should be unblocked now
            assertAcquired(acquire7);
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateDifferentSizesOpportunisticAcquisition()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire3, NODE_1);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire4, NODE_2);
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire5, NODE_1);
            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire6, NODE_2);
            // each of the nodes is filled in with 32+16+16

            // try to allocate 32 and 16
            NodeAllocator.NodeLease acquire7 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire7);

            NodeAllocator.NodeLease acquire8 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertNotAcquired(acquire8);

            // free 32MB on NODE_2;
            acquire2.release();
            // even though pending 32MB was reserving space on NODE_1 it will still use free space on NODE_2 when it got available (it has higher priority than 16MB request which came later)
            assertAcquired(acquire7);

            // release 16MB on NODE_1
            acquire1.release();
            // pending 16MB request should be unblocked now
            assertAcquired(acquire8);
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateReleaseBeforeAcquired()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first two allocations should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_1);

            // another two should block
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire3);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire4);

            // releasing a blocked one should not unblock anything
            acquire3.release();
            assertNotAcquired(acquire4);

            // releasing an acquired one should unblock one which is still blocked
            acquire2.release();
            assertEventually(() -> assertAcquired(acquire4, NODE_1));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testNoMatchingNodeAvailable()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // request a node with specific catalog (not present)
            NodeAllocator.NodeLease acquireNoMatching = nodeAllocator.acquire(REQ_CATALOG_1, DataSize.of(64, GIGABYTE), false);
            assertNotAcquired(acquireNoMatching);
            ticker.increment(59, TimeUnit.SECONDS); // still below timeout
            nodeAllocatorService.processPendingAcquires();
            assertNotAcquired(acquireNoMatching);
            ticker.increment(2, TimeUnit.SECONDS); // past 1 minute timeout
            nodeAllocatorService.processPendingAcquires();
            assertThatThrownBy(() -> Futures.getUnchecked(acquireNoMatching.getNode()))
                    .hasMessageContaining("No nodes available to run query");

            // add node with specific catalog
            nodeManager.addNodes(NODE_2);

            // we should be able to acquire the node now
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_CATALOG_1, DataSize.of(64, GIGABYTE), false);
            assertAcquired(acquire1, NODE_2);

            // acquiring one more should block (only one acquire fits a node as we request 64GB)
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_CATALOG_1, DataSize.of(64, GIGABYTE), false);
            assertNotAcquired(acquire2);

            // remove node with catalog
            nodeManager.removeNode(NODE_2);
            // TODO: make BinPackingNodeAllocatorService react on node removed automatically
            nodeAllocatorService.processPendingAcquires();
            ticker.increment(61, TimeUnit.SECONDS); // wait past the timeout
            nodeAllocatorService.processPendingAcquires();

            // pending acquire2 should be completed now but with an exception
            assertEventually(() -> {
                assertFalse(acquire2.getNode().isCancelled());
                assertTrue(acquire2.getNode().isDone());
                assertThatThrownBy(() -> getFutureValue(acquire2.getNode()))
                        .hasMessage("No nodes available to run query");
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testNoMatchingNodeAvailableTimeoutReset()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // request a node with specific catalog (not present)
            NodeAllocator.NodeLease acquireNoMatching1 = nodeAllocator.acquire(REQ_CATALOG_1, DataSize.of(64, GIGABYTE), false);
            NodeAllocator.NodeLease acquireNoMatching2 = nodeAllocator.acquire(REQ_CATALOG_1, DataSize.of(64, GIGABYTE), false);
            assertNotAcquired(acquireNoMatching1);
            assertNotAcquired(acquireNoMatching2);

            // wait for a while and add a node
            ticker.increment(30, TimeUnit.SECONDS); // past 1 minute timeout
            nodeManager.addNodes(NODE_2);

            // only one of the leases should be completed but timeout counter for period where no nodes
            // are available should be reset for the other one
            nodeAllocatorService.processPendingAcquires();
            assertThat(acquireNoMatching1.getNode().isDone() != acquireNoMatching2.getNode().isDone())
                    .describedAs("exactly one of pending acquires should be completed")
                    .isTrue();

            NodeAllocator.NodeLease theAcquireLease = acquireNoMatching1.getNode().isDone() ? acquireNoMatching1 : acquireNoMatching2;
            NodeAllocator.NodeLease theNotAcquireLease = acquireNoMatching1.getNode().isDone() ? acquireNoMatching2 : acquireNoMatching1;

            // remove the node - we are again in situation where no matching nodes exist in cluster
            nodeManager.removeNode(NODE_2);

            // sleep for a while before releasing lease, as background processPendingAcquires may be still running with old snapshot
            // containing NODE_2, and theNotAcquireLease could be fulfilled when theAcquireLease is released
            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            theAcquireLease.release();
            nodeAllocatorService.processPendingAcquires();
            assertNotAcquired(theNotAcquireLease);

            ticker.increment(59, TimeUnit.SECONDS); // still below 1m timeout as the reset happened in previous step
            nodeAllocatorService.processPendingAcquires();
            assertNotAcquired(theNotAcquireLease);

            ticker.increment(2, TimeUnit.SECONDS);
            nodeAllocatorService.processPendingAcquires();
            assertThatThrownBy(() -> Futures.getUnchecked(theNotAcquireLease.getNode()))
                    .hasMessageContaining("No nodes available to run query");
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testRemoveAcquiredNode()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);

            // remove acquired node
            nodeManager.removeNode(NODE_1);

            // we should still be able to release lease for removed node
            acquire1.release();
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateNodeWithAddressRequirements()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);

        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NODE_2, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_2);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NODE_2, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NODE_2, DataSize.of(32, GIGABYTE), false);
            // no more place on NODE_2
            assertNotAcquired(acquire3);

            // requests for other node are still good
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NODE_1, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire4, NODE_1);

            // release some space on NODE_2
            acquire1.release();
            // pending acquisition should be unblocked
            assertEventually(() -> assertAcquired(acquire3));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateNotEnoughRuntimeMemory()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first allocation is fine
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));

            // bump memory usage on NODE_1
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(33, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(33, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // second allocation of 32GB should go to another node
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);
            acquire2.attachTaskId(taskId(2));

            // third allocation of 32GB should also use NODE_2 as there is not enough runtime memory on NODE_1
            // second allocation of 32GB should go to another node
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire3, NODE_2);
            acquire3.attachTaskId(taskId(3));

            // fourth allocation of 16 should fit on NODE_1
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire4, NODE_1);
            acquire4.attachTaskId(taskId(4));

            // fifth allocation of 16 should no longer fit on NODE_1. There is 16GB unreserved but only 15GB taking runtime usage into account
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertNotAcquired(acquire5);

            // even tiny allocations should not fit now
            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(REQ_NONE, DataSize.of(1, GIGABYTE), false);
            assertNotAcquired(acquire6);

            // if memory usage decreases on NODE_1 the pending 16GB allocation should complete
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(32, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(32, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();
            nodeAllocatorService.processPendingAcquires();
            assertAcquired(acquire5, NODE_1);
            acquire5.attachTaskId(taskId(5));

            //  acquire6 should still be pending
            assertNotAcquired(acquire6);
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateRuntimeMemoryDiscrepancies()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1);

        setupNodeAllocatorService(nodeManager);
        // test when global memory usage on node is greater than per task usage
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first allocation is fine
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));

            // bump memory usage on NODE_1; per-task usage is kept small
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(33, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(4, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // global (greater) memory usage should take precedence
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire2);
        }

        setupNodeAllocatorService(nodeManager);
        // test when global memory usage on node is smaller than per task usage
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first allocation is fine
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));

            // bump memory usage on NODE_1; per-task usage is 33GB and global is 4GB
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(4, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(33, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // per-task (greater) memory usage should take precedence
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire2);
        }

        setupNodeAllocatorService(nodeManager);
        // test when per-task memory usage not present at all
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // first allocation is fine
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));

            // bump memory usage on NODE_1; per-task usage is 33GB and global is 4GB
            updateWorkerUsedMemory(NODE_1, DataSize.of(33, GIGABYTE), ImmutableMap.of());
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // global memory usage should be used (not per-task usage)
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire2);
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testSpaceReservedOnPrimaryNodeIfNoNodeWithEnoughRuntimeMemoryAvailable()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        // test when global memory usage on node is greater than per task usage
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // reserve 32GB on NODE_1 and 16GB on NODE_2
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(16, GIGABYTE), false);
            assertAcquired(acquire2, NODE_2);
            acquire2.attachTaskId(taskId(2));

            // make actual usage on NODE_2 greater than on NODE_1
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(40, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(40, GIGABYTE)));
            updateWorkerUsedMemory(NODE_2,
                    DataSize.of(41, GIGABYTE),
                    ImmutableMap.of(taskId(2), DataSize.of(41, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // try to allocate 32GB task
            // it will not fit on neither of nodes. space should be reserved on NODE_2 as it has more memory available
            // when you do not take runtime memory into account
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire3);

            // to check that is the case try to allocate 20GB; NODE_1 should be picked
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(20, GIGABYTE), false);
            assertAcquired(acquire4, NODE_1);
            acquire4.attachTaskId(taskId(2));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateWithRuntimeMemoryEstimateOverhead()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1);
        setupNodeAllocatorService(nodeManager, DataSize.of(4, GIGABYTE));

        // test when global memory usage on node is greater than per task usage
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // allocated 32GB
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquire1, NODE_1);
            acquire1.attachTaskId(taskId(1));

            // set runtime usage of task1 to 30GB
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(30, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(30, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // including overhead node runtime usage is 30+4 = 34GB so another 32GB task will not fit
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquire2);

            // decrease runtime usage to 28GB
            // set runtime usage of task1 to 30GB
            updateWorkerUsedMemory(NODE_1,
                    DataSize.of(28, GIGABYTE),
                    ImmutableMap.of(taskId(1), DataSize.of(28, GIGABYTE)));
            nodeAllocatorService.refreshNodePoolMemoryInfos();

            // now pending acquire should be fulfilled
            nodeAllocatorService.processPendingAcquires();
            assertAcquired(acquire2, NODE_1);
        }
    }

    @Test
    public void testStressAcquireRelease()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1);
        setupNodeAllocatorService(nodeManager, DataSize.of(4, GIGABYTE));

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            for (int i = 0; i < 10_000_000; ++i) {
                NodeAllocator.NodeLease lease = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
                lease.release();
            }
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateSpeculative()
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(NODE_1, NODE_2);
        setupNodeAllocatorService(nodeManager);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION)) {
            // allocate two speculative tasks
            NodeAllocator.NodeLease acquireSpeculative1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(64, GIGABYTE), true);
            assertAcquired(acquireSpeculative1, NODE_1);
            NodeAllocator.NodeLease acquireSpeculative2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), true);
            assertAcquired(acquireSpeculative2, NODE_2);

            // non-speculative tasks should still get node
            NodeAllocator.NodeLease acquireNonSpeculative1 = nodeAllocator.acquire(REQ_NONE, DataSize.of(64, GIGABYTE), false);
            assertAcquired(acquireNonSpeculative1, NODE_2);
            NodeAllocator.NodeLease acquireNonSpeculative2 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquireNonSpeculative2, NODE_1);

            // new speculative task will not fit (even tiny one)
            NodeAllocator.NodeLease acquireSpeculative3 = nodeAllocator.acquire(REQ_NONE, DataSize.of(1, GIGABYTE), true);
            assertNotAcquired(acquireSpeculative3);

            // if you switch it to non-speculative it will schedule
            acquireSpeculative3.setSpeculative(false);
            assertAcquired(acquireSpeculative3, NODE_1);

            // release all speculative tasks
            acquireSpeculative1.release();
            acquireSpeculative2.release();
            acquireSpeculative3.release();

            // we have 32G free on NODE_1 now
            NodeAllocator.NodeLease acquireNonSpeculative4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertAcquired(acquireNonSpeculative4, NODE_1);

            // no place for speculative task
            NodeAllocator.NodeLease acquireSpeculative4 = nodeAllocator.acquire(REQ_NONE, DataSize.of(1, GIGABYTE), true);
            assertNotAcquired(acquireSpeculative4);

            // no place for another non-speculative task
            NodeAllocator.NodeLease acquireNonSpeculative5 = nodeAllocator.acquire(REQ_NONE, DataSize.of(32, GIGABYTE), false);
            assertNotAcquired(acquireNonSpeculative5);

            // release acquireNonSpeculative4 - a non-speculative task should be scheduled before speculative one
            acquireNonSpeculative4.release();
            assertAcquired(acquireNonSpeculative5);
            assertNotAcquired(acquireSpeculative4);

            // on subsequent release speculative task will get node
            acquireNonSpeculative5.release();
            assertAcquired(acquireSpeculative4);
        }
    }

    private TaskId taskId(int partition)
    {
        return new TaskId(new StageId("test_query", 0), partition, 0);
    }

    private void assertAcquired(NodeAllocator.NodeLease lease, InternalNode node)
    {
        assertAcquired(lease, Optional.of(node));
    }

    private void assertAcquired(NodeAllocator.NodeLease lease)
    {
        assertAcquired(lease, Optional.empty());
    }

    private void assertAcquired(NodeAllocator.NodeLease lease, Optional<InternalNode> expectedNode)
    {
        assertEventually(() -> {
            assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
            assertTrue(lease.getNode().isDone(), "node lease not acquired");
            if (expectedNode.isPresent()) {
                assertEquals(lease.getNode().get(), expectedNode.get());
            }
        });
    }

    private void assertNotAcquired(NodeAllocator.NodeLease lease)
    {
        assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
        assertFalse(lease.getNode().isDone(), "node lease acquired");
        // enforce pending acquires processing and check again
        nodeAllocatorService.processPendingAcquires();
        assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
        assertFalse(lease.getNode().isDone(), "node lease acquired");
    }

    private static void assertEventually(ThrowingRunnable assertion)
    {
        Assert.assertEventually(
                new io.airlift.units.Duration(TEST_TIMEOUT, TimeUnit.MILLISECONDS),
                new io.airlift.units.Duration(10, TimeUnit.MILLISECONDS),
                () -> {
                    try {
                        assertion.run();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    interface ThrowingRunnable
    {
        void run()
                throws Exception;
    }
}
