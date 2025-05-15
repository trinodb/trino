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
package io.trino.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.client.NodeVersion;
import io.trino.execution.scheduler.NetworkLocation;
import io.trino.execution.scheduler.NetworkTopology;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.NodeSelector;
import io.trino.execution.scheduler.NodeSelectorFactory;
import io.trino.execution.scheduler.SplitPlacementResult;
import io.trino.execution.scheduler.TopologyAwareNodeSelectorConfig;
import io.trino.execution.scheduler.TopologyAwareNodeSelectorFactory;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSession;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestNodeScheduler
{
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeSchedulerConfig nodeSchedulerConfig;
    private NodeScheduler nodeScheduler;
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;
    private Session session;

    @BeforeEach
    public void setUp()
    {
        session = TestingSession.testSessionBuilder().build();
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setMinPendingSplitsPerTask(10)
                .setMaxAdjustedPendingSplitsWeightPerTask(100)
                .setIncludeCoordinator(false);

        nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap));
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));
        remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));

        finalizerService.start();
    }

    private void setUpNodes()
    {
        nodeManager.addNodes(
                new InternalNode("other1", URI.create("http://10.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false));
    }

    @AfterEach
    public void tearDown()
    {
        remoteTaskExecutor.shutdown();
        remoteTaskExecutor = null;
        remoteTaskScheduledExecutor.shutdown();
        remoteTaskScheduledExecutor = null;
        nodeSchedulerConfig = null;
        nodeScheduler = null;
        nodeSelector = null;
        finalizerService.destroy();
        finalizerService = null;
    }

    // Test exception throw when no nodes available to schedule
    @Test
    public void testAssignmentWhenNoNodes()
    {
        assertTrinoExceptionThrownBy(() -> computeSingleAssignment(nodeSelector, new Split(TEST_CATALOG_HANDLE, new TestSplitRemote())))
                .hasErrorCode(NO_NODES_AVAILABLE)
                .hasMessageMatching("No nodes available to run query");
    }

    @Test
    public void testScheduleLocal()
    {
        setUpNodes();
        Split split = new Split(TEST_CATALOG_HANDLE, new TestSplitLocallyAccessible());
        Map.Entry<InternalNode, Split> assignment = getOnlyElement(computeSingleAssignment(nodeSelector, split).entries());
        assertThat(assignment.getKey().getHostAndPort()).isEqualTo(split.getAddresses().get(0));
        assertThat(assignment.getValue()).isEqualTo(split);
    }

    @Test
    @Timeout(60)
    public void testTopologyAwareScheduling()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        InternalNodeManager nodeManager = new InMemoryNodeManager(
                new InternalNode("node1", URI.create("http://host1.rack1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("node2", URI.create("http://host2.rack1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("node3", URI.create("http://host3.rack2:13"), NodeVersion.UNKNOWN, false));

        // contents of taskMap indicate the node-task map for the current stage
        Map<InternalNode, RemoteTask> taskMap = new HashMap<>();
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(25)
                .setIncludeCoordinator(false)
                .setMinPendingSplitsPerTask(20);

        TestNetworkTopology topology = new TestNetworkTopology();
        NodeSelectorFactory nodeSelectorFactory = new TopologyAwareNodeSelectorFactory(topology, nodeManager, nodeSchedulerConfig, nodeTaskMap, getNetworkTopologyConfig());
        NodeScheduler nodeScheduler = new NodeScheduler(nodeSelectorFactory);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));

        // Fill up the nodes with non-local data
        ImmutableSet.Builder<Split> nonRackLocalBuilder = ImmutableSet.builder();
        for (int i = 0; i < (25 + 11) * 3; i++) {
            nonRackLocalBuilder.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromParts("data.other_rack", 1))));
        }
        Set<Split> nonRackLocalSplits = nonRackLocalBuilder.build();
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments.keySet()) {
            TaskId taskId = new TaskId(new StageId("test", 1), task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(25);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        // Continue assigning to fill up part of the queue
        nonRackLocalSplits = Sets.difference(nonRackLocalSplits, new HashSet<>(assignments.values()));
        assignments = nodeSelector.computeAssignments(nonRackLocalSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        nonRackLocalSplits = Sets.difference(nonRackLocalSplits, new HashSet<>(assignments.values()));
        // Check that 3 of the splits were rejected, since they're non-local
        assertThat(nonRackLocalSplits).hasSize(3);

        // Assign rack-local splits
        ImmutableSet.Builder<Split> rackLocalSplits = ImmutableSet.builder();
        HostAddress dataHost1 = HostAddress.fromParts("data.rack1", 1);
        HostAddress dataHost2 = HostAddress.fromParts("data.rack2", 1);
        for (int i = 0; i < 6 * 2; i++) {
            rackLocalSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(dataHost1)));
        }
        for (int i = 0; i < 6; i++) {
            rackLocalSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(dataHost2)));
        }
        assignments = nodeSelector.computeAssignments(rackLocalSplits.build(), ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        Set<Split> unassigned = Sets.difference(rackLocalSplits.build(), new HashSet<>(assignments.values()));
        // Compute the assignments a second time to account for the fact that some splits may not have been assigned due to asynchronous
        // loading of the NetworkLocationCache
        assignments = nodeSelector.computeAssignments(unassigned, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                    .build());
        }
        unassigned = Sets.difference(unassigned, new HashSet<>(assignments.values()));
        assertThat(unassigned).hasSize(3);
        int rack1 = 0;
        int rack2 = 0;
        for (Split split : unassigned) {
            String rack = topology.locate(split.getAddresses().get(0)).getSegments().get(0);
            switch (rack) {
                case "rack1":
                    rack1++;
                    break;
                case "rack2":
                    rack2++;
                    break;
                default:
                    throw new AssertionError("Unexpected rack: " + rack);
            }
        }
        assertThat(rack1).isEqualTo(2);
        assertThat(rack2).isEqualTo(1);

        // Assign local splits
        ImmutableSet.Builder<Split> localSplits = ImmutableSet.builder();
        localSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromParts("host1.rack1", 1))));
        localSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromParts("host2.rack1", 1))));
        localSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromParts("host3.rack2", 1))));
        assignments = nodeSelector.computeAssignments(localSplits.build(), ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments.size()).isEqualTo(3);
        assertThat(assignments.keySet().size()).isEqualTo(3);
    }

    @Test
    public void testScheduleRemote()
    {
        setUpNodes();
        Multimap<InternalNode, Split> assignments = computeSingleAssignment(nodeSelector, new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        assertThat(assignments.size()).isEqualTo(1);
    }

    @Test
    public void testBasicAssignment()
    {
        setUpNodes();
        Set<InternalNode> activeCatalogNodes = nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE).stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableSet());

        // One split for each node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < activeCatalogNodes.size(); i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments.entries()).hasSize(assignments.size());
        for (InternalNode node : activeCatalogNodes) {
            assertThat(assignments.keySet()).contains(node);
        }
    }

    @Test
    public void testMaxSplitsPerNode()
    {
        setUpNodes();
        InternalNode newNode = new InternalNode("other4", URI.create("http://10.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            initialSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }

        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        // Max out number of splits on node
        TaskId taskId1 = new TaskId(new StageId("test", 1), 1, 0);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId1));
        nodeTaskMap.addTask(newNode, remoteTask1);

        TaskId taskId2 = new TaskId(new StageId("test", 1), 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(taskId2, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId2));
        nodeTaskMap.addTask(newNode, remoteTask2);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();

        // no split should be assigned to the newNode, as it already has maxNodeSplits assigned to it
        assertThat(assignments.keySet().contains(newNode)).isFalse();

        remoteTask1.abort();
        remoteTask2.abort();

        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(newNode)).isEqualTo(PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testBasicAssignmentMaxUnacknowledgedSplitsPerTask()
    {
        // Use non-default max unacknowledged splits per task
        nodeSelector = nodeScheduler.createNodeSelector(sessionWithMaxUnacknowledgedSplitsPerTask(1), Optional.of(TEST_CATALOG_HANDLE));
        setUpNodes();
        // One split for each node, and one extra split that can't be placed
        Set<InternalNode> activeCatalogNodes = nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE).stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableSet());
        int splitCount = activeCatalogNodes.size() + 1;
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments.entries()).hasSize(activeCatalogNodes.size());
        for (InternalNode node : activeCatalogNodes) {
            assertThat(assignments.keySet()).contains(node);
        }
    }

    @Test
    public void testMaxSplitsPerNodePerTask()
    {
        setUpNodes();
        InternalNode newNode = new InternalNode("other4", URI.create("http://10.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(newNode);

        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < 20; i++) {
            initialSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }

        List<RemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        for (InternalNode node : nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE)) {
            // Max out number of splits on node
            TaskId taskId = new TaskId(new StageId("test", 1), 1, 0);
            RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            tasks.add(remoteTask);
        }

        TaskId taskId = new TaskId(new StageId("test", 1), 2, 0);
        RemoteTask newRemoteTask = remoteTaskFactory.createTableScanTask(taskId, newNode, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(newNode, taskId));
        // Max out pending splits on new node
        taskMap.put(newNode, newRemoteTask);
        nodeTaskMap.addTask(newNode, newRemoteTask);
        tasks.add(newRemoteTask);

        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }
        Multimap<InternalNode, Split> assignments = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();

        // no split should be assigned to the newNode, as it already has
        // maxSplitsPerNode + maxSplitsPerNodePerTask assigned to it
        assertThat(assignments.keySet()).hasSize(3); // Splits should be scheduled on the other three nodes
        assertThat(assignments.keySet().contains(newNode)).isFalse(); // No splits scheduled on the maxed out node

        for (RemoteTask task : tasks) {
            task.abort();
        }
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(newNode)).isEqualTo(PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testTaskCompletion()
            throws Exception
    {
        setUpNodes();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE), 0);
        TaskId taskId = new TaskId(new StageId("test", 1), 1, 0);
        RemoteTask remoteTask = remoteTaskFactory.createTableScanTask(
                taskId,
                chosenNode,
                ImmutableList.of(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId));
        nodeTaskMap.addTask(chosenNode, remoteTask);
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode).getCount()).isEqualTo(1);
        remoteTask.abort();
        MILLISECONDS.sleep(100); // Sleep until cache expires
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode)).isEqualTo(PartitionedSplitsInfo.forZeroSplits());

        remoteTask.abort();
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode)).isEqualTo(PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testSplitCount()
    {
        setUpNodes();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        InternalNode chosenNode = Iterables.get(nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE), 0);

        TaskId taskId1 = new TaskId(new StageId("test", 1), 1, 0);
        RemoteTask remoteTask1 = remoteTaskFactory.createTableScanTask(taskId1,
                chosenNode,
                ImmutableList.of(
                        new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()),
                        new Split(TEST_CATALOG_HANDLE, new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId1));

        TaskId taskId2 = new TaskId(new StageId("test", 1), 2, 0);
        RemoteTask remoteTask2 = remoteTaskFactory.createTableScanTask(
                taskId2,
                chosenNode,
                ImmutableList.of(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote())),
                nodeTaskMap.createPartitionedSplitCountTracker(chosenNode, taskId2));

        nodeTaskMap.addTask(chosenNode, remoteTask1);
        nodeTaskMap.addTask(chosenNode, remoteTask2);
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode).getCount()).isEqualTo(3);

        remoteTask1.abort();
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode).getCount()).isEqualTo(1);
        remoteTask2.abort();
        assertThat(nodeTaskMap.getPartitionedSplitsOnNode(chosenNode)).isEqualTo(PartitionedSplitsInfo.forZeroSplits());
    }

    @Test
    public void testOptimizedLocalScheduling()
    {
        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:11"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(node1);
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(node2);

        Set<Split> splits = new LinkedHashSet<>();
        // 20 splits with node1 as local node to be assigned in the first iteration of computeAssignments
        for (int i = 0; i < 20; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitLocal()));
        }
        // computeAssignments just returns a mapping of nodes with splits to be assigned, it does not assign splits
        Multimap<InternalNode, Split> assignments1 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        // Check that all 20 splits are being assigned to node1 as optimized-local-scheduling is enabled
        assertThat(assignments1.size()).isEqualTo(20);
        assertThat(assignments1.keySet().size()).isEqualTo(1);
        assertThat(assignments1.keySet()).contains(node1);

        // 19 splits with node2 as local node to be assigned in the first iteration of computeAssignments
        for (int i = 0; i < 19; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromString("10.0.0.1:12"))));
        }
        Multimap<InternalNode, Split> assignments2 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        // Check that all 39 splits are being assigned (20 splits assigned to node1 and 19 splits assigned to node2)
        assertThat(assignments2.size()).isEqualTo(39);
        assertThat(assignments2.keySet().size()).isEqualTo(2);
        assertThat(assignments2.keySet()).contains(node1);
        assertThat(assignments2.keySet()).contains(node2);

        long node1Splits = assignments2.values().stream()
                .map(Split::getConnectorSplit)
                .filter(TestSplitLocal.class::isInstance)
                .count();
        assertThat(node1Splits).isEqualTo(20);

        long node2Splits = assignments2.values().stream()
                .map(Split::getConnectorSplit)
                .filter(TestSplitRemote.class::isInstance)
                .count();
        assertThat(node2Splits).isEqualTo(19);

        // 1 split with node1 as local node
        splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitLocal()));
        // 1 split with node2 as local node
        splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromString("10.0.0.1:12"))));
        //splits now contains 41 splits : 21 with node1 as local node and 20 with node2 as local node
        Multimap<InternalNode, Split> assignments3 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        // Check that only 40 splits are being assigned as there is a single task
        assertThat(assignments3.size()).isEqualTo(40);
        assertThat(assignments3.keySet().size()).isEqualTo(2);
        assertThat(assignments3.keySet()).contains(node1);
        assertThat(assignments3.keySet()).contains(node2);

        // The first 20 splits have node1 as local, the next 19 have node2 as local, the 40th split has node1 as local and the 41st has node2 as local
        // If optimized-local-scheduling is disabled, the 41st split will be unassigned (the last slot in node2 will be taken up by the 40th split with node1 as local)
        // optimized-local-scheduling ensures that all splits that can be assigned locally will be assigned first
        node1Splits = assignments3.values().stream()
                .map(Split::getConnectorSplit)
                .filter(TestSplitLocal.class::isInstance)
                .count();
        assertThat(node1Splits).isEqualTo(20);

        node2Splits = assignments3.values().stream()
                .map(Split::getConnectorSplit)
                .filter(TestSplitRemote.class::isInstance)
                .count();
        assertThat(node2Splits).isEqualTo(20);
    }

    @Test
    public void testMaxUnacknowledgedSplitsPerTask()
    {
        int maxUnacknowledgedSplitsPerTask = 5;
        nodeSelector = nodeScheduler.createNodeSelector(sessionWithMaxUnacknowledgedSplitsPerTask(maxUnacknowledgedSplitsPerTask), Optional.of(TEST_CATALOG_HANDLE));
        setUpNodes();
        ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
        for (int i = 0; i < maxUnacknowledgedSplitsPerTask; i++) {
            initialSplits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }

        List<InternalNode> nodes = nodeManager.getActiveCatalogNodes(TEST_CATALOG_HANDLE).stream()
                .filter(node -> !node.isCoordinator())
                .collect(toImmutableList());
        List<MockRemoteTaskFactory.MockRemoteTask> tasks = new ArrayList<>();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int counter = 1;
        for (InternalNode node : nodes) {
            // Max out number of unacknowledged splits on each task
            TaskId taskId = new TaskId(new StageId("test", 1), counter, 0);
            counter++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            nodeTaskMap.addTask(node, remoteTask);
            remoteTask.setMaxUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask);
            remoteTask.setUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask);
            tasks.add(remoteTask);
        }

        // One split per node
        Set<Split> splits = new HashSet<>();
        for (int i = 0; i < nodes.size(); i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, new TestSplitRemote()));
        }
        SplitPlacementResult splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        // No splits should have been placed, max unacknowledged was already reached
        assertThat(splitPlacements.getAssignments().size()).isEqualTo(0);

        // Unblock one task
        MockRemoteTaskFactory.MockRemoteTask taskOne = tasks.get(0);
        taskOne.finishSplits(1);
        taskOne.setUnacknowledgedSplits(taskOne.getUnacknowledgedPartitionedSplitCount() - 1);
        assertThat(splitPlacements.getBlocked().isDone()).isTrue();

        // Attempt to schedule again, only the node with the unblocked task should be chosen
        splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        assertThat(splitPlacements.getAssignments().size()).isEqualTo(1);
        assertThat(splitPlacements.getAssignments().keySet()).contains(nodes.get(0));

        // Make the first node appear to have no splits, unacknowledged splits alone should force the splits to be spread across nodes
        taskOne.clearSplits();
        // Give all tasks with room for 1 unacknowledged split
        tasks.forEach(task -> task.setUnacknowledgedSplits(maxUnacknowledgedSplitsPerTask - 1));

        splitPlacements = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(tasks));
        // One split placed on each node
        assertThat(splitPlacements.getAssignments().size()).isEqualTo(nodes.size());
        assertThat(splitPlacements.getAssignments().keySet().containsAll(nodes)).isTrue();
    }

    @Test
    @Timeout(60)
    public void testTopologyAwareFailover()
    {
        nodeManager = new InMemoryNodeManager(
                new InternalNode("node1", URI.create("http://host1.rack1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("node2", URI.create("http://host2.rack1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("node3", URI.create("http://host3.rack2:13"), NodeVersion.UNKNOWN, false));
        NodeSelectorFactory nodeSelectorFactory = new TopologyAwareNodeSelectorFactory(
                new TestNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap, getNetworkTopologyConfig());
        NodeScheduler nodeScheduler = new NodeScheduler(nodeSelectorFactory);
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));

        Split rigidSplit = new Split(TEST_CATALOG_HANDLE, new TestSplitLocal(HostAddress.fromString("host99.rack1:11")));
        assertThatThrownBy(() -> computeSingleAssignment(nodeSelector, rigidSplit)).hasMessageContaining("No nodes available");

        Split flexibleSplit = new Split(TEST_CATALOG_HANDLE, new TestSplitRemote(HostAddress.fromString("host99.rack1:11")));
        org.assertj.guava.api.Assertions.assertThat(computeSingleAssignment(nodeSelector, flexibleSplit)).containsValues(flexibleSplit);
    }

    private Multimap<InternalNode, Split> computeSingleAssignment(NodeSelector nodeSelector, Split split)
    {
        return nodeSelector.computeAssignments(ImmutableSet.of(split), ImmutableList.copyOf(taskMap.values())).getAssignments();
    }

    private static Session sessionWithMaxUnacknowledgedSplitsPerTask(int maxUnacknowledgedSplitsPerTask)
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty(SystemSessionProperties.MAX_UNACKNOWLEDGED_SPLITS_PER_TASK, Integer.toString(maxUnacknowledgedSplitsPerTask))
                .build();
    }

    private static class TestSplitLocal
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(TestSplitLocal.class);

        private final HostAddress address;
        private final SplitWeight splitWeight;

        private TestSplitLocal()
        {
            this(HostAddress.fromString("10.0.0.1:11"));
        }

        private TestSplitLocal(HostAddress address)
        {
            this(address, SplitWeight.standard());
        }

        private TestSplitLocal(HostAddress address, SplitWeight splitWeight)
        {
            this.address = requireNonNull(address, "address is null");
            this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of(address);
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return splitWeight;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + address.getRetainedSizeInBytes()
                    + splitWeight.getRetainedSizeInBytes();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("address", address)
                    .toString();
        }
    }

    private static class TestSplitLocallyAccessible
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of(HostAddress.fromString("10.0.0.1:11"));
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return 0;
        }
    }

    private static class TestSplitRemote
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(TestSplitRemote.class);

        private final List<HostAddress> hosts;
        private final SplitWeight splitWeight;

        TestSplitRemote()
        {
            this(HostAddress.fromString(format("10.%s.%s.%s:%s",
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(15, 5000))));
        }

        TestSplitRemote(HostAddress host)
        {
            this(host, SplitWeight.standard());
        }

        TestSplitRemote(HostAddress host, SplitWeight splitWeight)
        {
            this.hosts = ImmutableList.of(requireNonNull(host, "host is null"));
            this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return hosts;
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return splitWeight;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(hosts, HostAddress::getRetainedSizeInBytes)
                    + splitWeight.getRetainedSizeInBytes();
        }
    }

    private static class TestNetworkTopology
            implements NetworkTopology
    {
        @Override
        public NetworkLocation locate(HostAddress address)
        {
            List<String> parts = new ArrayList<>(ImmutableList.copyOf(Splitter.on(".").split(address.getHostText())));
            Collections.reverse(parts);
            return new NetworkLocation(parts);
        }
    }

    private static TopologyAwareNodeSelectorConfig getNetworkTopologyConfig()
    {
        return new TopologyAwareNodeSelectorConfig()
                .setLocationSegmentNames(ImmutableList.of("rack", "machine"));
    }
}
