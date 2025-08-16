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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.testing.TestingTicker;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestUniformNodeSelector
{
    private static final InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
    private static final InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
    private final Set<Split> splits = new LinkedHashSet<>();
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private TestingInternalNodeManager nodeManager;
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
        nodeManager = TestingInternalNodeManager.createDefault();
        nodeManager.addNodes(node1);
        nodeManager.addNodes(node2);

        nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setMinPendingSplitsPerTask(10)
                .setMaxAdjustedPendingSplitsWeightPerTask(100)
                .setIncludeCoordinator(false);

        // contents of taskMap indicate the node-task map for the current stage
        nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(CURRENT_NODE, nodeManager, nodeSchedulerConfig, nodeTaskMap));
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(session);
        remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));

        finalizerService.start();
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

    @Test
    public void testQueueSizeAdjustmentScaleDown()
    {
        TestingTicker ticker = new TestingTicker();
        UniformNodeSelector.QueueSizeAdjuster queueSizeAdjuster = new UniformNodeSelector.QueueSizeAdjuster(10, 100, ticker);

        nodeSelector = new UniformNodeSelector(
                CURRENT_NODE,
                nodeTaskMap,
                false,
                () -> createNodeMap(),
                10,
                100,
                10,
                500,
                NodeSchedulerConfig.SplitsBalancingPolicy.STAGE,
                false,
                queueSizeAdjuster);

        for (int i = 0; i < 20; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, TestingSplit.createRemoteSplit()));
        }

        // assign splits, mark all splits running to trigger adjustment
        Multimap<InternalNode, Split> assignments1 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments1.size()).isEqualTo(2);
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments1.keySet()) {
            TaskId taskId = new TaskId(new StageId("test", 1), task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments1.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount()); // mark all task running
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        Set<Split> unassignedSplits = Sets.difference(splits, new HashSet<>(assignments1.values()));
        assertThat(unassignedSplits).hasSize(18);
        // It's possible to add new assignments because split queue was upscaled
        Multimap<InternalNode, Split> assignments2 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments2.size()).isEqualTo(2);

        // update remote tasks
        for (InternalNode node : assignments2.keySet()) {
            MockRemoteTaskFactory.MockRemoteTask remoteTask = (MockRemoteTaskFactory.MockRemoteTask) taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments2.get(node))
                    .build());
        }
        long maxPendingSplitsWeightPerTaskBeforeScaleDown = queueSizeAdjuster.getAdjustedMaxPendingSplitsWeightPerTask(node1.getNodeIdentifier());
        assertThat(20).isEqualTo(maxPendingSplitsWeightPerTaskBeforeScaleDown);
        // compute assignments called before scale down interval
        ticker.increment(999, TimeUnit.MILLISECONDS);
        Multimap<InternalNode, Split> assignments3 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments3.size()).isEqualTo(0); // no new assignments added to nodes
        assertThat(maxPendingSplitsWeightPerTaskBeforeScaleDown).isEqualTo(queueSizeAdjuster.getAdjustedMaxPendingSplitsWeightPerTask(node1.getNodeIdentifier()));
        // compute assignments called with passed scale down interval
        ticker.increment(1, TimeUnit.MILLISECONDS);
        Multimap<InternalNode, Split> assignments4 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments4.size()).isEqualTo(0); // no new assignments added to nodes
        long maxPendingSplitsWeightPerTaskAfterScaleDown = queueSizeAdjuster.getAdjustedMaxPendingSplitsWeightPerTask(node1.getNodeIdentifier());
        assertThat(13).isEqualTo(maxPendingSplitsWeightPerTaskAfterScaleDown);
    }

    @Test
    public void testQueueSizeAdjustmentAllNodes()
    {
        for (int i = 0; i < 20 * 9; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, TestingSplit.createRemoteSplit()));
        }

        // assign splits, marked all running to trigger adjustment
        Multimap<InternalNode, Split> assignments1 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments1.size()).isEqualTo(40);
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments1.keySet()) {
            TaskId taskId = new TaskId(new StageId("test", 1), task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments1.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount()); // mark all task running
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        Set<Split> unassignedSplits = Sets.difference(splits, new HashSet<>(assignments1.values()));
        assertThat(unassignedSplits).hasSize(140);

        // assign splits, mark all splits running to trigger adjustment
        Multimap<InternalNode, Split> assignments2 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments2.keySet()) {
            MockRemoteTaskFactory.MockRemoteTask remoteTask = (MockRemoteTaskFactory.MockRemoteTask) taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments2.get(node))
                    .build());
            remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount()); // mark all task running
        }
        unassignedSplits = Sets.difference(unassignedSplits, new HashSet<>(assignments2.values()));
        assertThat(unassignedSplits).hasSize(100); // 140 (unassigned splits) - (2 (queue size adjustment) * 10 (minPendingSplitsPerTask)) * 2 (nodes)

        // assign splits without setting all splits running
        Multimap<InternalNode, Split> assignments3 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments3.keySet()) {
            RemoteTask remoteTask = taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments3.get(node))
                    .build());
        }
        unassignedSplits = Sets.difference(unassignedSplits, new HashSet<>(assignments3.values()));
        assertThat(unassignedSplits).hasSize(20); // 100 (unassigned splits) - (4 (queue size adjustment) * 10 (minPendingSplitsPerTask)) * 2 (nodes)

        // compute assignments with exhausted nodes
        Multimap<InternalNode, Split> assignments4 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        unassignedSplits = Sets.difference(unassignedSplits, new HashSet<>(assignments4.values()));
        assertThat(unassignedSplits).hasSize(20); // no new split assignments, queued are more than 0
    }

    @Test
    public void testQueueSizeAdjustmentOneOfAll()
    {
        for (int i = 0; i < 20 * 9; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, TestingSplit.createRemoteSplit()));
        }

        // assign splits, mark all splits for node1 running to trigger adjustment
        Multimap<InternalNode, Split> assignments1 = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignments1.size()).isEqualTo(40);
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments1.keySet()) {
            TaskId taskId = new TaskId(new StageId("test", 1), task, 0);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments1.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            if (node.equals(node1)) {
                remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount()); // mark all task running
            }
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        Set<Split> unassignedSplits = Sets.difference(splits, new HashSet<>(assignments1.values()));
        assertThat(unassignedSplits).hasSize(140);

        // assign splits, mark all splits for node1 running to trigger adjustment
        Multimap<InternalNode, Split> assignments2 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments2.keySet()) {
            MockRemoteTaskFactory.MockRemoteTask remoteTask = (MockRemoteTaskFactory.MockRemoteTask) taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments2.get(node))
                    .build());
            if (node.equals(node1)) {
                remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount());
            }
        }
        unassignedSplits = Sets.difference(unassignedSplits, new HashSet<>(assignments2.values()));
        assertThat(unassignedSplits).hasSize(120);
        assertThat(assignments2.get(node1)).hasSize(20); // 2x max pending
        assertThat(assignments2.containsKey(node2)).isFalse();

        // assign splits, mark all splits for node1 running to trigger adjustment
        Multimap<InternalNode, Split> assignments3 = nodeSelector.computeAssignments(unassignedSplits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        for (InternalNode node : assignments3.keySet()) {
            MockRemoteTaskFactory.MockRemoteTask remoteTask = (MockRemoteTaskFactory.MockRemoteTask) taskMap.get(node);
            remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(new PlanNodeId("sourceId"), assignments3.get(node))
                    .build());
            if (node.equals(node1)) {
                remoteTask.startSplits(remoteTask.getPartitionedSplitsInfo().getCount());
            }
        }
        unassignedSplits = Sets.difference(unassignedSplits, new HashSet<>(assignments3.values()));
        assertThat(unassignedSplits).hasSize(80);
        assertThat(assignments3.get(node1)).hasSize(40); // 4x max pending
        assertThat(assignments2.containsKey(node2)).isFalse();
    }

    @Test
    public void testFailover()
    {
        // Node selector without nodeMap memoization, so removing nodes takes effect immediately:
        nodeSelector = new UniformNodeSelector(
                CURRENT_NODE,
                nodeTaskMap,
                false,
                () -> createNodeMap(),
                10,
                2000,
                1000,
                2000,
                NodeSchedulerConfig.SplitsBalancingPolicy.STAGE,
                true,
                new UniformNodeSelector.QueueSizeAdjuster(1000, 10000, new TestingTicker()));

        Split rigidSplit = new Split(TEST_CATALOG_HANDLE, new TestingSplit(false, ImmutableList.of(node1.getHostAndPort())));
        splits.add(rigidSplit);
        Split flexibleSplit = new Split(TEST_CATALOG_HANDLE, new TestingSplit(true, ImmutableList.of(node1.getHostAndPort())));
        splits.add(flexibleSplit);

        // Both nodes alive, but both splits prefer node 1.
        Multimap<InternalNode, Split> assignmentsNode1Alive = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        ArrayListMultimap<InternalNode, Split> expected = ArrayListMultimap.create();
        expected.putAll(node1, splits);
        org.assertj.guava.api.Assertions.assertThat(assignmentsNode1Alive).hasSameEntriesAs(expected);

        nodeManager.removeNode(node1);
        // Now the flexible split can fail over to node2, while the rigid split cannot.
        assertThatThrownBy(() -> nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values()))).hasMessage("No nodes available to run query");
        Multimap<InternalNode, Split> assignmentsNode1Dead =
                nodeSelector.computeAssignments(ImmutableSet.of(flexibleSplit), ImmutableList.copyOf(taskMap.values())).getAssignments();
        expected = ArrayListMultimap.create();
        expected.put(node2, flexibleSplit);
        org.assertj.guava.api.Assertions.assertThat(assignmentsNode1Dead).hasSameEntriesAs(expected);
    }

    private NodeMap createNodeMap()
    {
        Set<InternalNode> nodes = nodeManager.getNodes(ACTIVE);

        Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        ImmutableSetMultimap.Builder<HostAddress, InternalNode> byHostAndPort = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<InetAddress, InternalNode> byHost = ImmutableSetMultimap.builder();
        for (InternalNode node : nodes) {
            try {
                byHostAndPort.put(node.getHostAndPort(), node);
                byHost.put(node.getInternalAddress(), node);
            }
            catch (UnknownHostException e) {
                // pass
            }
        }

        return new NodeMap(byHostAndPort.build(), byHost.build(), ImmutableSetMultimap.of(), coordinatorNodeIds);
    }
}
