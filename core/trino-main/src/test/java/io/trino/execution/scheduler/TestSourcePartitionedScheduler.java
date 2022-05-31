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
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.MockRemoteTaskFactory.MockRemoteTask;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.PartitionedSplitsInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TableInfo;
import io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import io.trino.split.ConnectorAwareSplitSource;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy.NODE;
import static io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy.STAGE;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.trino.execution.scheduler.StageExecution.State.PLANNED;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULING;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSourcePartitionedScheduler
{
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");
    private static final CatalogName CONNECTOR_ID = TEST_TABLE_HANDLE.getCatalogName();
    private static final QueryId QUERY_ID = new QueryId("query");
    private static final DynamicFilterId DYNAMIC_FILTER_ID = new DynamicFilterId("filter1");

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();
    private final Metadata metadata = createTestMetadataManager();
    private final FunctionManager functionManager = createTestingFunctionManager();
    private final TypeOperators typeOperators = new TypeOperators();
    private final Session session = TestingSession.testSessionBuilder().build();

    public TestSourcePartitionedScheduler()
    {
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
    }

    @BeforeClass
    public void setUp()
    {
        finalizerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        queryExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        finalizerService.destroy();
    }

    @Test
    public void testScheduleNoSplits()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(0, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1, STAGE);

        ScheduleResult scheduleResult = scheduler.schedule();

        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEffectivelyFinished(scheduleResult, scheduler);

        stage.abort();
    }

    @Test
    public void testScheduleSplitsOneAtATime()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1, STAGE);

        for (int i = 0; i < 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // only finishes when last split is fetched
            if (i == 59) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i < 3 ? 1 : 0);
            assertEquals(stage.getAllTasks().size(), i < 3 ? i + 1 : 3);

            assertPartitionedSplitCount(stage, min(i + 1, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBatched()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 7, STAGE);

        for (int i = 0; i <= (60 / 7); i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == (60 / 7)) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i == 0 ? 3 : 0);
            assertEquals(stage.getAllTasks().size(), 3);

            assertPartitionedSplitCount(stage, min((i + 1) * 7, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBlock()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(80, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1, STAGE);

        // schedule first 60 splits, which will cause the scheduler to block
        for (int i = 0; i <= 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            assertFalse(scheduleResult.isFinished());

            // blocks at 20 per node
            assertEquals(scheduleResult.getBlocked().isDone(), i != 60);

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i < 3 ? 1 : 0);
            assertEquals(stage.getAllTasks().size(), i < 3 ? i + 1 : 3);

            assertPartitionedSplitCount(stage, min(i + 1, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        // todo rewrite MockRemoteTask to fire a tate transition when splits are cleared, and then validate blocked future completes

        // drop the 20 splits from one node
        ((MockRemoteTask) stage.getAllTasks().get(0)).clearSplits();

        // schedule remaining 20 splits
        for (int i = 0; i < 20; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == 19) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // does not block again
            assertTrue(scheduleResult.getBlocked().isDone());

            // no additional tasks will be created
            assertEquals(scheduleResult.getNewTasks().size(), 0);
            assertEquals(stage.getAllTasks().size(), 3);

            // we dropped 20 splits so start at 40 and count to 60
            assertPartitionedSplitCount(stage, min(i + 41, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSlowSplitSource()
    {
        QueuedSplitSource queuedSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(queuedSplitSource, stage, nodeManager, nodeTaskMap, 1, STAGE);

        // schedule with no splits - will block
        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 0);
        assertEquals(stage.getAllTasks().size(), 0);

        queuedSplitSource.addSplits(1);
        assertTrue(scheduleResult.getBlocked().isDone());
    }

    @Test
    public void testNoNodes()
    {
        assertTrinoExceptionThrownBy(() -> {
            NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap));

            PlanFragment plan = createFragment();
            StageExecution stage = createStageExecution(plan, nodeTaskMap);

            StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                    stage,
                    TABLE_SCAN_NODE_ID,
                    new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(20, TestingSplit::createRemoteSplit)),
                    new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(CONNECTOR_ID)), stage::getAllTasks),
                    2,
                    new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                    new TableExecuteContextManager(),
                    () -> false);
            scheduler.schedule();
        }).hasErrorCode(NO_NODES_AVAILABLE);
    }

    @Test
    public void testWorkerBalancedSplitAssignment()
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        PlanFragment firstPlan = createFragment();
        StageExecution firstStage = createStageExecution(firstPlan, nodeTaskMap);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(createFixedSplitSource(15, TestingSplit::createRemoteSplit), firstStage, nodeManager, nodeTaskMap, 200, NODE);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 5);
        }

        // Add new node
        InternalNode additionalNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Schedule 5 splits in another query. Since the new node does not have any splits, all 5 splits are assigned to the new node
        PlanFragment secondPlan = createFragment();
        StageExecution secondStage = createStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(createFixedSplitSource(5, TestingSplit::createRemoteSplit), secondStage, nodeManager, nodeTaskMap, 200, NODE);

        scheduleResult = secondScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, secondScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEquals(secondStage.getAllTasks().size(), 1);
        RemoteTask task = secondStage.getAllTasks().get(0);
        assertEquals(task.getPartitionedSplitsInfo().getCount(), 5);

        firstStage.abort();
        secondStage.abort();
    }

    @Test
    public void testStageBalancedSplitAssignment()
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        PlanFragment firstPlan = createFragment();
        StageExecution firstStage = createStageExecution(firstPlan, nodeTaskMap);
        QueuedSplitSource firstSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(firstSplitSource, firstStage, nodeManager, nodeTaskMap, 200, STAGE);
        firstSplitSource.addSplits(15);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 5);
        }

        // Add new node
        InternalNode additionalNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Schedule 5 splits in first query. Since the new node does not have any splits, all 5 splits are assigned to the new node
        firstSplitSource.addSplits(5);
        firstSplitSource.close();
        scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEquals(firstStage.getAllTasks().size(), 4);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 5);
        }

        // Add new node
        InternalNode anotherAdditionalNode = new InternalNode("other5", URI.create("http://127.0.0.1:15"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, anotherAdditionalNode);

        // Schedule 5 splits in another query. New query should be balanced across all nodes
        PlanFragment secondPlan = createFragment();
        StageExecution secondStage = createStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(createFixedSplitSource(5, TestingSplit::createRemoteSplit), secondStage, nodeManager, nodeTaskMap, 200, STAGE);

        scheduleResult = secondScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, secondScheduler);
        assertEquals(secondStage.getAllTasks().size(), 5);
        for (RemoteTask remoteTask : secondStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 1);
        }

        firstStage.abort();
        secondStage.abort();
    }

    @Test
    public void testNewTaskScheduledWhenChildStageBufferIsUnderutilized()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap, new Duration(0, SECONDS)));

        PlanFragment plan = createFragment();
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        // setting under utilized child output buffer
        StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                stage,
                TABLE_SCAN_NODE_ID,
                new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(500, TestingSplit::createRemoteSplit)),
                new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(CONNECTOR_ID)), stage::getAllTasks),
                500,
                new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                new TableExecuteContextManager(),
                () -> false);

        // the queues of 3 running nodes should be full
        ScheduleResult scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason().get(), SPLIT_QUEUES_FULL);
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(scheduleResult.getSplitsScheduled(), 300);
        for (RemoteTask remoteTask : scheduleResult.getNewTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 100);
        }

        // new node added - the pending splits should go to it since the child tasks are not blocked
        nodeManager.addNode(CONNECTOR_ID, new InternalNode("other4", URI.create("http://127.0.0.4:14"), NodeVersion.UNKNOWN, false));
        scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason().get(), SPLIT_QUEUES_FULL); // split queue is full but still the source task creation isn't blocked
        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEquals(scheduleResult.getSplitsScheduled(), 100);
    }

    @Test
    public void testNoNewTaskScheduledWhenChildStageBufferIsOverutilized()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap, new Duration(0, SECONDS)));

        PlanFragment plan = createFragment();
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        // setting over utilized child output buffer
        StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                stage,
                TABLE_SCAN_NODE_ID,
                new ConnectorAwareSplitSource(CONNECTOR_ID, createFixedSplitSource(400, TestingSplit::createRemoteSplit)),
                new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(CONNECTOR_ID)), stage::getAllTasks),
                400,
                new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                new TableExecuteContextManager(),
                () -> true);

        // the queues of 3 running nodes should be full
        ScheduleResult scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason().get(), SPLIT_QUEUES_FULL);
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(scheduleResult.getSplitsScheduled(), 300);
        for (RemoteTask remoteTask : scheduleResult.getNewTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 100);
        }

        // new node added but 1 child's output buffer is overutilized - so lockdown the tasks
        nodeManager.addNode(CONNECTOR_ID, new InternalNode("other4", URI.create("http://127.0.0.4:14"), NodeVersion.UNKNOWN, false));
        scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason().get(), SPLIT_QUEUES_FULL);
        assertEquals(scheduleResult.getNewTasks().size(), 0);
        assertEquals(scheduleResult.getSplitsScheduled(), 0);
    }

    @Test
    public void testDynamicFiltersUnblockedOnBlockedBuildSource()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap));
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig());
        dynamicFilterService.registerQuery(
                QUERY_ID,
                TEST_SESSION,
                ImmutableSet.of(DYNAMIC_FILTER_ID),
                ImmutableSet.of(DYNAMIC_FILTER_ID),
                ImmutableSet.of(DYNAMIC_FILTER_ID));
        StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                stage,
                TABLE_SCAN_NODE_ID,
                new ConnectorAwareSplitSource(CONNECTOR_ID, createBlockedSplitSource()),
                new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(CONNECTOR_ID)), stage::getAllTasks),
                2,
                dynamicFilterService,
                new TableExecuteContextManager(),
                () -> true);

        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("DF_SYMBOL1", BIGINT);
        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                QUERY_ID,
                ImmutableList.of(new DynamicFilters.Descriptor(DYNAMIC_FILTER_ID, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());

        // make sure dynamic filtering collecting task was created immediately
        assertEquals(stage.getState(), PLANNED);
        scheduler.start();
        assertEquals(stage.getAllTasks().size(), 1);
        assertEquals(stage.getState(), SCHEDULING);

        // make sure dynamic filter is initially blocked
        assertFalse(dynamicFilter.isBlocked().isDone());

        // make sure dynamic filter is unblocked due to build side source tasks being blocked
        ScheduleResult scheduleResult = scheduler.schedule();
        assertTrue(dynamicFilter.isBlocked().isDone());

        // no new probe splits should be scheduled
        assertEquals(scheduleResult.getSplitsScheduled(), 0);
    }

    private static void assertPartitionedSplitCount(StageExecution stage, int expectedPartitionedSplitCount)
    {
        assertEquals(stage.getAllTasks().stream().mapToInt(remoteTask -> remoteTask.getPartitionedSplitsInfo().getCount()).sum(), expectedPartitionedSplitCount);
    }

    private static void assertEffectivelyFinished(ScheduleResult scheduleResult, StageScheduler scheduler)
    {
        if (scheduleResult.isFinished()) {
            assertTrue(scheduleResult.getBlocked().isDone());
            return;
        }

        assertTrue(scheduleResult.getBlocked().isDone());
        ScheduleResult nextScheduleResult = scheduler.schedule();
        assertTrue(nextScheduleResult.isFinished());
        assertTrue(nextScheduleResult.getBlocked().isDone());
        assertEquals(nextScheduleResult.getNewTasks().size(), 0);
        assertEquals(nextScheduleResult.getSplitsScheduled(), 0);
    }

    private StageScheduler getSourcePartitionedScheduler(
            ConnectorSplitSource splitSource,
            StageExecution stage,
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            int splitBatchSize,
            SplitsBalancingPolicy splitsBalancingPolicy)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(20)
                .setMaxPendingSplitsPerTask(0)
                .setSplitsBalancingPolicy(splitsBalancingPolicy);
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap, new Duration(0, SECONDS)));

        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(CONNECTOR_ID)), stage::getAllTasks);
        return newSourcePartitionedSchedulerAsStageScheduler(
                stage,
                TABLE_SCAN_NODE_ID,
                new ConnectorAwareSplitSource(CONNECTOR_ID, splitSource),
                placementPolicy,
                splitBatchSize,
                new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                new TableExecuteContextManager(),
                () -> false);
    }

    private static PlanFragment createFragment()
    {
        Symbol symbol = new Symbol("column");
        Symbol buildSymbol = new Symbol("buildColumn");

        // table scan with splitCount splits
        TableScanNode tableScan = TableScanNode.newInstance(
                TABLE_SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                false,
                Optional.empty());
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScan,
                createDynamicFilterExpression(TEST_SESSION, createTestMetadataManager(), DYNAMIC_FILTER_ID, VARCHAR, symbol.toSymbolReference()));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of(buildSymbol), Optional.empty(), REPLICATE, RetryPolicy.NONE);
        return new PlanFragment(
                new PlanFragmentId("plan_id"),
                new JoinNode(new PlanNodeId("join_id"),
                        INNER,
                        filterNode,
                        remote,
                        ImmutableList.of(),
                        tableScan.getOutputSymbols(),
                        remote.getOutputSymbols(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(DYNAMIC_FILTER_ID, buildSymbol),
                        Optional.empty()),
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }

    private static ConnectorSplitSource createBlockedSplitSource()
    {
        return new ConnectorSplitSource()
        {
            @Override
            public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
            {
                return new CompletableFuture<>();
            }

            @Override
            public void close()
            {
            }

            @Override
            public boolean isFinished()
            {
                return false;
            }
        };
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(splits.build());
    }

    private StageExecution createStageExecution(PlanFragment fragment, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(QUERY_ID, 0);
        SqlStage stage = SqlStage.createSqlStage(stageId,
                fragment,
                ImmutableMap.of(TABLE_SCAN_NODE_ID, new TableInfo(new QualifiedObjectName("test", "test", "test"), TupleDomain.all())),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                new SplitSchedulerStats());
        ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> outputBuffers = ImmutableMap.builder();
        outputBuffers.put(fragment.getId(), new PartitionedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 1));
        fragment.getRemoteSourceNodes().stream()
                .flatMap(node -> node.getSourceFragmentIds().stream())
                .forEach(fragmentId -> outputBuffers.put(fragmentId, new PartitionedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 10)));
        return createPipelinedStageExecution(
                stage,
                outputBuffers.buildOrThrow(),
                TaskLifecycleListener.NO_OP,
                new NoOpFailureDetector(),
                queryExecutor,
                Optional.of(new int[] {0}),
                0);
    }

    private static class QueuedSplitSource
            implements ConnectorSplitSource
    {
        private final Supplier<ConnectorSplit> splitFactory;
        private final LinkedBlockingQueue<ConnectorSplit> queue = new LinkedBlockingQueue<>();
        private CompletableFuture<?> notEmptyFuture = new CompletableFuture<>();
        private boolean closed;

        public QueuedSplitSource(Supplier<ConnectorSplit> splitFactory)
        {
            this.splitFactory = requireNonNull(splitFactory, "splitFactory is null");
        }

        synchronized void addSplits(int count)
        {
            if (closed) {
                return;
            }
            for (int i = 0; i < count; i++) {
                queue.add(splitFactory.get());
                notEmptyFuture.complete(null);
            }
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            checkArgument(partitionHandle.equals(NOT_PARTITIONED), "partitionHandle must be NOT_PARTITIONED");
            return notEmptyFuture
                    .thenApply(x -> getBatch(maxSize))
                    .thenApply(splits -> new ConnectorSplitBatch(splits, isFinished()));
        }

        private synchronized List<ConnectorSplit> getBatch(int maxSize)
        {
            // take up to maxSize elements from the queue
            List<ConnectorSplit> elements = new ArrayList<>(maxSize);
            queue.drainTo(elements, maxSize);

            // if the queue is empty and the current future is finished, create a new one so
            // a new readers can be notified when the queue has elements to read
            if (queue.isEmpty() && !closed) {
                if (notEmptyFuture.isDone()) {
                    notEmptyFuture = new CompletableFuture<>();
                }
            }

            return ImmutableList.copyOf(elements);
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed && queue.isEmpty();
        }

        @Override
        public synchronized void close()
        {
            closed = true;
        }
    }
}
