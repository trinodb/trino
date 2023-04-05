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
import io.trino.cost.StatsAndCosts;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.PartitionedSplitsInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TableInfo;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import io.trino.split.ConnectorAwareSplitSource;
import io.trino.split.SplitSource;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy.STAGE;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static io.trino.execution.scheduler.StageExecution.State.PLANNED;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULING;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMultiSourcePartitionedScheduler
{
    private static final PlanNodeId TABLE_SCAN_1_NODE_ID = new PlanNodeId("1");
    private static final PlanNodeId TABLE_SCAN_2_NODE_ID = new PlanNodeId("2");
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
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();

    public TestMultiSourcePartitionedScheduler()
    {
        nodeManager.addNodes(
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
    public void testScheduleSplitsBatchedNoBlocking()
    {
        // Test whether two internal schedulers were completely scheduled - no blocking case
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(60), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(60)),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                7);

        for (int i = 0; i <= (60 / 7) * 2; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == (60 / 7) * 2) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i == 0 ? 3 : 0);
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 40);
        }
        stage.abort();
    }

    @Test
    public void testScheduleSplitsBatchedBlockingSplitSource()
    {
        // Test case when one internal scheduler has blocking split source and finally is blocked
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);
        QueuedSplitSource blockingSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(10), TABLE_SCAN_2_NODE_ID, blockingSplitSource),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                5);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);

        scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 0);
        assertEquals(scheduleResult.getBlockedReason(), Optional.of(WAITING_FOR_SOURCE));

        blockingSplitSource.addSplits(2, true);

        scheduleResult = scheduler.schedule();
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getSplitsScheduled(), 2);
        assertEquals(scheduleResult.getNewTasks().size(), 0);
        assertEquals(scheduleResult.getBlockedReason(), Optional.empty());
        assertTrue(scheduleResult.isFinished());

        assertPartitionedSplitCount(stage, 12);
        assertEffectivelyFinished(scheduleResult, scheduler);

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 4);
        }
        stage.abort();
    }

    @Test
    public void testScheduleSplitsTasksAreFull()
    {
        // Test the case when tasks are full
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(200), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(200)),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                200);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getSplitsScheduled(), 300);
        assertFalse(scheduleResult.isFinished());
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(scheduleResult.getBlockedReason(), Optional.of(SPLIT_QUEUES_FULL));

        assertEquals(stage.getAllTasks().stream().mapToInt(task -> task.getPartitionedSplitsInfo().getCount()).sum(), 300);
        stage.abort();
    }

    @Test
    public void testBalancedSplitAssignment()
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        PlanFragment firstPlan = createFragment();
        StageExecution firstStage = createStageExecution(firstPlan, nodeTaskMap);

        QueuedSplitSource firstSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        QueuedSplitSource secondSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, firstSplitSource, TABLE_SCAN_2_NODE_ID, secondSplitSource),
                createSplitPlacementPolicies(session, firstStage, nodeTaskMap, nodeManager),
                firstStage,
                15);

        // Only first split source produces splits at that moment
        firstSplitSource.addSplits(15, true);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            // All splits were balanced between nodes
            assertEquals(splitsInfo.getCount(), 5);
        }

        // Add new node
        InternalNode additionalNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(additionalNode);

        // Second source produced splits now
        secondSplitSource.addSplits(3, true);

        scheduleResult = scheduler.schedule();

        assertEffectivelyFinished(scheduleResult, scheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertTrue(scheduleResult.isFinished());
        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEquals(firstStage.getAllTasks().size(), 4);

        assertEquals(firstStage.getAllTasks().get(0).getPartitionedSplitsInfo().getCount(), 5);
        assertEquals(firstStage.getAllTasks().get(1).getPartitionedSplitsInfo().getCount(), 5);
        assertEquals(firstStage.getAllTasks().get(2).getPartitionedSplitsInfo().getCount(), 5);
        assertEquals(firstStage.getAllTasks().get(3).getPartitionedSplitsInfo().getCount(), 3);

        // Second source produces
        PlanFragment secondPlan = createFragment();
        StageExecution secondStage = createStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(10), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(10)),
                createSplitPlacementPolicies(session, secondStage, nodeTaskMap, nodeManager),
                secondStage,
                10);

        scheduleResult = secondScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, secondScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertTrue(scheduleResult.isFinished());
        assertEquals(scheduleResult.getNewTasks().size(), 4);
        assertEquals(secondStage.getAllTasks().size(), 4);

        for (RemoteTask task : secondStage.getAllTasks()) {
            assertEquals(task.getPartitionedSplitsInfo().getCount(), 5);
        }
        firstStage.abort();
        secondStage.abort();
    }

    @Test
    public void testScheduleEmptySources()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(0), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(0)),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                15);

        ScheduleResult scheduleResult = scheduler.schedule();

        // If both split sources produce no splits then internal schedulers add one split - it can be expected by some operators e.g. AggregationOperator
        assertEquals(scheduleResult.getNewTasks().size(), 2);
        assertEffectivelyFinished(scheduleResult, scheduler);

        stage.abort();
    }

    @Test
    public void testDynamicFiltersUnblockedOnBlockedBuildSource()
    {
        PlanFragment plan = createFragment();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        StageExecution stage = createStageExecution(plan, nodeTaskMap);
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig());
        dynamicFilterService.registerQuery(
                QUERY_ID,
                TEST_SESSION,
                ImmutableSet.of(DYNAMIC_FILTER_ID),
                ImmutableSet.of(DYNAMIC_FILTER_ID),
                ImmutableSet.of(DYNAMIC_FILTER_ID));

        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, new QueuedSplitSource(), TABLE_SCAN_2_NODE_ID, new QueuedSplitSource()),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                dynamicFilterService,
                () -> true,
                15);

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

    @Test
    public void testNoNewTaskScheduledWhenChildStageBufferIsOverUtilized()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager(
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        PlanFragment plan = createFragment();
        StageExecution stage = createStageExecution(plan, nodeTaskMap);

        // setting over utilized child output buffer
        StageScheduler scheduler = prepareScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(200), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(200)),
                createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager),
                stage,
                new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                () -> true,
                200);
        // the queues of 3 running nodes should be full
        ScheduleResult scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason(), Optional.of(SPLIT_QUEUES_FULL));
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(scheduleResult.getSplitsScheduled(), 300);
        for (RemoteTask remoteTask : scheduleResult.getNewTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 100);
        }

        // new node added but 1 child's output buffer is overutilized - so lockdown the tasks
        nodeManager.addNodes(new InternalNode("other4", URI.create("http://127.0.0.4:14"), NodeVersion.UNKNOWN, false));
        scheduleResult = scheduler.schedule();
        assertEquals(scheduleResult.getBlockedReason(), Optional.of(SPLIT_QUEUES_FULL));
        assertEquals(scheduleResult.getNewTasks().size(), 0);
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

    private StageScheduler prepareScheduler(
            Map<PlanNodeId, ConnectorSplitSource> splitSources,
            SplitPlacementPolicy splitPlacementPolicy,
            StageExecution stage,
            int splitBatchSize)
    {
        return prepareScheduler(
                splitSources,
                splitPlacementPolicy,
                stage,
                new DynamicFilterService(metadata, functionManager, typeOperators, new DynamicFilterConfig()),
                () -> false,
                splitBatchSize);
    }

    private StageScheduler prepareScheduler(
            Map<PlanNodeId, ConnectorSplitSource> splitSources,
            SplitPlacementPolicy splitPlacementPolicy,
            StageExecution stage,
            DynamicFilterService dynamicFilterService,
            BooleanSupplier anySourceTaskBlocked,
            int splitBatchSize)
    {
        Map<PlanNodeId, SplitSource> sources = splitSources.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ConnectorAwareSplitSource(TEST_CATALOG_HANDLE, e.getValue())));
        return new MultiSourcePartitionedScheduler(
                stage,
                sources,
                splitPlacementPolicy,
                splitBatchSize,
                dynamicFilterService,
                new TableExecuteContextManager(),
                anySourceTaskBlocked);
    }

    private PlanFragment createFragment()
    {
        return createFragment(TEST_TABLE_HANDLE, TEST_TABLE_HANDLE);
    }

    private PlanFragment createFragment(TableHandle firstTableHandle, TableHandle secondTableHandle)
    {
        Symbol symbol = new Symbol("column");
        Symbol buildSymbol = new Symbol("buildColumn");

        TableScanNode tableScanOne = new TableScanNode(
                TABLE_SCAN_1_NODE_ID,
                firstTableHandle,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                false,
                Optional.empty());
        FilterNode filterNodeOne = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScanOne,
                createDynamicFilterExpression(TEST_SESSION, createTestMetadataManager(), DYNAMIC_FILTER_ID, VARCHAR, symbol.toSymbolReference()));
        TableScanNode tableScanTwo = new TableScanNode(
                TABLE_SCAN_2_NODE_ID,
                secondTableHandle,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                false,
                Optional.empty());
        FilterNode filterNodeTwo = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScanTwo,
                createDynamicFilterExpression(TEST_SESSION, createTestMetadataManager(), DYNAMIC_FILTER_ID, VARCHAR, symbol.toSymbolReference()));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of(buildSymbol), Optional.empty(), REPLICATE, RetryPolicy.NONE);

        return new PlanFragment(
                new PlanFragmentId("plan_id"),
                new JoinNode(
                        new PlanNodeId("join_id"),
                        INNER,
                        new ExchangeNode(
                                planNodeIdAllocator.getNextId(),
                                REPARTITION,
                                LOCAL,
                                new PartitioningScheme(
                                        Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()),
                                        tableScanOne.getOutputSymbols()),
                                ImmutableList.of(
                                        filterNodeOne,
                                        filterNodeTwo),
                                ImmutableList.of(tableScanOne.getOutputSymbols(), tableScanTwo.getOutputSymbols()),
                                Optional.empty()),
                        remote,
                        ImmutableList.of(),
                        tableScanOne.getOutputSymbols(),
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
                Optional.empty(),
                ImmutableList.of(TABLE_SCAN_1_NODE_ID, TABLE_SCAN_2_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount)
    {
        return new FixedSplitSource(IntStream.range(0, splitCount).mapToObj(ix -> new TestingSplit(true, ImmutableList.of())).toList());
    }

    private SplitPlacementPolicy createSplitPlacementPolicies(Session session, StageExecution stage, NodeTaskMap nodeTaskMap, InternalNodeManager nodeManager)
    {
        return createSplitPlacementPolicies(session, stage, nodeTaskMap, nodeManager, TEST_CATALOG_HANDLE);
    }

    private SplitPlacementPolicy createSplitPlacementPolicies(Session session, StageExecution stage, NodeTaskMap nodeTaskMap, InternalNodeManager nodeManager, CatalogHandle catalog)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(100)
                .setMinPendingSplitsPerTask(0)
                .setSplitsBalancingPolicy(STAGE);
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap, new Duration(0, SECONDS)));
        return new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(session, Optional.of(catalog)), stage::getAllTasks);
    }

    private StageExecution createStageExecution(PlanFragment fragment, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(QUERY_ID, 0);
        SqlStage stage = SqlStage.createSqlStage(
                stageId,
                fragment,
                ImmutableMap.of(
                        TABLE_SCAN_1_NODE_ID, new TableInfo(Optional.of("test"), new QualifiedObjectName("test", "test", "test"), TupleDomain.all()),
                        TABLE_SCAN_2_NODE_ID, new TableInfo(Optional.of("test"), new QualifiedObjectName("test", "test", "test"), TupleDomain.all())),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                noopTracer(),
                new SplitSchedulerStats());
        ImmutableMap.Builder<PlanFragmentId, PipelinedOutputBufferManager> outputBuffers = ImmutableMap.builder();
        outputBuffers.put(fragment.getId(), new PartitionedPipelinedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 1));
        fragment.getRemoteSourceNodes().stream()
                .flatMap(node -> node.getSourceFragmentIds().stream())
                .forEach(fragmentId -> outputBuffers.put(fragmentId, new PartitionedPipelinedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 10)));
        return createPipelinedStageExecution(
                stage,
                outputBuffers.buildOrThrow(),
                TaskLifecycleListener.NO_OP,
                new NoOpFailureDetector(),
                queryExecutor,
                Optional.of(new int[] {0}),
                0);
    }

    private static class InMemoryNodeManagerByCatalog
            extends InMemoryNodeManager
    {
        private final Function<CatalogHandle, Set<InternalNode>> nodesByCatalogs;

        public InMemoryNodeManagerByCatalog(Set<InternalNode> nodes, Function<CatalogHandle, Set<InternalNode>> nodesByCatalogs)
        {
            super(nodes);
            this.nodesByCatalogs = nodesByCatalogs;
        }

        @Override
        public Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle)
        {
            return nodesByCatalogs.apply(catalogHandle);
        }
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

        public QueuedSplitSource()
        {
            this.splitFactory = TestingSplit::createRemoteSplit;
        }

        synchronized void addSplits(int count, boolean lastSplits)
        {
            if (closed) {
                return;
            }
            for (int i = 0; i < count; i++) {
                queue.add(splitFactory.get());
            }
            if (lastSplits) {
                close();
            }
            notEmptyFuture.complete(null);
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
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
