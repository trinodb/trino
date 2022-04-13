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

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.Lifespan;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TestingRemoteTaskFactory;
import io.trino.execution.TestingRemoteTaskFactory.TestingRemoteTask;
import io.trino.execution.scheduler.TestingExchange.TestingExchangeSinkHandle;
import io.trino.execution.scheduler.TestingExchange.TestingExchangeSourceHandle;
import io.trino.execution.scheduler.TestingNodeSelectorFactory.TestingNodeSupplier;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestFaultTolerantStageScheduler
{
    private static final QueryId QUERY_ID = new QueryId("query");
    private static final Session SESSION = testSessionBuilder()
            .setQueryId(QUERY_ID)
            .build();

    private static final StageId STAGE_ID = new StageId(QUERY_ID, 0);
    private static final PlanFragmentId FRAGMENT_ID = new PlanFragmentId("0");
    private static final PlanFragmentId SOURCE_FRAGMENT_ID_1 = new PlanFragmentId("1");
    private static final PlanFragmentId SOURCE_FRAGMENT_ID_2 = new PlanFragmentId("2");
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("table_scan_id");

    private static final CatalogName CATALOG = new CatalogName("catalog");

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://127.0.0.1:8080"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://127.0.0.1:8081"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://127.0.0.1:8082"), NodeVersion.UNKNOWN, false);

    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private FixedCountNodeAllocatorService nodeAllocatorService;

    private TestingTicker ticker;
    private TestFutureCompletor futureCompletor;

    @BeforeClass
    public void beforeClass()
    {
        finalizerService = new FinalizerService();
        finalizerService.start();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        ticker = new TestingTicker();
        futureCompletor = new TestFutureCompletor(ticker);
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        nodeTaskMap = null;
        if (finalizerService != null) {
            finalizerService.destroy();
            finalizerService = null;
        }
    }

    private void setupNodeAllocatorService(TestingNodeSupplier nodeSupplier)
    {
        shutdownNodeAllocatorService(); // just in case
        nodeAllocatorService = new FixedCountNodeAllocatorService(new NodeScheduler(new TestingNodeSelectorFactory(NODE_1, nodeSupplier)));
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
    public void testHappyPath()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(5, 2);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG),
                NODE_3, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sinkExchange = new TestingExchange(false);

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.of(sinkExchange),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    2,
                    1);

            ListenableFuture<Void> blocked = scheduler.isBlocked();
            assertUnblocked(blocked);

            scheduler.schedule();

            blocked = scheduler.isBlocked();
            // blocked on first source exchange
            assertBlocked(blocked);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            // still blocked on the second source exchange
            assertBlocked(blocked);
            assertFalse(scheduler.isBlocked().isDone());

            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            // now unblocked
            assertUnblocked(blocked);
            assertUnblocked(scheduler.isBlocked());

            scheduler.schedule();

            blocked = scheduler.isBlocked();
            // blocked on node allocation
            assertBlocked(blocked);

            // not all tasks have been enumerated yet
            assertFalse(sinkExchange.isNoMoreSinks());

            Map<TaskId, TestingRemoteTask> tasks = remoteTaskFactory.getTasks();
            // one task per node
            assertThat(tasks).hasSize(3);
            assertThat(tasks).containsKey(getTaskId(0, 0));
            assertThat(tasks).containsKey(getTaskId(1, 0));
            assertThat(tasks).containsKey(getTaskId(2, 0));

            TestingRemoteTask task = tasks.get(getTaskId(0, 0));
            // fail task for partition 0
            task.fail(new RuntimeException("some failure"));

            assertUnblocked(blocked);
            assertUnblocked(scheduler.isBlocked());

            // schedule more tasks
            moveTime(10, SECONDS); // skip retry delay
            scheduler.schedule();

            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(4);
            assertThat(tasks).containsKey(getTaskId(3, 0));

            blocked = scheduler.isBlocked();
            // blocked on task scheduling
            assertBlocked(blocked);

            // finish some task
            assertThat(tasks).containsKey(getTaskId(1, 0));
            tasks.get(getTaskId(1, 0)).finish();

            assertUnblocked(blocked);
            assertUnblocked(scheduler.isBlocked());
            assertThat(sinkExchange.getFinishedSinkHandles()).contains(new TestingExchangeSinkHandle(1));

            // this will schedule failed task
            scheduler.schedule();

            blocked = scheduler.isBlocked();
            // blocked on task scheduling
            assertBlocked(blocked);

            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(5);
            assertThat(tasks).containsKey(getTaskId(0, 1));

            // finish some task
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).containsKey(getTaskId(3, 0));
            tasks.get(getTaskId(3, 0)).finish();
            assertThat(sinkExchange.getFinishedSinkHandles()).contains(new TestingExchangeSinkHandle(1), new TestingExchangeSinkHandle(3));

            assertUnblocked(blocked);

            // schedule the last task
            scheduler.schedule();

            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(6);
            assertThat(tasks).containsKey(getTaskId(4, 0));

            // not finished yet, will be finished when all tasks succeed
            assertFalse(scheduler.isFinished());

            blocked = scheduler.isBlocked();
            // blocked on task scheduling
            assertBlocked(blocked);

            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).containsKey(getTaskId(4, 0));
            // finish remaining tasks
            tasks.get(getTaskId(0, 1)).finish();
            tasks.get(getTaskId(2, 0)).finish();
            tasks.get(getTaskId(4, 0)).finish();

            // now it's not blocked and finished
            assertUnblocked(blocked);
            assertUnblocked(scheduler.isBlocked());

            assertThat(sinkExchange.getFinishedSinkHandles()).contains(
                    new TestingExchangeSinkHandle(0),
                    new TestingExchangeSinkHandle(1),
                    new TestingExchangeSinkHandle(2),
                    new TestingExchangeSinkHandle(3),
                    new TestingExchangeSinkHandle(4));

            assertTrue(scheduler.isFinished());
        }
    }

    @Test
    public void testTasksWaitingForNodes()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        List<Split> splits = ImmutableList.of(
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_1.getHostAndPort())), Lifespan.taskWide()),  // 0
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_1.getHostAndPort())), Lifespan.taskWide()),  // 1
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_1.getHostAndPort())), Lifespan.taskWide()),  // 2
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_2.getHostAndPort())), Lifespan.taskWide()),  // 3
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_1.getHostAndPort())), Lifespan.taskWide()),  // 4
                new Split(CATALOG, new TestingSplit(false, ImmutableList.of(NODE_3.getHostAndPort())), Lifespan.taskWide())); // 5
        TestingTaskSourceFactory taskSourceFactory = new TestingTaskSourceFactory(Optional.of(CATALOG), splits, 2);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG),
                NODE_3, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sinkExchange = new TestingExchange(false);
        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);
        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.of(sinkExchange),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    2,
                    3); // allow for 3 tasks waiting for nodes before blocking

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            scheduler.schedule();

            Map<TaskId, TestingRemoteTask> tasks;

            // we reached max pending tasks count (3) on split 4 and blocked; task for split 5 will not be allocated even though NODE_3 is free
            assertBlocked(scheduler.isBlocked());
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(2);
            assertThat(tasks).containsKey(getTaskId(0, 0));
            assertThat(tasks).containsKey(getTaskId(3, 0));

            // unblocking NODE_2 does not help
            tasks.get(getTaskId(3, 0)).finish();
            scheduler.schedule();
            assertBlocked(scheduler.isBlocked());
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(2);
            assertThat(tasks).containsKey(getTaskId(0, 0));
            assertThat(tasks).containsKey(getTaskId(3, 0));

            // unblocking NODE_1 allows for scheduling next pending split for NODE_1 and NODE_3
            tasks.get(getTaskId(0, 0)).finish();
            scheduler.schedule();
            assertBlocked(scheduler.isBlocked());
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).hasSize(4);
            assertThat(tasks).containsKey(getTaskId(0, 0));
            assertThat(tasks).containsKey(getTaskId(1, 0)); // NEW (NODE_1)
            assertThat(tasks).containsKey(getTaskId(3, 0));
            assertThat(tasks).containsKey(getTaskId(5, 0)); // NEW (NODE_3)

            // finish all remaining tasks until scheduler is finished

            tasks.get(getTaskId(1, 0)).finish();
            scheduler.schedule();
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).containsKey(getTaskId(2, 0)); // NEW (NODE_1)

            tasks.get(getTaskId(2, 0)).finish();
            scheduler.schedule();
            tasks = remoteTaskFactory.getTasks();
            assertThat(tasks).containsKey(getTaskId(4, 0)); // NEW (NODE_1)

            tasks.get(getTaskId(4, 0)).finish();
            tasks.get(getTaskId(3, 0)).finish();
            tasks.get(getTaskId(5, 0)).finish();
            scheduler.schedule();
            assertUnblocked(scheduler.isBlocked());
            assertTrue(scheduler.isFinished());
        }
    }

    @Test
    public void testTaskLifecycleListener()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(2, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingTaskLifecycleListener taskLifecycleListener = new TestingTaskLifecycleListener();

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    taskLifecycleListener,
                    Optional.empty(),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    2,
                    1);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            assertUnblocked(scheduler.isBlocked());

            scheduler.schedule();
            assertBlocked(scheduler.isBlocked());

            assertThat(taskLifecycleListener.getTasks().get(FRAGMENT_ID)).contains(getTaskId(0, 0), getTaskId(1, 0));

            remoteTaskFactory.getTasks().get(getTaskId(0, 0)).fail(new RuntimeException("some exception"));

            assertUnblocked(scheduler.isBlocked());
            moveTime(10, SECONDS); // skip retry delay
            scheduler.schedule();
            assertBlocked(scheduler.isBlocked());

            assertThat(taskLifecycleListener.getTasks().get(FRAGMENT_ID)).contains(getTaskId(0, 0), getTaskId(1, 0), getTaskId(0, 1));
        }
    }

    @Test
    public void testTaskFailure()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(3, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.empty(),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    0,
                    1);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            assertUnblocked(scheduler.isBlocked());

            scheduler.schedule();

            ListenableFuture<Void> blocked = scheduler.isBlocked();
            // waiting on node acquisition
            assertBlocked(blocked);

            NodeAllocator.NodeLease acquireNode1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of(), DataSize.of(4, GIGABYTE)));
            NodeAllocator.NodeLease acquireNode2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of(), DataSize.of(4, GIGABYTE)));

            remoteTaskFactory.getTasks().get(getTaskId(0, 0)).fail(new RuntimeException("some failure"));

            assertUnblocked(blocked);
            assertUnblocked(acquireNode1.getNode());
            assertUnblocked(acquireNode2.getNode());

            assertThatThrownBy(scheduler::schedule)
                    .hasMessageContaining("some failure");

            assertUnblocked(scheduler.isBlocked());
            assertFalse(scheduler.isFinished());
        }
    }

    @Test
    public void testReportTaskFailure()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(2, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.empty(),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    1,
                    1);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            assertUnblocked(scheduler.isBlocked());

            scheduler.schedule();

            ListenableFuture<Void> blocked = scheduler.isBlocked();
            // waiting for tasks to finish
            assertBlocked(blocked);

            scheduler.reportTaskFailure(getTaskId(0, 0), new RuntimeException("some failure"));
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);

            assertUnblocked(blocked);
            moveTime(10, SECONDS); // skip retry delay
            scheduler.schedule();

            assertThat(remoteTaskFactory.getTasks()).containsKey(getTaskId(0, 1));

            remoteTaskFactory.getTasks().get(getTaskId(0, 1)).finish();
            remoteTaskFactory.getTasks().get(getTaskId(1, 0)).finish();

            assertUnblocked(scheduler.isBlocked());
            assertTrue(scheduler.isFinished());
        }
    }

    @Test
    public void testRetryDelay()
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(3, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG),
                NODE_3, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        Session session = testSessionBuilder()
                .setQueryId(QUERY_ID)
                .setSystemProperty("retry_initial_delay", "1s")
                .setSystemProperty("retry_max_delay", "3s")
                .setSystemProperty("retry_delay_scale_factor", "2.0")
                .build();

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(session, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    session,
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.empty(),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    6,
                    1);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            assertUnblocked(scheduler.isBlocked());
            scheduler.schedule();

            ListenableFuture<Void> blocked = scheduler.isBlocked();

            // T+0.0 all tasks are running
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(3);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+0.0 fail task 0.0
            scheduler.reportTaskFailure(getTaskId(0, 0), new RuntimeException("some failure"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(3);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+0.9 retry should not trigger yet
            moveTime(900, MILLISECONDS);
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(3);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+1.4s past retry delay for task 0
            moveTime(500, MILLISECONDS);
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(4);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+1.4 fail task 0.1
            scheduler.reportTaskFailure(getTaskId(0, 1), new RuntimeException("some other failure"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(4);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+3.3 another retry should not happen yet (delay is 2s on second failure)
            moveTime(1900, MILLISECONDS);
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(4);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+3.5s past retry delay for task 0.1
            moveTime(200, MILLISECONDS);
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(5);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+3.5 fail task 0.2
            scheduler.reportTaskFailure(getTaskId(0, 2), new RuntimeException("some other failure"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(5);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+6.4 another retry should not happen yet (delay is 3s on thirf failure (we reached limit)
            moveTime(2900, MILLISECONDS);
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(5);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+6.6s past retry delay for task 0.2
            moveTime(200, MILLISECONDS);
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(6);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+6.6 task 1 failure
            scheduler.reportTaskFailure(getTaskId(1, 0), new RuntimeException("some other failure"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(6);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+9.0 task1 still not retried (delay is 3s)
            moveTime(2400, MILLISECONDS);
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(6);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+9.0 task 0.3 completes successfully - should reset delay for stage
            remoteTaskFactory.getTasks().get(getTaskId(0, 3)).finish();
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(7);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.FINISHED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 1)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+9.0 kill task 1.1; delay should count from 1s again as there was a success
            scheduler.reportTaskFailure(getTaskId(1, 1), new RuntimeException("some other failure"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(7);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.FINISHED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+10.1 task 1.2 should be started
            moveTime(1100, MILLISECONDS);
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(8);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.FINISHED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 2)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.RUNNING);

            // T+10.1 if we kill task with out of memory error next try should be started right away
            scheduler.reportTaskFailure(getTaskId(2, 0), new TrinoException(StandardErrorCode.CLUSTER_OUT_OF_MEMORY, "oom"));
            assertUnblocked(blocked);
            scheduler.schedule();
            blocked = scheduler.isBlocked();
            assertBlocked(blocked);
            assertThat(remoteTaskFactory.getTasks()).hasSize(9);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 2)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(0, 3)).getTaskStatus().getState(), TaskState.FINISHED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 1)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(1, 2)).getTaskStatus().getState(), TaskState.RUNNING);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 0)).getTaskStatus().getState(), TaskState.FAILED);
            assertEquals(remoteTaskFactory.getTasks().get(getTaskId(2, 1)).getTaskStatus().getState(), TaskState.RUNNING);
        }
    }

    @Test
    public void testCancellation()
            throws Exception
    {
        testCancellation(true);
        testCancellation(false);
    }

    private void testCancellation(boolean abort)
            throws Exception
    {
        TestingRemoteTaskFactory remoteTaskFactory = new TestingRemoteTaskFactory();
        TestingTaskSourceFactory taskSourceFactory = createTaskSourceFactory(3, 1);
        TestingNodeSupplier nodeSupplier = TestingNodeSupplier.create(ImmutableMap.of(
                NODE_1, ImmutableList.of(CATALOG),
                NODE_2, ImmutableList.of(CATALOG)));
        setupNodeAllocatorService(nodeSupplier);

        TestingExchange sourceExchange1 = new TestingExchange(false);
        TestingExchange sourceExchange2 = new TestingExchange(false);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            FaultTolerantStageScheduler scheduler = createFaultTolerantTaskScheduler(
                    remoteTaskFactory,
                    taskSourceFactory,
                    nodeAllocator,
                    TaskLifecycleListener.NO_OP,
                    Optional.empty(),
                    ImmutableMap.of(SOURCE_FRAGMENT_ID_1, sourceExchange1, SOURCE_FRAGMENT_ID_2, sourceExchange2),
                    0,
                    1);

            sourceExchange1.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            sourceExchange2.setSourceHandles(ImmutableList.of(new TestingExchangeSourceHandle(0, 1)));
            assertUnblocked(scheduler.isBlocked());

            scheduler.schedule();

            ListenableFuture<Void> blocked = scheduler.isBlocked();
            // waiting on node acquisition
            assertBlocked(blocked);

            NodeAllocator.NodeLease acquireNode1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of(), DataSize.of(4, GIGABYTE)));
            NodeAllocator.NodeLease acquireNode2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of(), DataSize.of(4, GIGABYTE)));

            if (abort) {
                scheduler.abort();
            }
            else {
                scheduler.cancel();
            }

            assertUnblocked(blocked);
            assertUnblocked(acquireNode1.getNode());
            assertUnblocked(acquireNode2.getNode());

            scheduler.schedule();

            assertUnblocked(scheduler.isBlocked());
            assertFalse(scheduler.isFinished());
        }
    }

    private FaultTolerantStageScheduler createFaultTolerantTaskScheduler(
            RemoteTaskFactory remoteTaskFactory,
            TaskSourceFactory taskSourceFactory,
            NodeAllocator nodeAllocator,
            TaskLifecycleListener taskLifecycleListener,
            Optional<Exchange> sinkExchange,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            int retryAttempts,
            int maxTasksWaitingForNodePerStage)
    {
        return createFaultTolerantTaskScheduler(
                SESSION,
                remoteTaskFactory,
                taskSourceFactory,
                nodeAllocator,
                taskLifecycleListener,
                sinkExchange,
                sourceExchanges,
                retryAttempts,
                maxTasksWaitingForNodePerStage);
    }

    private FaultTolerantStageScheduler createFaultTolerantTaskScheduler(
            Session session,
            RemoteTaskFactory remoteTaskFactory,
            TaskSourceFactory taskSourceFactory,
            NodeAllocator nodeAllocator,
            TaskLifecycleListener taskLifecycleListener,
            Optional<Exchange> sinkExchange,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            int retryAttempts,
            int maxTasksWaitingForNodePerStage)
    {
        TaskDescriptorStorage taskDescriptorStorage = new TaskDescriptorStorage(DataSize.of(10, MEGABYTE));
        taskDescriptorStorage.initialize(SESSION.getQueryId());
        return new FaultTolerantStageScheduler(
                session,
                createSqlStage(remoteTaskFactory),
                new NoOpFailureDetector(),
                taskSourceFactory,
                nodeAllocator,
                taskDescriptorStorage,
                new ConstantPartitionMemoryEstimator(),
                taskLifecycleListener,
                futureCompletor,
                ticker,
                sinkExchange,
                Optional.empty(),
                sourceExchanges,
                Optional.empty(),
                Optional.empty(),
                new AtomicInteger(retryAttempts),
                retryAttempts,
                maxTasksWaitingForNodePerStage);
    }

    private SqlStage createSqlStage(RemoteTaskFactory remoteTaskFactory)
    {
        PlanFragment fragment = createPlanFragment();
        return SqlStage.createSqlStage(
                STAGE_ID,
                fragment,
                ImmutableMap.of(),
                remoteTaskFactory,
                SESSION,
                false,
                nodeTaskMap,
                directExecutor(),
                new SplitSchedulerStats());
    }

    private PlanFragment createPlanFragment()
    {
        Symbol probeColumnSymbol = new Symbol("probe_column");
        Symbol buildColumnSymbol = new Symbol("build_column");
        TableScanNode tableScan = new TableScanNode(
                TABLE_SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(probeColumnSymbol),
                ImmutableMap.of(probeColumnSymbol, new TestingColumnHandle("column")),
                TupleDomain.none(),
                Optional.empty(),
                false,
                Optional.empty());
        RemoteSourceNode remoteSource = new RemoteSourceNode(
                new PlanNodeId("remote_source_id"),
                ImmutableList.of(SOURCE_FRAGMENT_ID_1, SOURCE_FRAGMENT_ID_2),
                ImmutableList.of(buildColumnSymbol),
                Optional.empty(),
                REPLICATE,
                TASK);
        return new PlanFragment(
                FRAGMENT_ID,
                new JoinNode(
                        new PlanNodeId("join_id"),
                        INNER,
                        tableScan,
                        remoteSource,
                        ImmutableList.of(),
                        tableScan.getOutputSymbols(),
                        remoteSource.getOutputSymbols(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty()),
                ImmutableMap.of(probeColumnSymbol, VARCHAR, buildColumnSymbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(probeColumnSymbol, buildColumnSymbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }

    private static TestingTaskSourceFactory createTaskSourceFactory(int splitCount, int taskPerBatch)
    {
        return new TestingTaskSourceFactory(Optional.of(CATALOG), createSplits(splitCount), taskPerBatch);
    }

    private static List<Split> createSplits(int count)
    {
        return ImmutableList.copyOf(limit(cycle(new Split(CATALOG, createRemoteSplit(), Lifespan.taskWide())), count));
    }

    private static TaskId getTaskId(int partitionId, int attemptId)
    {
        return new TaskId(STAGE_ID, partitionId, attemptId);
    }

    private static void assertBlocked(ListenableFuture<?> blocked)
    {
        assertFalse(blocked.isDone());
    }

    private static void assertUnblocked(ListenableFuture<?> blocked)
    {
        assertTrue(blocked.isDone());
    }

    private void moveTime(int delta, TimeUnit unit)
    {
        ticker.increment(delta, unit);
        futureCompletor.trigger();
    }

    private static class TestFutureCompletor
            implements FaultTolerantStageScheduler.DelayedFutureCompletor
    {
        private final Stopwatch stopwatch;
        private final Set<Entry> entries = Sets.newConcurrentHashSet();

        private TestFutureCompletor(Ticker ticker)
        {
            this.stopwatch = Stopwatch.createStarted(ticker);
        }

        @Override
        public void completeFuture(SettableFuture<Void> future, Duration delay)
        {
            entries.add(new Entry(future, stopwatch.elapsed().plus(delay)));
        }

        public void trigger()
        {
            Duration now = stopwatch.elapsed();
            Iterator<Entry> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                if (entry.completionTime.compareTo(now) <= 0) {
                    entry.future.set(null);
                    iterator.remove();
                }
            }
        }

        private static class Entry
        {
            private final SettableFuture<Void> future;
            private final Duration completionTime;

            public Entry(SettableFuture<Void> future, Duration completionTime)
            {
                this.future = future;
                this.completionTime = completionTime;
            }
        }
    }
}
