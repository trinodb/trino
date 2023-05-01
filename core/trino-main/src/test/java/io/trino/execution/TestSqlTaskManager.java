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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.InternalNode;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.version.EmbedVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.SPLIT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestSqlTaskManager
{
    private static final TaskId TASK_ID = new TaskId(new StageId("query", 0), 1, 0);
    public static final OutputBufferId OUT = new OutputBufferId(0);

    private TaskExecutor taskExecutor;
    private TaskManagementExecutor taskManagementExecutor;
    private LocalMemoryManager localMemoryManager;
    private LocalSpillManager localSpillManager;

    @BeforeClass
    public void setUp()
    {
        localMemoryManager = new LocalMemoryManager(new NodeMemoryConfig());
        localSpillManager = new LocalSpillManager(new NodeSpillConfig());
        taskExecutor = new TaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();
        taskManagementExecutor = new TaskManagementExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        taskExecutor.stop();
        taskExecutor = null;
        taskManagementExecutor.close();
        taskManagementExecutor = null;
    }

    @Test
    public void testEmptyQuery()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(), PipelinedOutputBuffers.createInitial(PARTITIONED).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test(timeOut = 30_000)
    public void testSimpleQuery()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);

            BufferResult results = sqlTaskManager.getTaskResults(taskId, OUT, 0, DataSize.of(1, Unit.MEGABYTE)).getResultsFuture().get();
            assertFalse(results.isBufferComplete());
            assertEquals(results.getSerializedPages().size(), 1);
            assertEquals(getSerializedPagePositionCount(results.getSerializedPages().get(0)), 1);

            for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
                results = sqlTaskManager.getTaskResults(taskId, OUT, results.getToken() + results.getSerializedPages().size(), DataSize.of(1, Unit.MEGABYTE)).getResultsFuture().get();
            }
            assertTrue(results.isBufferComplete());
            assertEquals(results.getSerializedPages().size(), 0);

            // complete the task by calling destroy on it
            TaskInfo info = sqlTaskManager.destroyTaskResults(taskId, OUT);
            assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getVersion()).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testCancel()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.cancelTask(taskId));
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbort()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.abortTask(taskId));
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test(timeOut = 30_000)
    public void testAbortResults()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);

            sqlTaskManager.destroyTaskResults(taskId, OUT);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getVersion()).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testRemoveOldTasks()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)))) {
            TaskId taskId = TASK_ID;

            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.cancelTask(taskId));
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);

            Thread.sleep(100);
            sqlTaskManager.removeOldTasks();

            for (TaskInfo info : sqlTaskManager.getAllTaskInfo()) {
                assertNotEquals(info.getTaskStatus().getTaskId(), taskId);
            }
        }
    }

    @Test
    public void testFailStuckSplitTasks()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        TestingTicker ticker = new TestingTicker();

        TaskHandle taskHandle = taskExecutor.addTask(
                TASK_ID,
                () -> 1.0,
                1,
                new Duration(1, SECONDS),
                OptionalInt.of(1));
        MockSplitRunner mockSplitRunner = new MockSplitRunner();

        TaskExecutor taskExecutor = new TaskExecutor(4, 8, 3, 4, ticker);
        // Here we explicitly enqueue an indefinite running split runner
        taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(mockSplitRunner));

        taskExecutor.start();
        try {
            // wait for the task executor to start processing the split
            mockSplitRunner.waitForStart();

            TaskManagerConfig taskManagerConfig = new TaskManagerConfig()
                    .setInterruptStuckSplitTasksEnabled(true)
                    .setInterruptStuckSplitTasksDetectionInterval(new Duration(10, SECONDS))
                    .setInterruptStuckSplitTasksWarningThreshold(new Duration(10, SECONDS))
                    .setInterruptStuckSplitTasksTimeout(new Duration(10, SECONDS));

            try (SqlTaskManager sqlTaskManager = createSqlTaskManager(taskManagerConfig, new NodeMemoryConfig(), taskExecutor, stackTraceElements -> true)) {
                sqlTaskManager.addStateChangeListener(TASK_ID, (state) -> {
                    if (state.isTerminatingOrDone() && !taskHandle.isDestroyed()) {
                        taskExecutor.removeTask(taskHandle);
                    }
                });

                ticker.increment(30, SECONDS);
                sqlTaskManager.failStuckSplitTasks();

                mockSplitRunner.waitForFinish();
                List<TaskInfo> taskInfos = sqlTaskManager.getAllTaskInfo();
                assertEquals(taskInfos.size(), 1);

                TaskInfo taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, taskInfos.get(0));
                assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FAILED);
            }
        }
        finally {
            taskExecutor.stop();
        }
    }

    @Test
    public void testSessionPropertyMemoryLimitOverride()
    {
        NodeMemoryConfig memoryConfig = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(DataSize.ofBytes(3));

        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig(), memoryConfig)) {
            TaskId reduceLimitsId = new TaskId(new StageId("q1", 0), 1, 0);
            TaskId increaseLimitsId = new TaskId(new StageId("q2", 0), 1, 0);

            QueryContext reducesLimitsContext = sqlTaskManager.getQueryContext(reduceLimitsId.getQueryId());
            QueryContext attemptsIncreaseContext = sqlTaskManager.getQueryContext(increaseLimitsId.getQueryId());

            // not initialized with a task update yet
            assertFalse(reducesLimitsContext.isMemoryLimitsInitialized());
            assertEquals(reducesLimitsContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());

            assertFalse(attemptsIncreaseContext.isMemoryLimitsInitialized());
            assertEquals(attemptsIncreaseContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());

            // memory limits reduced by session properties
            sqlTaskManager.updateTask(
                    testSessionBuilder()
                            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1B")
                            .build(),
                    reduceLimitsId,
                    Span.getInvalid(),
                    Optional.of(PLAN_FRAGMENT),
                    ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                    ImmutableMap.of(),
                    false);
            assertTrue(reducesLimitsContext.isMemoryLimitsInitialized());
            assertEquals(reducesLimitsContext.getMaxUserMemory(), 1);

            // memory limits not increased by session properties
            sqlTaskManager.updateTask(
                    testSessionBuilder()
                            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "10B")
                            .build(),
                    increaseLimitsId,
                    Span.getInvalid(),
                    Optional.of(PLAN_FRAGMENT),
                    ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                    ImmutableMap.of(),
                    false);
            assertTrue(attemptsIncreaseContext.isMemoryLimitsInitialized());
            assertEquals(attemptsIncreaseContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());
        }
    }

    private SqlTaskManager createSqlTaskManager(TaskManagerConfig config)
    {
        return createSqlTaskManager(config, new NodeMemoryConfig());
    }

    private SqlTaskManager createSqlTaskManager(TaskManagerConfig taskManagerConfig, NodeMemoryConfig nodeMemoryConfig)
    {
        return new SqlTaskManager(
                new EmbedVersion("testversion"),
                new NoConnectorServicesProvider(),
                createTestingPlanner(),
                new MockLocationFactory(),
                taskExecutor,
                createTestSplitMonitor(),
                new NodeInfo("test"),
                localMemoryManager,
                taskManagementExecutor,
                taskManagerConfig,
                nodeMemoryConfig,
                localSpillManager,
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry());
    }

    private SqlTaskManager createSqlTaskManager(
            TaskManagerConfig taskManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            TaskExecutor taskExecutor,
            Predicate<List<StackTraceElement>> stuckSplitStackTracePredicate)
    {
        return new SqlTaskManager(
                new EmbedVersion("testversion"),
                new NoConnectorServicesProvider(),
                createTestingPlanner(),
                new MockLocationFactory(),
                taskExecutor,
                createTestSplitMonitor(),
                new NodeInfo("test"),
                localMemoryManager,
                taskManagementExecutor,
                taskManagerConfig,
                nodeMemoryConfig,
                localSpillManager,
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry(),
                stuckSplitStackTracePredicate);
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, ImmutableSet<ScheduledSplit> splits, OutputBuffers outputBuffers)
    {
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, splits, true)),
                outputBuffers,
                ImmutableMap.of(),
                false);
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, OutputBuffers outputBuffers)
    {
        sqlTaskManager.getQueryContext(taskId.getQueryId())
                .addTaskContext(new TaskStateMachine(taskId, directExecutor()), testSessionBuilder().build(), () -> {}, false, false);
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                outputBuffers,
                ImmutableMap.of(),
                false);
    }

    private static TaskInfo pollTerminatingTaskInfoUntilDone(SqlTaskManager taskManager, TaskInfo taskInfo)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        assertTrue(taskInfo.getTaskStatus().getState().isTerminatingOrDone());
        int attempts = 3;
        while (attempts > 0 && taskInfo.getTaskStatus().getState().isTerminating()) {
            taskInfo = taskManager.getTaskInfo(taskInfo.getTaskStatus().getTaskId(), taskInfo.getTaskStatus().getVersion()).get(5, SECONDS);
            attempts--;
        }
        return taskInfo;
    }

    public static class MockDirectExchangeClientSupplier
            implements DirectExchangeClientSupplier
    {
        @Override
        public DirectExchangeClient get(
                QueryId queryId,
                ExchangeId exchangeId,
                LocalMemoryContext memoryContext,
                TaskFailureListener taskFailureListener,
                RetryPolicy retryPolicy)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return URI.create("http://fake.invalid/query/" + queryId);
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + taskId);
        }

        @Override
        public URI createTaskLocation(InternalNode node, TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + node.getNodeIdentifier() + "/" + taskId);
        }

        @Override
        public URI createMemoryInfoLocation(InternalNode node)
        {
            return URI.create("http://fake.invalid/" + node.getNodeIdentifier() + "/memory");
        }
    }

    private static class MockSplitRunner
            implements SplitRunner
    {
        private final SettableFuture<Void> startedFuture = SettableFuture.create();
        private final SettableFuture<Void> finishedFuture = SettableFuture.create();

        @GuardedBy("this")
        private Thread runnerThread;
        @GuardedBy("this")
        private boolean closed;

        public void waitForStart()
                throws ExecutionException, InterruptedException, TimeoutException
        {
            startedFuture.get(10, SECONDS);
        }

        public void waitForFinish()
                throws ExecutionException, InterruptedException, TimeoutException
        {
            finishedFuture.get(10, SECONDS);
        }

        @Override
        public int getPipelineId()
        {
            return 0;
        }

        @Override
        public Span getPipelineSpan()
        {
            return Span.getInvalid();
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed;
        }

        @Override
        public ListenableFuture<Void> processFor(Duration duration)
        {
            startedFuture.set(null);
            synchronized (this) {
                runnerThread = Thread.currentThread();

                if (closed) {
                    finishedFuture.set(null);
                    return immediateVoidFuture();
                }
            }

            while (true) {
                try {
                    Thread.sleep(100000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }

            synchronized (this) {
                closed = true;
            }
            finishedFuture.set(null);

            return immediateVoidFuture();
        }

        @Override
        public String getInfo()
        {
            return "MockSplitRunner";
        }

        @Override
        public synchronized void close()
        {
            closed = true;

            if (runnerThread != null) {
                runnerThread.interrupt();
            }
        }
    }

    private static class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

        @Override
        public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }
}
