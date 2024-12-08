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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.InternalNode;
import io.trino.metadata.LanguageFunctionEngineManager;
import io.trino.metadata.WorkerLanguageFunctionProvider;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.util.EmbedVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class BaseTestSqlTaskManager
{
    public static final OutputBufferId OUT = new OutputBufferId(0);
    private final AtomicInteger sequence = new AtomicInteger();

    private TaskExecutor taskExecutor;
    private TaskManagementExecutor taskManagementExecutor;

    protected abstract TaskExecutor createTaskExecutor();

    @BeforeAll
    public void setUp()
    {
        taskExecutor = createTaskExecutor();
        taskExecutor.start();
        taskManagementExecutor = new TaskManagementExecutor();
    }

    @AfterAll
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
            TaskId taskId = newTaskId();
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withNoMoreBufferIds());
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);

            taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(), PipelinedOutputBuffers.createInitial(PARTITIONED).withNoMoreBufferIds());
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);
        }
    }

    @Test
    @Timeout(30)
    public void testSimpleQuery()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = newTaskId();
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FLUSHING);

            BufferResult results = sqlTaskManager.getTaskResults(taskId, OUT, 0, DataSize.of(1, Unit.MEGABYTE)).getResultsFuture().get();
            assertThat(results.isBufferComplete()).isFalse();
            assertThat(results.getSerializedPages()).hasSize(1);
            assertThat(getSerializedPagePositionCount(results.getSerializedPages().get(0))).isEqualTo(1);

            for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
                results = sqlTaskManager.getTaskResults(taskId, OUT, results.getToken() + results.getSerializedPages().size(), DataSize.of(1, Unit.MEGABYTE)).getResultsFuture().get();
            }
            assertThat(results.isBufferComplete()).isTrue();
            assertThat(results.getSerializedPages()).isEmpty();

            // complete the task by calling destroy on it
            TaskInfo info = sqlTaskManager.destroyTaskResults(taskId, OUT);
            assertThat(info.outputBuffers().getState()).isEqualTo(BufferState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.taskStatus().getVersion()).get();
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);
            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);
        }
    }

    @Test
    public void testCancel()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = newTaskId();
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);
            assertThat(taskInfo.stats().getEndTime()).isNull();

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);
            assertThat(taskInfo.stats().getEndTime()).isNull();

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.cancelTask(taskId));
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.CANCELED);
            assertThat(taskInfo.stats().getEndTime()).isNotNull();

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.CANCELED);
            assertThat(taskInfo.stats().getEndTime()).isNotNull();
        }
    }

    @Test
    public void testAbort()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = newTaskId();
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);
            assertThat(taskInfo.stats().getEndTime()).isNull();

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);
            assertThat(taskInfo.stats().getEndTime()).isNull();

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.abortTask(taskId));
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.ABORTED);
            assertThat(taskInfo.stats().getEndTime()).isNotNull();

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.ABORTED);
            assertThat(taskInfo.stats().getEndTime()).isNotNull();
        }
    }

    @Test
    @Timeout(30)
    public void testAbortResults()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = newTaskId();
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FLUSHING);

            sqlTaskManager.destroyTaskResults(taskId, OUT);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.taskStatus().getVersion()).get();
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.FINISHED);
        }
    }

    @Test
    public void testRemoveOldTasks()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)))) {
            TaskId taskId = newTaskId();

            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.RUNNING);

            taskInfo = pollTerminatingTaskInfoUntilDone(sqlTaskManager, sqlTaskManager.cancelTask(taskId));
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.CANCELED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertThat(taskInfo.taskStatus().getState()).isEqualTo(TaskState.CANCELED);

            Thread.sleep(100);
            sqlTaskManager.removeOldTasks();

            for (TaskInfo info : sqlTaskManager.getAllTaskInfo()) {
                assertThat(info.taskStatus().getTaskId())
                        .isNotEqualTo(taskId);
            }
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
            assertThat(reducesLimitsContext.isMemoryLimitsInitialized()).isFalse();
            assertThat(reducesLimitsContext.getMaxUserMemory()).isEqualTo(memoryConfig.getMaxQueryMemoryPerNode().toBytes());

            assertThat(attemptsIncreaseContext.isMemoryLimitsInitialized()).isFalse();
            assertThat(attemptsIncreaseContext.getMaxUserMemory()).isEqualTo(memoryConfig.getMaxQueryMemoryPerNode().toBytes());

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
            assertThat(reducesLimitsContext.isMemoryLimitsInitialized()).isTrue();
            assertThat(reducesLimitsContext.getMaxUserMemory()).isEqualTo(1);

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
            assertThat(attemptsIncreaseContext.isMemoryLimitsInitialized()).isTrue();
            assertThat(attemptsIncreaseContext.getMaxUserMemory()).isEqualTo(memoryConfig.getMaxQueryMemoryPerNode().toBytes());
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
                new WorkerLanguageFunctionProvider(new LanguageFunctionEngineManager()),
                new MockLocationFactory(),
                taskExecutor,
                createTestSplitMonitor(),
                new NodeInfo("test"),
                new LocalMemoryManager(nodeMemoryConfig),
                taskManagementExecutor,
                taskManagerConfig,
                nodeMemoryConfig,
                new LocalSpillManager(new NodeSpillConfig()),
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())));
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
        assertThat(taskInfo.taskStatus().getState().isTerminatingOrDone()).isTrue();
        int attempts = 3;
        while (attempts > 0 && taskInfo.taskStatus().getState().isTerminating()) {
            taskInfo = taskManager.getTaskInfo(taskInfo.taskStatus().getTaskId(), taskInfo.taskStatus().getVersion()).get(5, SECONDS);
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
                Span parentSpan,
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

    private TaskId newTaskId()
    {
        return new TaskId(new StageId("query" + sequence.incrementAndGet(), 0), 1, 0);
    }
}
