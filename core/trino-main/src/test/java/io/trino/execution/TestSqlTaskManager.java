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
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.InternalNode;
import io.trino.operator.ExchangeClient;
import io.trino.operator.ExchangeClientSupplier;
import io.trino.spi.QueryId;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.version.EmbedVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static io.trino.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.SPLIT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestSqlTaskManager
{
    private static final TaskId TASK_ID = new TaskId("query", 0, 1);
    public static final OutputBufferId OUT = new OutputBufferId(0);

    private final TaskExecutor taskExecutor;
    private final TaskManagementExecutor taskManagementExecutor;
    private final LocalMemoryManager localMemoryManager;
    private final LocalSpillManager localSpillManager;

    public TestSqlTaskManager()
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
        taskManagementExecutor.close();
    }

    @Test
    public void testEmptyQuery()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(), createInitialEmptyOutputBuffers(PARTITIONED).withNoMoreBufferIds());
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
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);

            BufferResult results = sqlTaskManager.getTaskResults(taskId, OUT, 0, DataSize.of(1, Unit.MEGABYTE)).get();
            assertFalse(results.isBufferComplete());
            assertEquals(results.getSerializedPages().size(), 1);
            assertEquals(results.getSerializedPages().get(0).getPositionCount(), 1);

            for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
                results = sqlTaskManager.getTaskResults(taskId, OUT, results.getToken() + results.getSerializedPages().size(), DataSize.of(1, Unit.MEGABYTE)).get();
            }
            assertTrue(results.isBufferComplete());
            assertEquals(results.getSerializedPages().size(), 0);

            // complete the task by calling abort on it
            TaskInfo info = sqlTaskManager.abortTaskResults(taskId, OUT);
            assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getVersion()).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testCancel()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.cancelTask(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbort()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.abortTask(taskId);
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
            createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

            TaskInfo taskInfo = sqlTaskManager.getTaskInfo(taskId, TaskStatus.STARTING_VERSION).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);

            sqlTaskManager.abortTaskResults(taskId, OUT);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getVersion()).get();
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testRemoveOldTasks()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)))) {
            TaskId taskId = TASK_ID;

            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.cancelTask(taskId);
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
    public void testSessionPropertyMemoryLimitOverride()
    {
        NodeMemoryConfig memoryConfig = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(DataSize.ofBytes(3))
                .setMaxQueryTotalMemoryPerNode(DataSize.ofBytes(4));

        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig(), memoryConfig)) {
            TaskId reduceLimitsId = new TaskId("q1", 0, 1);
            TaskId increaseLimitsId = new TaskId("q2", 0, 1);

            QueryContext reducesLimitsContext = sqlTaskManager.getQueryContext(reduceLimitsId.getQueryId());
            QueryContext attemptsIncreaseContext = sqlTaskManager.getQueryContext(increaseLimitsId.getQueryId());

            // not initialized with a task update yet
            assertFalse(reducesLimitsContext.isMemoryLimitsInitialized());
            assertEquals(reducesLimitsContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());
            assertEquals(reducesLimitsContext.getMaxTotalMemory(), memoryConfig.getMaxQueryTotalMemoryPerNode().toBytes());

            assertFalse(attemptsIncreaseContext.isMemoryLimitsInitialized());
            assertEquals(attemptsIncreaseContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());
            assertEquals(attemptsIncreaseContext.getMaxTotalMemory(), memoryConfig.getMaxQueryTotalMemoryPerNode().toBytes());

            // memory limits reduced by session properties
            sqlTaskManager.updateTask(
                    testSessionBuilder()
                            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1B")
                            .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2B")
                            .build(),
                    reduceLimitsId,
                    Optional.of(PLAN_FRAGMENT),
                    ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                    ImmutableMap.of());
            assertTrue(reducesLimitsContext.isMemoryLimitsInitialized());
            assertEquals(reducesLimitsContext.getMaxUserMemory(), 1);
            assertEquals(reducesLimitsContext.getMaxTotalMemory(), 2);

            // memory limits not increased by session properties
            sqlTaskManager.updateTask(
                    testSessionBuilder()
                            .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "10B")
                            .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "10B")
                            .build(),
                    increaseLimitsId,
                    Optional.of(PLAN_FRAGMENT),
                    ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                    createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                    ImmutableMap.of());
            assertTrue(attemptsIncreaseContext.isMemoryLimitsInitialized());
            assertEquals(attemptsIncreaseContext.getMaxUserMemory(), memoryConfig.getMaxQueryMemoryPerNode().toBytes());
            assertEquals(attemptsIncreaseContext.getMaxTotalMemory(), memoryConfig.getMaxQueryTotalMemoryPerNode().toBytes());
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
                new TestingGcMonitor());
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, ImmutableSet<ScheduledSplit> splits, OutputBuffers outputBuffers)
    {
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, splits, true)),
                outputBuffers,
                ImmutableMap.of());
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, OutputBuffers outputBuffers)
    {
        sqlTaskManager.getQueryContext(taskId.getQueryId())
                .addTaskContext(new TaskStateMachine(taskId, directExecutor()), testSessionBuilder().build(), () -> {}, false, false);
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                outputBuffers,
                ImmutableMap.of());
    }

    public static class MockExchangeClientSupplier
            implements ExchangeClientSupplier
    {
        @Override
        public ExchangeClient get(LocalMemoryContext systemMemoryContext)
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
}
