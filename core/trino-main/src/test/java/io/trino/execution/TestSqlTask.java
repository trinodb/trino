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
import io.airlift.stats.CounterStat;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.metadata.HandleResolver;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolId;
import io.trino.spi.predicate.Domain;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.SqlTask.createSqlTask;
import static io.trino.execution.TaskStatus.STARTING_VERSION;
import static io.trino.execution.TaskTestUtils.EMPTY_SPLIT_ASSIGNMENTS;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.SPLIT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.TaskTestUtils.updateTask;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSqlTask
{
    public static final OutputBufferId OUT = new OutputBufferId(0);
    private final TaskExecutor taskExecutor;
    private final ScheduledExecutorService taskNotificationExecutor;
    private final ScheduledExecutorService driverYieldExecutor;
    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicInteger nextTaskId = new AtomicInteger();

    public TestSqlTask()
    {
        taskExecutor = new TaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();

        taskNotificationExecutor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));
        driverYieldExecutor = newScheduledThreadPool(2, threadsNamed("driver-yield-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                createTestSplitMonitor(),
                new TaskManagerConfig());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        taskExecutor.stop();
        taskNotificationExecutor.shutdownNow();
        driverYieldExecutor.shutdown();
    }

    @Test(timeOut = 30_000)
    public void testEmptyQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                ImmutableMap.of());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertEquals(taskInfo.getTaskStatus().getVersion(), STARTING_VERSION);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertEquals(taskInfo.getTaskStatus().getVersion(), STARTING_VERSION);

        taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                ImmutableMap.of());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test(timeOut = 30_000)
    public void testSimpleQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        assertEquals(sqlTask.getTaskStatus().getState(), TaskState.RUNNING);
        assertEquals(sqlTask.getTaskStatus().getVersion(), STARTING_VERSION);
        sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                ImmutableMap.of());

        TaskInfo taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);
        assertEquals(taskInfo.getTaskStatus().getVersion(), STARTING_VERSION + 1);

        // completed future should be returned immediately when old caller's version is used
        assertTrue(sqlTask.getTaskInfo(STARTING_VERSION).isDone());

        BufferResult results = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE)).get();
        assertFalse(results.isBufferComplete());
        assertEquals(results.getSerializedPages().size(), 1);
        assertEquals(getSerializedPagePositionCount(results.getSerializedPages().get(0)), 1);

        for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
            results = sqlTask.getTaskResults(OUT, results.getToken() + results.getSerializedPages().size(), DataSize.of(1, MEGABYTE)).get();
        }
        assertEquals(results.getSerializedPages().size(), 0);

        // complete the task by calling abort on it
        TaskInfo info = sqlTask.abortTaskResults(OUT);
        assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(info.getTaskStatus().getVersion()).get();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        // completed future should be returned immediately when task is finished
        assertTrue(sqlTask.getTaskInfo(STARTING_VERSION + 100).isDone());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test
    public void testCancel()
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                ImmutableMap.of());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.cancel();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());
    }

    @Test(timeOut = 30_000)
    public void testAbort()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        assertEquals(sqlTask.getTaskStatus().getState(), TaskState.RUNNING);
        assertEquals(sqlTask.getTaskStatus().getVersion(), STARTING_VERSION);
        sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                ImmutableMap.of());

        TaskInfo taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FLUSHING);
        assertEquals(taskInfo.getTaskStatus().getVersion(), STARTING_VERSION + 1);

        sqlTask.abortTaskResults(OUT);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getVersion()).get();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds();
        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, outputBuffers);

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        // close the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)), outputBuffers);

        // finish the task by calling abort on it
        sqlTask.abortTaskResults(OUT);

        // buffer will be closed by cancel event (wait for event to fire)
        bufferResult.get(1, SECONDS);

        // verify the buffer is closed
        bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        sqlTask.cancel();
        assertEquals(sqlTask.getTaskInfo().getTaskStatus().getState(), TaskState.CANCELED);

        // buffer future will complete.. the event is async so wait a bit for event to propagate
        bufferResult.get(1, SECONDS);

        bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
    }

    @Test(timeOut = 30_000)
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        long taskStatusVersion = sqlTask.getTaskInfo().getTaskStatus().getVersion();
        sqlTask.failed(new Exception("test"));
        assertEquals(sqlTask.getTaskInfo(taskStatusVersion).get().getTaskStatus().getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait a bit for event to fire
        assertThatThrownBy(() -> bufferResult.get(1, SECONDS))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Waited 1 seconds");
        assertFalse(sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE)).isDone());
    }

    @Test(timeOut = 30_000)
    public void testDynamicFilters()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();
        sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), false)),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                ImmutableMap.of());

        assertEquals(sqlTask.getTaskStatus().getDynamicFiltersVersion(), INITIAL_DYNAMIC_FILTERS_VERSION);

        TaskContext taskContext = sqlTask.getQueryContext().getTaskContextByTaskId(sqlTask.getTaskId());

        ListenableFuture<?> future = sqlTask.getTaskStatus(STARTING_VERSION);
        assertFalse(future.isDone());

        // make sure future gets unblocked when dynamic filters version is updated
        taskContext.updateDomains(ImmutableMap.of(new DynamicFilterId("filter"), Domain.none(BIGINT)));
        assertEquals(sqlTask.getTaskStatus().getVersion(), STARTING_VERSION + 1);
        assertEquals(sqlTask.getTaskStatus().getDynamicFiltersVersion(), INITIAL_DYNAMIC_FILTERS_VERSION + 1);
        future.get();
    }

    private SqlTask createInitialTask()
    {
        TaskId taskId = new TaskId(new StageId("query", 0), nextTaskId.incrementAndGet(), 0);
        URI location = URI.create("fake://task/" + taskId);

        QueryContext queryContext = new QueryContext(new QueryId("query"),
                DataSize.of(1, MEGABYTE),
                Optional.empty(),
                new MemoryPool(new MemoryPoolId("test"), DataSize.of(1, GIGABYTE)),
                new TestingGcMonitor(),
                taskNotificationExecutor,
                driverYieldExecutor,
                DataSize.of(1, MEGABYTE),
                new SpillSpaceTracker(DataSize.of(1, GIGABYTE)));

        queryContext.addTaskContext(new TaskStateMachine(taskId, taskNotificationExecutor), testSessionBuilder().build(), () -> {}, false, false);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                sqlTaskExecutionFactory,
                taskNotificationExecutor,
                sqlTask -> {},
                DataSize.of(32, MEGABYTE),
                DataSize.of(200, MEGABYTE),
                new ExchangeManagerRegistry(new HandleResolver()),
                new CounterStat());
    }
}
