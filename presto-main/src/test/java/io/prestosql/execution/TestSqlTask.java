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
package io.prestosql.execution;

import com.google.common.base.Functions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.BufferState;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.MemoryPool;
import io.prestosql.memory.QueryContext;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.eventlistener.TracerEvent;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.tracer.DefaultTracer;
import io.prestosql.spi.tracer.Tracer;
import io.prestosql.spi.tracer.TracerEventType;
import io.prestosql.spiller.SpillSpaceTracker;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.tracer.TracerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.SqlTask.createSqlTask;
import static io.prestosql.execution.TaskTestUtils.EMPTY_SOURCES;
import static io.prestosql.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.prestosql.execution.TaskTestUtils.SPLIT;
import static io.prestosql.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.prestosql.execution.TaskTestUtils.createTestSplitMonitor;
import static io.prestosql.execution.TaskTestUtils.createTestingPlanner;
import static io.prestosql.execution.TaskTestUtils.updateTask;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_CANCELED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_FAILED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_FINISHED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_RUNNING;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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

    @Test
    public void testEmptyQuery()
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_FINISHED));
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        BufferResult results = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).get();
        assertEquals(results.isBufferComplete(), false);
        assertEquals(results.getSerializedPages().size(), 1);
        assertEquals(results.getSerializedPages().get(0).getPositionCount(), 1);

        for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
            results = sqlTask.getTaskResults(OUT, results.getToken() + results.getSerializedPages().size(), new DataSize(1, MEGABYTE)).get();
        }
        assertEquals(results.getSerializedPages().size(), 0);

        // complete the task by calling abort on it
        TaskInfo info = sqlTask.abortTaskResults(OUT);
        assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getState()).get(1, SECONDS);
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_FINISHED));
    }

    @Test
    public void testCancel()
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
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
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_CANCELED));
    }

    @Test
    public void testAbort()
            throws Exception
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        sqlTask.abortTaskResults(OUT);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getState()).get(1, SECONDS);
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_FINISHED));
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds();
        updateTask(sqlTask, EMPTY_SOURCES, outputBuffers);

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        // close the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)), outputBuffers);

        // finish the task by calling abort on it
        sqlTask.abortTaskResults(OUT);

        // buffer will be closed by cancel event (wait for event to fire)
        bufferResult.get(1, SECONDS);

        // verify the buffer is closed
        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_FINISHED));
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        updateTask(sqlTask, EMPTY_SOURCES, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        sqlTask.cancel();
        assertEquals(sqlTask.getTaskInfo().getTaskStatus().getState(), TaskState.CANCELED);

        // buffer future will complete.. the event is async so wait a bit for event to propagate
        bufferResult.get(1, SECONDS);

        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_CANCELED));
    }

    @Test
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        SqlTask sqlTask = createInitialTask(tracerEvents);

        updateTask(sqlTask, EMPTY_SOURCES, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        TaskState taskState = sqlTask.getTaskInfo().getTaskStatus().getState();
        sqlTask.failed(new Exception("test"));
        assertEquals(sqlTask.getTaskInfo(taskState).get(1, SECONDS).getTaskStatus().getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait a bit for event to fire
        try {
            assertTrue(bufferResult.get(1, SECONDS).isBufferComplete());
            fail("expected TimeoutException");
        }
        catch (TimeoutException expected) {
            // expected
        }
        assertFalse(sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).isDone());
        checkEvents(tracerEvents, ImmutableList.of(TASK_STATE_CHANGE_RUNNING, TASK_STATE_CHANGE_FAILED));
    }

    private SqlTask createInitialTask(List<TracerEvent> tracerEvents)
    {
        TaskId taskId = new TaskId("query", 0, nextTaskId.incrementAndGet());
        URI location = URI.create("fake://task/" + taskId);

        QueryContext queryContext = new QueryContext(new QueryId("query"),
                new DataSize(1, MEGABYTE),
                new DataSize(2, MEGABYTE),
                new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE)),
                new TestingGcMonitor(),
                taskNotificationExecutor,
                driverYieldExecutor,
                new DataSize(1, MEGABYTE),
                new SpillSpaceTracker(new DataSize(1, GIGABYTE)));

        queryContext.addTaskContext(new TaskStateMachine(taskId, taskNotificationExecutor), testSessionBuilder().build(), false, false, OptionalInt.empty());

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                sqlTaskExecutionFactory,
                taskNotificationExecutor,
                Functions.identity(),
                new DataSize(32, MEGABYTE),
                new CounterStat(),
                new TestingTracerFactory(tracerEvents));
    }

    private void checkEvents(List<TracerEvent> tracerEvents, List<TracerEventType> expectedEventTypes)
    {
        long start = System.currentTimeMillis();
        while (tracerEvents.size() == 0 ||
                tracerEvents.get(tracerEvents.size() - 1).getEventType() != TASK_STATE_CHANGE_FAILED.toTracerEventType() ||
                tracerEvents.get(tracerEvents.size() - 1).getEventType() != TASK_STATE_CHANGE_FINISHED.toTracerEventType() ||
                tracerEvents.get(tracerEvents.size() - 1).getEventType() != TASK_STATE_CHANGE_CANCELED.toTracerEventType()) {
            if (System.currentTimeMillis() - start > 5000) {
                break;
            }
        }

        List<String> eventTypes = tracerEvents.stream().map(TracerEvent::getEventType).collect(Collectors.toList());
        assertTrue(expectedEventTypes.stream().map(TracerEventType::toTracerEventType).allMatch(eventTypes::contains));
    }

    private static class TestingTracerFactory
            implements TracerFactory
    {
        private List<TracerEvent> tracerEvents;
        private final JsonCodec<Map<String, Object>> jsonCodec = JsonCodec.mapJsonCodec(String.class, Object.class);

        public TestingTracerFactory(List<TracerEvent> tracerEvents)
        {
            this.tracerEvents = requireNonNull(tracerEvents);
        }

        @Override
        public Tracer createTracer(String queryId, Session session)
        {
            return DefaultTracer.createBasicTracer(this.tracerEvents::add, jsonCodec::toJson, "node", URI.create("http://test.com"), queryId);
        }
    }
}
