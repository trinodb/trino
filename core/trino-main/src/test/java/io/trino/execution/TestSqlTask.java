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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.timesharing.TimeSharingTaskExecutor;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.Domain;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.LocalExecutionPlanner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import static io.trino.execution.SqlTask.createSqlTask;
import static io.trino.execution.TaskStatus.STARTING_VERSION;
import static io.trino.execution.TaskTestUtils.DYNAMIC_FILTER_SOURCE_ID;
import static io.trino.execution.TaskTestUtils.EMPTY_SPLIT_ASSIGNMENTS;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT_WITH_DYNAMIC_FILTER_SOURCE;
import static io.trino.execution.TaskTestUtils.SPLIT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.TaskTestUtils.updateTask;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSqlTask
{
    public static final OutputBufferId OUT = new OutputBufferId(0);

    private TaskExecutor taskExecutor;
    private ScheduledExecutorService taskNotificationExecutor;
    private ScheduledExecutorService driverYieldExecutor;
    private ScheduledExecutorService driverTimeoutExecutor;
    private SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicInteger nextTaskId = new AtomicInteger();

    @BeforeAll
    public void setUp()
    {
        taskExecutor = new TimeSharingTaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();

        taskNotificationExecutor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));
        driverYieldExecutor = newScheduledThreadPool(2, threadsNamed("driver-yield-%s"));
        driverTimeoutExecutor = newScheduledThreadPool(2, threadsNamed("driver-timeout-%s"));
        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                createTestSplitMonitor(),
                noopTracer(),
                new TaskManagerConfig());
    }

    @AfterAll
    public void destroy()
    {
        taskExecutor.stop();
        taskExecutor = null;
        taskNotificationExecutor.shutdownNow();
        driverYieldExecutor.shutdown();
        driverTimeoutExecutor.shutdown();
        sqlTaskExecutionFactory = null;
    }

    @Test
    @Timeout(30)
    public void testEmptyQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                PipelinedOutputBuffers.createInitial(PARTITIONED)
                        .withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(taskInfo.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION);

        taskInfo = sqlTask.getTaskInfo();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(taskInfo.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION);

        taskInfo = sqlTask.updateTask(TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                PipelinedOutputBuffers.createInitial(PARTITIONED)
                        .withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);
    }

    @Test
    @Timeout(30)
    public void testSimpleQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        assertThat(sqlTask.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(sqlTask.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION);
        sqlTask.updateTask(TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);

        TaskInfo taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FLUSHING);
        assertThat(taskInfo.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION + 1);

        // completed future should be returned immediately when old caller's version is used
        assertThat(sqlTask.getTaskInfo(STARTING_VERSION).isDone()).isTrue();

        BufferResult results = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE)).get();
        assertThat(results.isBufferComplete()).isFalse();
        assertThat(results.getSerializedPages().size()).isEqualTo(1);
        assertThat(getSerializedPagePositionCount(results.getSerializedPages().get(0))).isEqualTo(1);

        for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
            results = sqlTask.getTaskResults(OUT, results.getToken() + results.getSerializedPages().size(), DataSize.of(1, MEGABYTE)).get();
        }
        assertThat(results.getSerializedPages().size()).isEqualTo(0);

        // complete the task by calling destroy on it
        TaskInfo info = sqlTask.destroyTaskResults(OUT);
        assertThat(info.getOutputBuffers().getState()).isEqualTo(BufferState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(info.getTaskStatus().getVersion()).get();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);

        // completed future should be returned immediately when task is finished
        assertThat(sqlTask.getTaskInfo(STARTING_VERSION + 100).isDone()).isTrue();

        taskInfo = sqlTask.getTaskInfo();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);
    }

    @Test
    public void testCancel()
    {
        SqlTask sqlTask = createInitialTask();

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                PipelinedOutputBuffers.createInitial(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(taskInfo.getStats().getEndTime()).isNull();

        taskInfo = sqlTask.getTaskInfo();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(taskInfo.getStats().getEndTime()).isNull();

        taskInfo = sqlTask.cancel();
        // This call can race and report either cancelling or cancelled
        assertThat(taskInfo.getTaskStatus().getState().isTerminatingOrDone()).isTrue();
        // Task cancellation can race with output buffer state updates, but should transition to cancelled quickly
        int attempts = 1;
        while (!taskInfo.getTaskStatus().getState().isDone() && attempts < 3) {
            taskInfo = Futures.getUnchecked(sqlTask.getTaskInfo(taskInfo.getTaskStatus().getVersion()));
            attempts++;
        }
        assertThat(taskInfo.getTaskStatus().getState())
                .describedAs("Failed to see CANCELED after " + attempts + " attempts")
                .isEqualTo(TaskState.CANCELED);
        assertThat(taskInfo.getStats().getEndTime()).isNotNull();

        taskInfo = sqlTask.getTaskInfo();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.CANCELED);
        assertThat(taskInfo.getStats().getEndTime()).isNotNull();
    }

    @Test
    @Timeout(30)
    public void testAbort()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        assertThat(sqlTask.getTaskStatus().getState()).isEqualTo(TaskState.RUNNING);
        assertThat(sqlTask.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION);
        sqlTask.updateTask(TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);

        TaskInfo taskInfo = sqlTask.getTaskInfo(STARTING_VERSION).get();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FLUSHING);
        assertThat(taskInfo.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION + 1);

        sqlTask.destroyTaskResults(OUT);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getVersion()).get();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertThat(taskInfo.getTaskStatus().getState()).isEqualTo(TaskState.FINISHED);
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        OutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds();
        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, outputBuffers);

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertThat(bufferResult.isDone()).isFalse();

        // close the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)), outputBuffers);

        // finish the task by calling abort on it
        sqlTask.destroyTaskResults(OUT);

        // buffer will be closed by cancel event (wait for event to fire)
        bufferResult.get(1, SECONDS);

        // verify the buffer is closed
        bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertThat(bufferResult.isDone()).isTrue();
        assertThat(bufferResult.get().isBufferComplete()).isTrue();
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertThat(bufferResult.isDone()).isFalse();

        sqlTask.cancel();
        assertThat(sqlTask.getTaskInfo().getTaskStatus().getState().isTerminatingOrDone()).isTrue();

        // buffer future will complete, the event is async so wait a bit for event to propagate
        bufferResult.get(1, SECONDS);

        bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertThat(bufferResult.isDone()).isTrue();
        assertThat(bufferResult.get().isBufferComplete()).isTrue();
    }

    @Test
    @Timeout(30)
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();

        updateTask(sqlTask, EMPTY_SPLIT_ASSIGNMENTS, PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE));
        assertThat(bufferResult.isDone()).isFalse();

        long taskStatusVersion = sqlTask.getTaskInfo().getTaskStatus().getVersion();
        sqlTask.failed(new Exception("test"));
        // This call can race and return either FAILED or FAILING
        TaskInfo taskInfo = sqlTask.getTaskInfo(taskStatusVersion).get();
        assertThat(taskInfo.getTaskStatus().getState().isTerminatingOrDone()).isTrue();

        // This call should resolve to FAILED if the prior call did not
        taskStatusVersion = taskInfo.getTaskStatus().getVersion();
        assertThat(sqlTask.getTaskInfo(taskStatusVersion).get().getTaskStatus().getState()).isEqualTo(TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait a bit for event to fire
        assertThatThrownBy(() -> bufferResult.get(1, SECONDS))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Waited 1 seconds");
        assertThat(sqlTask.getTaskResults(OUT, 0, DataSize.of(1, MEGABYTE)).isDone()).isFalse();
    }

    @Test
    @Timeout(30)
    public void testDynamicFilters()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();
        sqlTask.updateTask(
                TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT_WITH_DYNAMIC_FILTER_SOURCE),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), false)),
                PipelinedOutputBuffers.createInitial(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                ImmutableMap.of(),
                false);

        assertThat(sqlTask.getTaskStatus().getDynamicFiltersVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION);

        TaskContext taskContext = sqlTask.getQueryContext().getTaskContextByTaskId(sqlTask.getTaskId());

        ListenableFuture<?> future = sqlTask.getTaskStatus(STARTING_VERSION);
        assertThat(future.isDone()).isFalse();

        // make sure future gets unblocked when dynamic filters version is updated
        taskContext.updateDomains(ImmutableMap.of(DYNAMIC_FILTER_SOURCE_ID, Domain.none(BIGINT)));
        assertThat(sqlTask.getTaskStatus().getVersion()).isEqualTo(STARTING_VERSION + 1);
        assertThat(sqlTask.getTaskStatus().getDynamicFiltersVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION + 1);
        future.get();
    }

    @Test
    @Timeout(30)
    public void testDynamicFilterFetchAfterTaskDone()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask();
        OutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds();
        sqlTask.updateTask(
                TEST_SESSION,
                Span.getInvalid(),
                Optional.of(PLAN_FRAGMENT_WITH_DYNAMIC_FILTER_SOURCE),
                ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), false)),
                outputBuffers,
                ImmutableMap.of(),
                false);

        assertThat(sqlTask.getTaskStatus().getDynamicFiltersVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION);

        // close the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)), outputBuffers);

        // complete the task by calling destroy on it
        TaskInfo info = sqlTask.destroyTaskResults(OUT);
        assertThat(info.getOutputBuffers().getState()).isEqualTo(BufferState.FINISHED);

        assertEventually(new Duration(10, SECONDS), () -> {
            TaskStatus status = sqlTask.getTaskStatus(info.getTaskStatus().getVersion()).get();
            assertThat(status.getState()).isEqualTo(TaskState.FINISHED);
            assertThat(status.getDynamicFiltersVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION + 1);
        });
        VersionedDynamicFilterDomains versionedDynamicFilters = sqlTask.acknowledgeAndGetNewDynamicFilterDomains(INITIAL_DYNAMIC_FILTERS_VERSION);
        assertThat(versionedDynamicFilters.getVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION + 1);
        assertThat(versionedDynamicFilters.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of(DYNAMIC_FILTER_SOURCE_ID, Domain.none(BIGINT)));
    }

    private SqlTask createInitialTask()
    {
        TaskId taskId = new TaskId(new StageId("query", 0), nextTaskId.incrementAndGet(), 0);
        URI location = URI.create("fake://task/" + taskId);

        QueryContext queryContext = new QueryContext(new QueryId("query"),
                DataSize.of(1, MEGABYTE),
                new MemoryPool(DataSize.of(1, GIGABYTE)),
                new TestingGcMonitor(),
                taskNotificationExecutor,
                driverYieldExecutor,
                driverTimeoutExecutor,
                DataSize.of(1, MEGABYTE),
                new SpillSpaceTracker(DataSize.of(1, GIGABYTE)));

        queryContext.addTaskContext(new TaskStateMachine(taskId, taskNotificationExecutor), testSessionBuilder().build(), () -> {}, false, false);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                noopTracer(),
                sqlTaskExecutionFactory,
                taskNotificationExecutor,
                sqlTask -> {},
                DataSize.of(32, MEGABYTE),
                DataSize.of(200, MEGABYTE),
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer()),
                new CounterStat());
    }
}
