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
import com.google.common.collect.Sets;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.timesharing.TimeSharingTaskExecutor;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.SqlTask.createSqlTask;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.TaskTestUtils.updateTask;
import static io.trino.execution.TestSqlTask.OUT;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestMemoryRevokingScheduler
{
    private final AtomicInteger idGenerator = new AtomicInteger();
    private final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(DataSize.of(10, GIGABYTE));
    private final Map<QueryId, QueryContext> queryContexts = new HashMap<>();

    private MemoryPool memoryPool;
    private TaskExecutor taskExecutor;
    private ScheduledExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private Set<OperatorContext> allOperatorContexts;

    @BeforeEach
    public void setUp()
    {
        memoryPool = new MemoryPool(DataSize.ofBytes(10));

        taskExecutor = new TimeSharingTaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();

        // Must be single threaded
        executor = newScheduledThreadPool(1, threadsNamed("task-notification-%s"));
        scheduledExecutor = newScheduledThreadPool(2, threadsNamed("task-notification-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                executor,
                taskExecutor,
                planner,
                createTestSplitMonitor(),
                noopTracer(),
                new TaskManagerConfig());

        allOperatorContexts = null;
    }

    @AfterEach
    public void tearDown()
    {
        queryContexts.clear();
        memoryPool = null;
        taskExecutor.stop();
        taskExecutor = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        sqlTaskExecutionFactory = null;
        allOperatorContexts = null;
    }

    @Test
    public void testScheduleMemoryRevoking()
            throws Exception
    {
        QueryContext q1 = getOrCreateQueryContext(new QueryId("q1"));
        QueryContext q2 = getOrCreateQueryContext(new QueryId("q2"));

        SqlTask sqlTask1 = newSqlTask(q1.getQueryId());
        SqlTask sqlTask2 = newSqlTask(q2.getQueryId());

        TaskContext taskContext1 = getOrCreateTaskContext(sqlTask1);
        PipelineContext pipelineContext11 = taskContext1.addPipelineContext(0, false, false, false);
        DriverContext driverContext111 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext1 = driverContext111.addOperatorContext(1, new PlanNodeId("na"), "na");
        OperatorContext operatorContext2 = driverContext111.addOperatorContext(2, new PlanNodeId("na"), "na");
        DriverContext driverContext112 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext3 = driverContext112.addOperatorContext(3, new PlanNodeId("na"), "na");

        TaskContext taskContext2 = getOrCreateTaskContext(sqlTask2);
        PipelineContext pipelineContext21 = taskContext2.addPipelineContext(1, false, false, false);
        DriverContext driverContext211 = pipelineContext21.addDriverContext();
        OperatorContext operatorContext4 = driverContext211.addOperatorContext(4, new PlanNodeId("na"), "na");
        OperatorContext operatorContext5 = driverContext211.addOperatorContext(5, new PlanNodeId("na"), "na");

        Collection<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(memoryPool, () -> tasks, executor, 1.0, 1.0);

        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2, operatorContext3, operatorContext4, operatorContext5);
        assertMemoryRevokingNotRequested();

        requestMemoryRevoking(scheduler);
        assertThat(10).isEqualTo(memoryPool.getFreeBytes());
        assertMemoryRevokingNotRequested();

        LocalMemoryContext revocableMemory1 = operatorContext1.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory3 = operatorContext3.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory4 = operatorContext4.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory5 = operatorContext5.localRevocableMemoryContext();

        revocableMemory1.setBytes(3);
        revocableMemory3.setBytes(6);
        assertThat(1).isEqualTo(memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // we are still good - no revoking needed
        assertMemoryRevokingNotRequested();

        revocableMemory4.setBytes(7);
        assertThat(-6).isEqualTo(memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // we need to revoke 3 and 6
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);

        // yet another revoking request should not change anything
        requestMemoryRevoking(scheduler);
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);

        // lets revoke some bytes
        revocableMemory1.setBytes(0);
        operatorContext1.resetMemoryRevokingRequested();
        requestMemoryRevoking(scheduler);
        assertMemoryRevokingRequestedFor(operatorContext3);
        assertThat(-3).isEqualTo(memoryPool.getFreeBytes());

        // and allocate some more
        revocableMemory5.setBytes(3);
        assertThat(-6).isEqualTo(memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // we are still good with just OC3 in process of revoking
        assertMemoryRevokingRequestedFor(operatorContext3);

        // and allocate some more
        revocableMemory5.setBytes(4);
        assertThat(-7).isEqualTo(memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // no we have to trigger revoking for OC4
        assertMemoryRevokingRequestedFor(operatorContext3, operatorContext4);
    }

    /**
     * Test that when a {@link MemoryPool} is over-allocated, revocable memory is revoked without delay (although asynchronously).
     */
    @Test
    public void testImmediateMemoryRevoking()
            throws Exception
    {
        // Given
        SqlTask sqlTask = newSqlTask(new QueryId("query"));
        OperatorContext operatorContext = createContexts(sqlTask);

        allOperatorContexts = ImmutableSet.of(operatorContext);
        List<SqlTask> tasks = ImmutableList.of(sqlTask);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(memoryPool, () -> tasks, executor, 1.0, 1.0);
        scheduler.registerPoolListeners(); // no periodic check initiated

        // When
        operatorContext.localRevocableMemoryContext().setBytes(12);
        awaitAsynchronousCallbacksRun();

        // Then
        assertMemoryRevokingRequestedFor(operatorContext);
    }

    private OperatorContext createContexts(SqlTask sqlTask)
    {
        TaskContext taskContext = getOrCreateTaskContext(sqlTask);
        PipelineContext pipelineContext = taskContext.addPipelineContext(0, false, false, false);
        DriverContext driverContext = pipelineContext.addDriverContext();

        return driverContext.addOperatorContext(1, new PlanNodeId("na"), "na");
    }

    private void requestMemoryRevoking(MemoryRevokingScheduler scheduler)
            throws Exception
    {
        scheduler.requestMemoryRevokingIfNeeded();
        awaitAsynchronousCallbacksRun();
    }

    private void awaitAsynchronousCallbacksRun()
            throws Exception
    {
        // Make sure asynchronous callback got called (executor is single-threaded).
        executor.invokeAll(singletonList((Callable<?>) () -> null));
    }

    private void assertMemoryRevokingRequestedFor(OperatorContext... operatorContexts)
    {
        Set<OperatorContext> operatorContextsSet = ImmutableSet.copyOf(operatorContexts);
        operatorContextsSet.forEach(
                operatorContext -> assertThat(operatorContext.isMemoryRevokingRequested())
                        .describedAs("expected memory requested for operator " + operatorContext.getOperatorId())
                        .isTrue());
        Sets.difference(allOperatorContexts, operatorContextsSet).forEach(
                operatorContext -> assertThat(operatorContext.isMemoryRevokingRequested())
                        .describedAs("expected memory  not requested for operator " + operatorContext.getOperatorId())
                        .isFalse());
    }

    private void assertMemoryRevokingNotRequested()
    {
        assertMemoryRevokingRequestedFor();
    }

    private SqlTask newSqlTask(QueryId queryId)
    {
        QueryContext queryContext = getOrCreateQueryContext(queryId);

        TaskId taskId = new TaskId(new StageId(queryId.getId(), 0), idGenerator.incrementAndGet(), 0);
        URI location = URI.create("fake://task/" + taskId);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                noopTracer(),
                sqlTaskExecutionFactory,
                executor,
                sqlTask -> {},
                DataSize.of(32, MEGABYTE),
                DataSize.of(200, MEGABYTE),
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())),
                new CounterStat());
    }

    private QueryContext getOrCreateQueryContext(QueryId queryId)
    {
        return queryContexts.computeIfAbsent(queryId, id -> new QueryContext(id,
                DataSize.of(1, MEGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                executor,
                scheduledExecutor,
                scheduledExecutor,
                DataSize.of(1, GIGABYTE),
                spillSpaceTracker));
    }

    private TaskContext getOrCreateTaskContext(SqlTask sqlTask)
    {
        if (sqlTask.getTaskContext().isEmpty()) {
            // update task to update underlying taskHolderReference with taskExecution + create a new taskContext
            updateTask(sqlTask, ImmutableList.of(), PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
        }
        return sqlTask.getTaskContext().orElseThrow(() -> new IllegalStateException("TaskContext not present"));
    }
}
