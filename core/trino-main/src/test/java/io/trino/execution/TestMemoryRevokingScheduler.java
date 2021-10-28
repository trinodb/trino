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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.HandleResolver;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.SqlTask.createSqlTask;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.TaskTestUtils.updateTask;
import static io.trino.execution.TestSqlTask.OUT;
import static io.trino.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryRevokingScheduler
{
    private final AtomicInteger idGeneator = new AtomicInteger();
    private final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(DataSize.of(10, GIGABYTE));
    private final Map<QueryId, QueryContext> queryContexts = new HashMap<>();

    private ScheduledExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private MemoryPool memoryPool;

    private Set<OperatorContext> allOperatorContexts;

    @BeforeMethod
    public void setUp()
    {
        memoryPool = new MemoryPool(GENERAL_POOL, DataSize.ofBytes(10));

        TaskExecutor taskExecutor = new TaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
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
                new TaskManagerConfig());

        allOperatorContexts = null;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryContexts.clear();
        memoryPool = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
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
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(singletonList(memoryPool), () -> tasks, executor, 1.0, 1.0);

        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2, operatorContext3, operatorContext4, operatorContext5);
        assertMemoryRevokingNotRequested();

        requestMemoryRevoking(scheduler);
        assertEquals(10, memoryPool.getFreeBytes());
        assertMemoryRevokingNotRequested();

        LocalMemoryContext revocableMemory1 = operatorContext1.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory3 = operatorContext3.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory4 = operatorContext4.localRevocableMemoryContext();
        LocalMemoryContext revocableMemory5 = operatorContext5.localRevocableMemoryContext();

        revocableMemory1.setBytes(3);
        revocableMemory3.setBytes(6);
        assertEquals(1, memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // we are still good - no revoking needed
        assertMemoryRevokingNotRequested();

        revocableMemory4.setBytes(7);
        assertEquals(-6, memoryPool.getFreeBytes());
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
        assertEquals(-3, memoryPool.getFreeBytes());

        // and allocate some more
        revocableMemory5.setBytes(3);
        assertEquals(-6, memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // we are still good with just OC3 in process of revoking
        assertMemoryRevokingRequestedFor(operatorContext3);

        // and allocate some more
        revocableMemory5.setBytes(4);
        assertEquals(-7, memoryPool.getFreeBytes());
        requestMemoryRevoking(scheduler);
        // no we have to trigger revoking for OC4
        assertMemoryRevokingRequestedFor(operatorContext3, operatorContext4);
    }

    @Test
    public void testCountAlreadyRevokedMemoryWithinAPool()
            throws Exception
    {
        // Given
        SqlTask sqlTask1 = newSqlTask(new QueryId("q1"));
        MemoryPool anotherMemoryPool = new MemoryPool(new MemoryPoolId("test"), DataSize.ofBytes(10));
        sqlTask1.getQueryContext().setMemoryPool(anotherMemoryPool);
        OperatorContext operatorContext1 = createContexts(sqlTask1);

        SqlTask sqlTask2 = newSqlTask(new QueryId("q2"));
        OperatorContext operatorContext2 = createContexts(sqlTask2);

        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(asList(memoryPool, anotherMemoryPool), () -> tasks, executor, 1.0, 1.0);
        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2);

        /*
         * sqlTask1 fills its pool
         */
        operatorContext1.localRevocableMemoryContext().setBytes(12);
        requestMemoryRevoking(scheduler);
        assertMemoryRevokingRequestedFor(operatorContext1);

        /*
         * When sqlTask2 fills its pool
         */
        operatorContext2.localRevocableMemoryContext().setBytes(12);
        requestMemoryRevoking(scheduler);

        /*
         * Then sqlTask2 should be asked to revoke its memory too
         */
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext2);
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
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(singletonList(memoryPool), () -> tasks, executor, 1.0, 1.0);
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
        OperatorContext operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("na"), "na");

        return operatorContext;
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
        ImmutableSet<OperatorContext> operatorContextsSet = ImmutableSet.copyOf(operatorContexts);
        operatorContextsSet.forEach(
                operatorContext -> assertTrue(operatorContext.isMemoryRevokingRequested(), "expected memory requested for operator " + operatorContext.getOperatorId()));
        Sets.difference(allOperatorContexts, operatorContextsSet).forEach(
                operatorContext -> assertFalse(operatorContext.isMemoryRevokingRequested(), "expected memory  not requested for operator " + operatorContext.getOperatorId()));
    }

    private void assertMemoryRevokingNotRequested()
    {
        assertMemoryRevokingRequestedFor();
    }

    private SqlTask newSqlTask(QueryId queryId)
    {
        QueryContext queryContext = getOrCreateQueryContext(queryId);

        TaskId taskId = new TaskId(new StageId(queryId.getId(), 0), idGeneator.incrementAndGet(), 0);
        URI location = URI.create("fake://task/" + taskId);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                sqlTaskExecutionFactory,
                executor,
                sqlTask -> {},
                DataSize.of(32, MEGABYTE),
                DataSize.of(200, MEGABYTE),
                new ExchangeManagerRegistry(new HandleResolver()),
                new CounterStat());
    }

    private QueryContext getOrCreateQueryContext(QueryId queryId)
    {
        return queryContexts.computeIfAbsent(queryId, id -> new QueryContext(id,
                DataSize.of(1, MEGABYTE),
                DataSize.of(2, MEGABYTE),
                Optional.empty(),
                memoryPool,
                new TestingGcMonitor(),
                executor,
                scheduledExecutor,
                DataSize.of(1, GIGABYTE),
                spillSpaceTracker));
    }

    private TaskContext getOrCreateTaskContext(SqlTask sqlTask)
    {
        if (sqlTask.getTaskContext().isEmpty()) {
            // update task to update underlying taskHolderReference with taskExecution + create a new taskContext
            updateTask(sqlTask, ImmutableList.of(), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
        }
        return sqlTask.getTaskContext().orElseThrow(() -> new IllegalStateException("TaskContext not present"));
    }
}
