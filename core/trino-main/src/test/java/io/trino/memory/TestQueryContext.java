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
package io.trino.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static io.trino.memory.LocalMemoryManager.RESERVED_POOL;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestQueryContext
{
    private static final ScheduledExecutorService TEST_EXECUTOR = newScheduledThreadPool(1, threadsNamed("test-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        TEST_EXECUTOR.shutdownNow();
    }

    @DataProvider
    public Object[][] testSetMemoryPoolOptions()
    {
        return new Object[][] {
                {false},
                {true},
        };
    }

    @Test(dataProvider = "testSetMemoryPoolOptions")
    public void testSetMemoryPool(boolean useReservedPool)
    {
        QueryId secondQuery = new QueryId("second");
        MemoryPool reservedPool = new MemoryPool(RESERVED_POOL, DataSize.ofBytes(10));
        long secondQueryMemory = reservedPool.getMaxBytes() - 1;
        if (useReservedPool) {
            assertTrue(reservedPool.reserve(secondQuery, "test", secondQueryMemory).isDone());
        }

        try (LocalQueryRunner localQueryRunner = LocalQueryRunner.create(TEST_SESSION)) {
            QueryContext queryContext = new QueryContext(
                    new QueryId("query"),
                    DataSize.ofBytes(10),
                    DataSize.ofBytes(20),
                    Optional.empty(),
                    new MemoryPool(GENERAL_POOL, DataSize.ofBytes(10)),
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    DataSize.ofBytes(0),
                    new SpillSpaceTracker(DataSize.ofBytes(0)));

            // Use memory
            queryContext.getQueryMemoryContext().initializeLocalMemoryContexts("test");
            LocalMemoryContext userMemoryContext = queryContext.getQueryMemoryContext().localUserMemoryContext();
            LocalMemoryContext revocableMemoryContext = queryContext.getQueryMemoryContext().localRevocableMemoryContext();
            assertTrue(userMemoryContext.setBytes(3).isDone());
            assertTrue(revocableMemoryContext.setBytes(5).isDone());

            queryContext.setMemoryPool(reservedPool);

            if (useReservedPool) {
                reservedPool.free(secondQuery, "test", secondQueryMemory);
            }

            // Free memory
            userMemoryContext.close();
            revocableMemoryContext.close();
        }
    }

    @Test
    public void testMoveTaggedAllocations()
    {
        MemoryPool generalPool = new MemoryPool(GENERAL_POOL, DataSize.ofBytes(10_000));
        MemoryPool reservedPool = new MemoryPool(RESERVED_POOL, DataSize.ofBytes(10_000));
        QueryId queryId = new QueryId("query");
        QueryContext queryContext = createQueryContext(queryId, generalPool);
        TaskStateMachine taskStateMachine = new TaskStateMachine(TaskId.valueOf("task-id"), TEST_EXECUTOR);
        TaskContext taskContext = queryContext.addTaskContext(taskStateMachine, TEST_SESSION, () -> {}, false, false);
        DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "test");

        // allocate some memory in the general pool
        LocalMemoryContext memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext("test_context");
        memoryContext.setBytes(1_000);

        Map<String, Long> allocations = generalPool.getTaggedMemoryAllocations().get(queryId);
        assertEquals(allocations, ImmutableMap.of("test_context", 1_000L));

        queryContext.setMemoryPool(reservedPool);

        assertNull(generalPool.getTaggedMemoryAllocations().get(queryId));
        allocations = reservedPool.getTaggedMemoryAllocations().get(queryId);
        assertEquals(allocations, ImmutableMap.of("test_context", 1_000L));

        assertEquals(generalPool.getFreeBytes(), 10_000);
        assertEquals(reservedPool.getFreeBytes(), 9_000);

        memoryContext.close();

        assertEquals(generalPool.getFreeBytes(), 10_000);
        assertEquals(reservedPool.getFreeBytes(), 10_000);
    }

    private static QueryContext createQueryContext(QueryId queryId, MemoryPool generalPool)
    {
        return new QueryContext(queryId,
                DataSize.ofBytes(10_000),
                DataSize.ofBytes(10_000),
                Optional.empty(),
                generalPool,
                new TestingGcMonitor(),
                TEST_EXECUTOR,
                TEST_EXECUTOR,
                DataSize.ofBytes(0),
                new SpillSpaceTracker(DataSize.ofBytes(0)));
    }
}
