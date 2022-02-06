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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.Split;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.TableScanOperator;
import io.trino.operator.TaskContext;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryBlocking
{
    private static final QueryId QUERY_ID = new QueryId("test_query");
    private static final TaskId TASK_ID = new TaskId(new StageId(QUERY_ID, 0), 0, 0);

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private MemoryPool memoryPool;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(DataSize.valueOf("100MB"))
                .setMemoryPoolSize(DataSize.valueOf("10B"))
                .setQueryId(QUERY_ID)
                .build();
        memoryPool = taskContext.getQueryContext().getMemoryPool();
        driverContext = taskContext
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testTableScanMemoryBlocking()
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        List<Type> types = ImmutableList.of(VARCHAR);
        TableScanOperator source = new TableScanOperator(driverContext.addOperatorContext(1, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 1)
                        .addSequencePage(10, 1)
                        .addSequencePage(10, 1)
                        .addSequencePage(10, 1)
                        .addSequencePage(10, 1)
                        .build()),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY);
        PageConsumerOperator sink = createSinkOperator(types);
        Driver driver = Driver.createDriver(driverContext, source, sink);
        assertSame(driver.getDriverContext(), driverContext);
        assertFalse(driver.isFinished());
        Split testSplit = new Split(TEST_CATALOG_HANDLE, new TestSplit());
        driver.updateSplitAssignment(new SplitAssignment(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, testSplit)), true));

        ListenableFuture<Void> blocked = driver.processForDuration(new Duration(1, NANOSECONDS));

        // the driver shouldn't block in the first call as it will be able to move a page between source and the sink operator
        // but the operator should be blocked
        assertTrue(blocked.isDone());
        assertFalse(source.getOperatorContext().isWaitingForMemory().isDone());

        // in the subsequent calls both the driver and the operator should be blocked
        // and they should stay blocked until more memory becomes available
        for (int i = 0; i < 10; i++) {
            blocked = driver.processForDuration(new Duration(1, NANOSECONDS));
            assertFalse(blocked.isDone());
            assertFalse(source.getOperatorContext().isWaitingForMemory().isDone());
        }

        // free up some memory
        memoryPool.free(TASK_ID, "test", memoryPool.getReservedBytes());

        // the operator should be unblocked
        assertTrue(source.getOperatorContext().isWaitingForMemory().isDone());

        // the driver shouldn't be blocked
        blocked = driver.processForDuration(new Duration(1, NANOSECONDS));
        assertTrue(blocked.isDone());
    }

    private PageConsumerOperator createSinkOperator(List<Type> types)
    {
        // materialize the output to catch some type errors
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(driverContext.getSession(), types);
        return new PageConsumerOperator(driverContext.addOperatorContext(2, new PlanNodeId("test"), "sink"), resultBuilder::page, Function.identity());
    }

    private static class TestSplit
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return null;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return 0;
        }
    }
}
