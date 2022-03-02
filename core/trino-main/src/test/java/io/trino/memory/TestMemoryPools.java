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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.buffer.TestingPagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OutputFactory;
import io.trino.operator.TableScanOperator;
import io.trino.operator.TaskContext;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryPools
{
    private static final DataSize TEN_MEGABYTES = DataSize.of(10, MEGABYTE);
    private static final DataSize TEN_MEGABYTES_WITHOUT_TWO_BYTES = DataSize.ofBytes(TEN_MEGABYTES.toBytes() - 2);
    private static final DataSize ONE_BYTE = DataSize.ofBytes(1);

    private QueryId fakeQueryId;
    private LocalQueryRunner localQueryRunner;
    private MemoryPool userPool;
    private List<Driver> drivers;
    private TaskContext taskContext;

    private void setUp(Supplier<List<Driver>> driversSupplier)
    {
        checkState(localQueryRunner == null, "Already set up");

        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("task_default_concurrency", "1")
                .build();

        localQueryRunner = LocalQueryRunner.builder(session)
                .withInitialTransaction()
                .build();

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        userPool = new MemoryPool(TEN_MEGABYTES);
        fakeQueryId = new QueryId("fake");
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(DataSize.of(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(new QueryId("query"),
                TEN_MEGABYTES,
                userPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                TEN_MEGABYTES,
                spillSpaceTracker);
        taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), localQueryRunner.getDefaultSession());
        drivers = driversSupplier.get();
    }

    private void setUpCountStarFromOrdersWithJoin()
    {
        // query will reserve all memory in the user pool and discard the output
        setUp(() -> {
            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> {}));
            return localQueryRunner.createDrivers("SELECT COUNT(*) FROM orders JOIN lineitem ON CAST(orders.orderkey AS VARCHAR) = CAST(lineitem.orderkey AS VARCHAR)", outputFactory, taskContext);
        });
    }

    private RevocableMemoryOperator setupConsumeRevocableMemory(DataSize reservedPerPage, long numberOfPages)
    {
        AtomicReference<RevocableMemoryOperator> createOperator = new AtomicReference<>();
        setUp(() -> {
            DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
            OperatorContext revokableOperatorContext = driverContext.addOperatorContext(
                    Integer.MAX_VALUE,
                    new PlanNodeId("revokable_operator"),
                    TableScanOperator.class.getSimpleName());

            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> {}));
            Operator outputOperator = outputFactory.createOutputOperator(2, new PlanNodeId("output"), ImmutableList.of(), Function.identity(), new TestingPagesSerdeFactory()).createOperator(driverContext);
            RevocableMemoryOperator revocableMemoryOperator = new RevocableMemoryOperator(revokableOperatorContext, reservedPerPage, numberOfPages);
            createOperator.set(revocableMemoryOperator);

            Driver driver = Driver.createDriver(driverContext, revocableMemoryOperator, outputOperator);
            return ImmutableList.of(driver);
        });
        return createOperator.get();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (localQueryRunner != null) {
            localQueryRunner.close();
            localQueryRunner = null;
        }
    }

    @Test
    public void testBlockingOnUserMemory()
    {
        setUpCountStarFromOrdersWithJoin();
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES.toBytes()));
        runDriversUntilBlocked(waitingForUserMemory());
        assertTrue(userPool.getFreeBytes() <= 0, format("Expected empty pool but got [%d]", userPool.getFreeBytes()));
        userPool.free(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertDriversProgress(waitingForUserMemory());
    }

    @Test
    public void testNotifyListenerOnMemoryReserved()
    {
        setupConsumeRevocableMemory(ONE_BYTE, 10);
        AtomicReference<MemoryPool> notifiedPool = new AtomicReference<>();
        AtomicLong notifiedBytes = new AtomicLong();
        userPool.addListener(MemoryPoolListener.onMemoryReserved(pool -> {
            notifiedPool.set(pool);
            notifiedBytes.set(pool.getReservedBytes());
        }));

        userPool.reserve(fakeQueryId, "test", 3);
        assertEquals(notifiedPool.get(), userPool);
        assertEquals(notifiedBytes.get(), 3L);
    }

    @Test
    public void testMemoryFutureCancellation()
    {
        setUpCountStarFromOrdersWithJoin();
        ListenableFuture<Void> future = userPool.reserve(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertTrue(!future.isDone());
        assertThatThrownBy(() -> future.cancel(true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("cancellation is not supported");
        userPool.free(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertTrue(future.isDone());
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeUser()
    {
        setupConsumeRevocableMemory(ONE_BYTE, 10);
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableMemory()), 2);
        assertTrue(userPool.getFreeBytes() <= 0, format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // lets free 5 bytes
        userPool.free(fakeQueryId, "test", 5);
        assertEquals(runDriversUntilBlocked(waitingForRevocableMemory()), 5);
        assertTrue(userPool.getFreeBytes() <= 0, format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // 3 more bytes is enough for driver to finish
        userPool.free(fakeQueryId, "test", 3);
        assertDriversProgress(waitingForRevocableMemory());
        assertEquals(userPool.getFreeBytes(), 10);
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeViaRevoke()
    {
        RevocableMemoryOperator revocableMemoryOperator = setupConsumeRevocableMemory(ONE_BYTE, 5);
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestMemoryRevoking();

        // 2 more iterations
        assertEquals(runDriversUntilBlocked(waitingForRevocableMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestMemoryRevoking();

        // 3 more bytes is enough for driver to finish
        assertDriversProgress(waitingForRevocableMemory());
        assertEquals(userPool.getFreeBytes(), 2);
    }

    @Test
    public void testTaggedAllocations()
    {
        QueryId testQuery = new QueryId("test_query");
        MemoryPool testPool = new MemoryPool(DataSize.ofBytes(1000));

        testPool.reserve(testQuery, "test_tag", 10);

        Map<String, Long> allocations = testPool.getTaggedMemoryAllocations().get(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 10L));

        // free 5 bytes for test_tag
        testPool.free(testQuery, "test_tag", 5);
        assertEquals(allocations, ImmutableMap.of("test_tag", 5L));

        testPool.reserve(testQuery, "test_tag2", 20);
        assertEquals(allocations, ImmutableMap.of("test_tag", 5L, "test_tag2", 20L));

        // free the remaining 5 bytes for test_tag
        testPool.free(testQuery, "test_tag", 5);
        assertEquals(allocations, ImmutableMap.of("test_tag2", 20L));

        // free all for test_tag2
        testPool.free(testQuery, "test_tag2", 20);
        assertEquals(testPool.getTaggedMemoryAllocations().size(), 0);
    }

    private long runDriversUntilBlocked(Predicate<OperatorContext> reason)
    {
        long iterationsCount = 0;

        // run driver, until it blocks
        while (!isOperatorBlocked(drivers, reason)) {
            for (Driver driver : drivers) {
                driver.process();
            }
            iterationsCount++;
        }

        // driver should be blocked waiting for memory
        for (Driver driver : drivers) {
            assertFalse(driver.isFinished());
        }
        return iterationsCount;
    }

    private void assertDriversProgress(Predicate<OperatorContext> reason)
    {
        do {
            assertFalse(isOperatorBlocked(drivers, reason));
            boolean progress = false;
            for (Driver driver : drivers) {
                ListenableFuture<Void> blocked = driver.process();
                progress = progress | blocked.isDone();
            }
            // query should not block
            assertTrue(progress);
        }
        while (!drivers.stream().allMatch(Driver::isFinished));
    }

    private Predicate<OperatorContext> waitingForUserMemory()
    {
        return (OperatorContext operatorContext) -> !operatorContext.isWaitingForMemory().isDone();
    }

    private Predicate<OperatorContext> waitingForRevocableMemory()
    {
        return (OperatorContext operatorContext) ->
                !operatorContext.isWaitingForRevocableMemory().isDone() &&
                        !operatorContext.isMemoryRevokingRequested();
    }

    private static boolean isOperatorBlocked(List<Driver> drivers, Predicate<OperatorContext> reason)
    {
        for (Driver driver : drivers) {
            for (OperatorContext operatorContext : driver.getDriverContext().getOperatorContexts()) {
                if (reason.test(operatorContext)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static class RevocableMemoryOperator
            implements Operator
    {
        private final DataSize reservedPerPage;
        private final long numberOfPages;
        private final OperatorContext operatorContext;
        private long producedPagesCount;
        private final LocalMemoryContext revocableMemoryContext;

        public RevocableMemoryOperator(OperatorContext operatorContext, DataSize reservedPerPage, long numberOfPages)
        {
            this.operatorContext = operatorContext;
            this.reservedPerPage = reservedPerPage;
            this.numberOfPages = numberOfPages;
            this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        }

        @Override
        public ListenableFuture<Void> startMemoryRevoke()
        {
            return immediateVoidFuture();
        }

        @Override
        public void finishMemoryRevoke()
        {
            revocableMemoryContext.setBytes(0);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            revocableMemoryContext.setBytes(0);
        }

        @Override
        public boolean isFinished()
        {
            return producedPagesCount >= numberOfPages;
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getOutput()
        {
            revocableMemoryContext.setBytes(revocableMemoryContext.getBytes() + reservedPerPage.toBytes());
            producedPagesCount++;
            if (producedPagesCount == numberOfPages) {
                finish();
            }
            return new Page(10);
        }
    }
}
