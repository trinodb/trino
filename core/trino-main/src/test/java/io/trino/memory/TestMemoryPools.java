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
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.TestingPagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OutputFactory;
import io.trino.operator.TableScanOperator;
import io.trino.operator.TaskContext;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestMemoryPools
{
    private static final DataSize TEN_MEGABYTES = DataSize.of(10, MEGABYTE);
    private static final DataSize TEN_MEGABYTES_WITHOUT_TWO_BYTES = DataSize.ofBytes(TEN_MEGABYTES.toBytes() - 2);
    private static final DataSize ONE_BYTE = DataSize.ofBytes(1);

    private final TaskId fakeTaskId = new TaskId(new StageId(new QueryId("fake"), 0), 0, 0);
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
    private final ScheduledExecutorService scheduler = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    private record RevocableMemoryDriver(Driver driver, RevocableMemoryOperator operator) {}

    private RevocableMemoryDriver createRevocableMemoryDriver(MemoryPool userPool, DataSize reservedPerPage, long numberOfPages)
    {
        QueryContext queryContext = new QueryContext(new QueryId("query"),
                TEN_MEGABYTES,
                userPool,
                new TestingGcMonitor(),
                executor,
                scheduler,
                scheduler,
                TEN_MEGABYTES,
                new SpillSpaceTracker(DataSize.of(1, GIGABYTE)));

        TaskContext taskContext = createTaskContext(queryContext, executor, TEST_SESSION);
        DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
        OperatorContext revokableOperatorContext = driverContext.addOperatorContext(
                Integer.MAX_VALUE,
                new PlanNodeId("revokable_operator"),
                TableScanOperator.class.getSimpleName());

        OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> {}));
        Operator outputOperator = outputFactory.createOutputOperator(2, new PlanNodeId("output"), ImmutableList.of(), Function.identity(), new TestingPagesSerdeFactory()).createOperator(driverContext);
        RevocableMemoryOperator revocableMemoryOperator = new RevocableMemoryOperator(revokableOperatorContext, reservedPerPage, numberOfPages);

        Driver driver = Driver.createDriver(driverContext, revocableMemoryOperator, outputOperator);
        return new RevocableMemoryDriver(driver, revocableMemoryOperator);
    }

    @Test
    void testNotifyListenerOnMemoryReserved()
    {
        MemoryPool userPool = new MemoryPool(TEN_MEGABYTES);
        AtomicReference<MemoryPool> notifiedPool = new AtomicReference<>();
        AtomicLong notifiedBytes = new AtomicLong();
        userPool.addListener(MemoryPoolListener.onMemoryReserved(pool -> {
            notifiedPool.set(pool);
            notifiedBytes.set(pool.getReservedBytes());
        }));

        userPool.reserve(fakeTaskId, "test", 3);
        assertThat(notifiedPool.get()).isEqualTo(userPool);
        assertThat(notifiedBytes.get()).isEqualTo(3L);
    }

    @Test
    void testMemoryFutureCancellation()
    {
        MemoryPool userPool = new MemoryPool(DataSize.of(100, BYTE));
        TaskId reserveTaskId = new TaskId(new StageId(new QueryId("reserve"), 0), 0, 0);
        assertThat(userPool.reserve(reserveTaskId, "reserve", 10)).isDone();

        ListenableFuture<Void> future = userPool.reserve(fakeTaskId, "test", 95);
        assertThat(future.isDone()).isFalse();

        assertThatThrownBy(() -> future.cancel(true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("cancellation is not supported");
        assertThat(future.isDone()).isFalse();

        userPool.free(reserveTaskId, "reserve", 10);
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testBlockingOnRevocableMemoryFreeUser()
    {
        MemoryPool userPool = new MemoryPool(TEN_MEGABYTES);
        RevocableMemoryDriver revocableMemoryDriver = createRevocableMemoryDriver(userPool, ONE_BYTE, 10);
        assertThat(userPool.tryReserve(fakeTaskId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes())).isTrue();

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertThat(runDriverUntilBlocked(revocableMemoryDriver.driver(), waitingForRevocableMemory())).isEqualTo(2);
        assertThat(userPool.getFreeBytes() <= 0)
                .describedAs(format("Expected empty pool but got [%d]", userPool.getFreeBytes()))
                .isTrue();

        // lets free 5 bytes
        userPool.free(fakeTaskId, "test", 5);
        assertThat(runDriverUntilBlocked(revocableMemoryDriver.driver(), waitingForRevocableMemory())).isEqualTo(5);
        assertThat(userPool.getFreeBytes() <= 0)
                .describedAs(format("Expected empty pool but got [%d]", userPool.getFreeBytes()))
                .isTrue();

        // 3 more bytes is enough for driver to finish
        userPool.free(fakeTaskId, "test", 3);
        assertDriverProgress(revocableMemoryDriver.driver(), waitingForRevocableMemory());
        assertThat(userPool.getFreeBytes()).isEqualTo(10);
    }

    @Test
    void testBlockingOnRevocableMemoryFreeViaRevoke()
    {
        MemoryPool userPool = new MemoryPool(TEN_MEGABYTES);
        RevocableMemoryDriver revocableMemoryDriver = createRevocableMemoryDriver(userPool, ONE_BYTE, 5);
        assertThat(userPool.tryReserve(fakeTaskId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes())).isTrue();

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertThat(runDriverUntilBlocked(revocableMemoryDriver.driver(), waitingForRevocableMemory())).isEqualTo(2);
        revocableMemoryDriver.operator().getOperatorContext().requestMemoryRevoking();

        // 2 more iterations
        assertThat(runDriverUntilBlocked(revocableMemoryDriver.driver(), waitingForRevocableMemory())).isEqualTo(2);
        revocableMemoryDriver.operator().getOperatorContext().requestMemoryRevoking();

        // 3 more bytes is enough for driver to finish
        assertDriverProgress(revocableMemoryDriver.driver(), waitingForRevocableMemory());
        assertThat(userPool.getFreeBytes()).isEqualTo(2);
    }

    @Test
    void testTaggedAllocations()
    {
        TaskId testTask = new TaskId(new StageId(new QueryId("test_query"), 0), 0, 0);
        MemoryPool testPool = new MemoryPool(DataSize.ofBytes(1000));

        testPool.reserve(testTask, "test_tag", 10);

        Map<String, Long> allocations = testPool.getTaggedMemoryAllocations().get(new QueryId("test_query"));
        assertThat(allocations).isEqualTo(ImmutableMap.of("test_tag", 10L));

        // free 5 bytes for test_tag
        testPool.free(testTask, "test_tag", 5);
        assertThat(allocations).isEqualTo(ImmutableMap.of("test_tag", 5L));

        testPool.reserve(testTask, "test_tag2", 20);
        assertThat(allocations).isEqualTo(ImmutableMap.of("test_tag", 5L, "test_tag2", 20L));

        // free the remaining 5 bytes for test_tag
        testPool.free(testTask, "test_tag", 5);
        assertThat(allocations).isEqualTo(ImmutableMap.of("test_tag2", 20L));

        // free all for test_tag2
        testPool.free(testTask, "test_tag2", 20);
        assertThat(testPool.getTaggedMemoryAllocations().size()).isEqualTo(0);
    }

    @Test
    void testPerTaskAllocations()
    {
        QueryId query1 = new QueryId("test_query1");
        TaskId q1task1 = new TaskId(new StageId(query1, 0), 0, 0);
        TaskId q1task2 = new TaskId(new StageId(query1, 0), 1, 0);

        QueryId query2 = new QueryId("test_query2");
        TaskId q2task1 = new TaskId(new StageId(query2, 0), 0, 0);

        MemoryPool testPool = new MemoryPool(DataSize.ofBytes(1000));

        // allocate for some task for q1
        testPool.reserve(q1task1, "tag", 10);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(10L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(10L);

        // different task same for q1
        testPool.reserve(q1task2, "tag", 7);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(17L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(10L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);

        // task for a different query
        testPool.reserve(q2task1, "tag", 9);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(17L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(10L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);

        // increase memory for one of the tasks
        testPool.reserve(q1task1, "tag", 3);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(20L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(13L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);

        // decrease memory for one of the tasks
        testPool.free(q1task1, "tag", 5);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(15L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(8L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);

        // try to reserve more than allocated by task
        assertThatThrownBy(() -> testPool.free(q1task1, "tag", 9))
                .hasMessage("tried to free more memory than is reserved by task");
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(15L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(8L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);

        // zero memory for one of the tasks
        testPool.free(q1task1, "tag", 8);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(7L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(0L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);

        // zero memory for all query the tasks
        testPool.free(q1task2, "tag", 7);
        assertThat(testPool.getQueryMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryMemoryReservation(query1)).isEqualTo(0L);
        assertThat(testPool.getQueryMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getTaskMemoryReservation(q1task1)).isEqualTo(0L);
        assertThat(testPool.getTaskMemoryReservation(q1task2)).isEqualTo(0L);
        assertThat(testPool.getTaskMemoryReservation(q2task1)).isEqualTo(9L);
    }

    @Test
    void testGlobalRevocableAllocations()
    {
        MemoryPool testPool = new MemoryPool(DataSize.ofBytes(1000));

        assertThat(testPool.tryReserveRevocable(999)).isTrue();
        assertThat(testPool.tryReserveRevocable(2)).isFalse();
        assertThat(testPool.getReservedBytes()).isEqualTo(0);
        assertThat(testPool.getReservedRevocableBytes()).isEqualTo(999);
        assertThat(testPool.getTaskMemoryReservations()).isEmpty();
        assertThat(testPool.getQueryMemoryReservations()).isEmpty();
        assertThat(testPool.getTaggedMemoryAllocations()).isEmpty();

        // non-revocable allocation should block
        QueryId query = new QueryId("test_query1");
        TaskId task = new TaskId(new StageId(query, 0), 0, 0);
        ListenableFuture<Void> memoryFuture = testPool.reserve(task, "tag", 2);
        assertThat(memoryFuture).isNotDone();

        // non-revocable allocation should unblock after global revocable is freed
        testPool.freeRevocable(999);
        assertThat(memoryFuture).isDone();

        assertThat(testPool.getReservedBytes()).isEqualTo(2L);
        assertThat(testPool.getReservedRevocableBytes()).isEqualTo(0);
    }

    @Test
    void testPerTaskRevocableAllocations()
    {
        QueryId query1 = new QueryId("test_query1");
        TaskId q1task1 = new TaskId(new StageId(query1, 0), 0, 0);
        TaskId q1task2 = new TaskId(new StageId(query1, 0), 1, 0);

        QueryId query2 = new QueryId("test_query2");
        TaskId q2task1 = new TaskId(new StageId(query2, 0), 0, 0);

        MemoryPool testPool = new MemoryPool(DataSize.ofBytes(1000));

        // allocate for some task for q1
        testPool.reserveRevocable(q1task1, 10);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(10L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(10L);

        // different task same for q1
        testPool.reserveRevocable(q1task2, 7);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(17L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(10L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);

        // task for a different query
        testPool.reserveRevocable(q2task1, 9);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(17L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(10L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);

        // increase memory for one of the tasks
        testPool.reserveRevocable(q1task1, 3);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(20L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(13L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);

        // decrease memory for one of the tasks
        testPool.freeRevocable(q1task1, 5);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(15L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(8L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);

        // try to reserve more than allocated by task
        assertThatThrownBy(() -> testPool.freeRevocable(q1task1, 9))
                .hasMessage("tried to free more revocable memory than is reserved by task");
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(15L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(3);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(8L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);

        // zero memory for one of the tasks
        testPool.freeRevocable(q1task1, 8);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(7L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(2);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(0L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(7L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);

        // zero memory for all query the tasks
        testPool.freeRevocable(q1task2, 7);
        assertThat(testPool.getQueryRevocableMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getQueryRevocableMemoryReservation(query1)).isEqualTo(0L);
        assertThat(testPool.getQueryRevocableMemoryReservation(query2)).isEqualTo(9L);
        assertThat(testPool.getTaskRevocableMemoryReservations().keySet()).hasSize(1);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task1)).isEqualTo(0L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q1task2)).isEqualTo(0L);
        assertThat(testPool.getTaskRevocableMemoryReservation(q2task1)).isEqualTo(9L);
    }

    private static long runDriverUntilBlocked(Driver driver, Predicate<OperatorContext> reason)
    {
        long iterationsCount = 0;

        // run driver, until it blocks
        while (!isBlockedFor(driver, reason)) {
            driver.processForNumberOfIterations(1);
            iterationsCount++;
        }

        // driver should be blocked waiting for memory
        assertThat(driver.isFinished()).isFalse();
        return iterationsCount;
    }

    private static void assertDriverProgress(Driver driver, Predicate<OperatorContext> reason)
    {
        do {
            assertThat(isBlockedFor(driver, reason)).isFalse();
            // query should not block
            assertThat(driver.processUntilBlocked()).isDone();
        }
        while (!driver.isFinished());
    }

    private static Predicate<OperatorContext> waitingForRevocableMemory()
    {
        return (OperatorContext operatorContext) ->
                !operatorContext.isWaitingForRevocableMemory().isDone() &&
                        !operatorContext.isMemoryRevokingRequested();
    }

    private static boolean isBlockedFor(Driver driver, Predicate<OperatorContext> reason)
    {
        for (OperatorContext operatorContext : driver.getDriverContext().getOperatorContexts()) {
            if (reason.test(operatorContext)) {
                return true;
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
