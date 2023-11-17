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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.Duration;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.Type;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PageConsumerOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestDriver
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNormalFinish()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator sink = createSinkOperator(types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        assertThat(driver.isFinished()).isFalse();
        ListenableFuture<Void> blocked = driver.processForDuration(new Duration(1, TimeUnit.SECONDS));
        assertThat(blocked.isDone()).isTrue();
        assertThat(driver.isFinished()).isTrue();

        assertThat(sink.isFinished()).isTrue();
        assertThat(source.isFinished()).isTrue();
    }

    // The race can be reproduced somewhat reliably when the invocationCount is 10K, but we use 1K iterations to cap the test runtime.
    @RepeatedTest(1000)
    @Timeout(10)
    public void testConcurrentClose()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "values");
        ValuesOperator source = new ValuesOperator(operatorContext, rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator sink = createSinkOperator(types);
        Driver driver = Driver.createDriver(driverContext, source, sink);
        // let these threads race
        scheduledExecutor.submit(() -> driver.processForDuration(new Duration(1, TimeUnit.NANOSECONDS))); // don't want to call isFinishedInternal in processFor
        scheduledExecutor.submit(driver::close);
        while (!driverContext.isTerminatingOrDone()) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testAbruptFinish()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        PageConsumerOperator sink = createSinkOperator(types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        assertThat(driver.isFinished()).isFalse();
        driver.close();
        assertThat(driver.isFinished()).isTrue();

        // finish is only called in normal operations
        assertThat(source.isFinished()).isFalse();
        assertThat(sink.isFinished()).isFalse();

        // close is always called (values operator doesn't have a closed state)
        assertThat(sink.isClosed()).isTrue();
    }

    @Test
    public void testAddSourceFinish()
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new TableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY);

        PageConsumerOperator sink = createSinkOperator(types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        assertThat(driver.isFinished()).isFalse();
        assertThat(driver.processForDuration(new Duration(1, TimeUnit.MILLISECONDS)).isDone()).isFalse();
        assertThat(driver.isFinished()).isFalse();

        driver.updateSplitAssignment(new SplitAssignment(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertThat(driver.isFinished()).isFalse();
        assertThat(driver.processForDuration(new Duration(1, TimeUnit.SECONDS)).isDone()).isTrue();
        assertThat(driver.isFinished()).isTrue();

        assertThat(sink.isFinished()).isTrue();
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    public void testBrokenOperatorCloseWhileProcessing()
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), false);
        Driver driver = Driver.createDriver(driverContext, brokenOperator, createSinkOperator(ImmutableList.of()));

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(() -> driver.processForDuration(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        brokenOperator.waitForLocked();

        driver.close();
        assertThat(driver.isFinished()).isTrue();

        assertThatThrownBy(() -> driverProcessFor.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(new TrinoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted"));

        assertThat(driver.getDestroyedFuture().isDone()).isTrue();
    }

    @Test
    public void testBrokenOperatorProcessWhileClosing()
            throws Exception
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), true);
        Driver driver = Driver.createDriver(driverContext, brokenOperator, createSinkOperator(ImmutableList.of()));

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        // block thread in operator close
        Future<Boolean> driverClose = executor.submit(() -> {
            driver.close();
            return true;
        });
        brokenOperator.waitForLocked();

        assertThat(driver.processForDuration(new Duration(1, TimeUnit.MILLISECONDS)).isDone()).isTrue();
        assertThat(driver.isFinished()).isTrue();
        assertThat(driver.getDestroyedFuture().isDone()).isFalse();

        brokenOperator.unlock();

        assertThat(driverClose.get()).isTrue();
        assertThat(driver.getDestroyedFuture().isDone()).isTrue();
    }

    @Test
    public void testMemoryRevocationRace()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new AlwaysBlockedMemoryRevokingTableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "scan"),
                new PlanNodeId("source"),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TEST_TABLE_HANDLE,
                ImmutableList.of());

        Driver driver = Driver.createDriver(driverContext, source, createSinkOperator(types));
        // the table scan operator will request memory revocation with requestMemoryRevoking()
        // while the driver is still not done with the processFor() method and before it moves to
        // updateDriverBlockedFuture() method.
        assertThat(driver.processForDuration(new Duration(100, TimeUnit.MILLISECONDS)).isDone()).isTrue();
    }

    @Test
    public void testUnblocksOnFinish()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new AlwaysBlockedTableScanOperator(
                driverContext.addOperatorContext(99, new PlanNodeId("test"), "scan"),
                new PlanNodeId("source"),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TEST_TABLE_HANDLE,
                ImmutableList.of());

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(driverContext.getSession(), types);
        BlockedSinkOperator sink = new BlockedSinkOperator(driverContext.addOperatorContext(1, new PlanNodeId("test"), "sink"), resultBuilder::page, Function.identity());
        Driver driver = Driver.createDriver(driverContext, source, sink);

        ListenableFuture<Void> blocked = driver.processForDuration(new Duration(100, TimeUnit.MILLISECONDS));
        assertThat(blocked.isDone()).isFalse();

        sink.setFinished();
        assertThat(blocked.isDone()).isTrue();
    }

    @Test
    public void testBrokenOperatorAddSource()
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        // create a table scan operator that does not block, which will cause the driver loop to busy wait
        TableScanOperator source = new NotBlockedTableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TEST_TABLE_HANDLE,
                ImmutableList.of());

        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"));
        Driver driver = Driver.createDriver(driverContext, source, brokenOperator);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(() -> driver.processForDuration(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        brokenOperator.waitForLocked();

        assertThat(driver.getDriverContext()).isSameAs(driverContext);

        assertThat(driver.isFinished()).isFalse();
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertThat(driver.processForDuration(new Duration(1, TimeUnit.MILLISECONDS)).isDone()).isTrue();
        assertThat(driver.isFinished()).isFalse();

        driver.updateSplitAssignment(new SplitAssignment(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertThat(driver.getDestroyedFuture().isDone()).isFalse();
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertThat(driver.processForDuration(new Duration(1, TimeUnit.SECONDS)).isDone()).isTrue();
        assertThat(driver.isFinished()).isFalse();

        driver.close();
        assertThat(driver.isFinished()).isTrue();

        assertThatThrownBy(() -> driverProcessFor.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(new TrinoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted"));

        assertThat(driver.getDestroyedFuture().isDone()).isTrue();
    }

    private static Split newMockSplit()
    {
        return new Split(TEST_CATALOG_HANDLE, new MockSplit());
    }

    private PageConsumerOperator createSinkOperator(List<Type> types)
    {
        // materialize the output to catch some type errors
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(driverContext.getSession(), types);
        return new PageConsumerOperator(driverContext.addOperatorContext(1, new PlanNodeId("test"), "sink"), resultBuilder::page, Function.identity());
    }

    private static class BrokenOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final ReentrantLock lock = new ReentrantLock();
        private final CountDownLatch lockedLatch = new CountDownLatch(1);
        private final CountDownLatch unlockLatch = new CountDownLatch(1);
        private final boolean lockForClose;

        private BrokenOperator(OperatorContext operatorContext)
        {
            this(operatorContext, false);
        }

        private BrokenOperator(OperatorContext operatorContext, boolean lockForClose)
        {
            this.operatorContext = operatorContext;
            this.lockForClose = lockForClose;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        public void unlock()
        {
            unlockLatch.countDown();
        }

        private void waitForLocked()
        {
            try {
                assertThat(lockedLatch.await(10, TimeUnit.SECONDS)).isTrue();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        private void waitForUnlock()
        {
            try {
                assertThat(lock.tryLock(1, TimeUnit.SECONDS)).isTrue();
                try {
                    lockedLatch.countDown();
                    assertThat(unlockLatch.await(5, TimeUnit.SECONDS)).isTrue();
                }
                finally {
                    lock.unlock();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        @Override
        public void finish()
        {
            waitForUnlock();
        }

        @Override
        public boolean isFinished()
        {
            waitForUnlock();
            return true;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            waitForUnlock();
            return NOT_BLOCKED;
        }

        @Override
        public boolean needsInput()
        {
            waitForUnlock();
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            waitForUnlock();
        }

        @Override
        public Page getOutput()
        {
            waitForUnlock();
            return null;
        }

        @Override
        public void close()
        {
            if (lockForClose) {
                waitForUnlock();
            }
        }
    }

    private static class BlockedSinkOperator
            extends PageConsumerOperator
    {
        private final SettableFuture<Void> finished = SettableFuture.create();

        public BlockedSinkOperator(
                OperatorContext operatorContext,
                Consumer<Page> pageConsumer,
                Function<Page, Page> pagePreprocessor)
        {
            super(operatorContext, pageConsumer, pagePreprocessor);
            operatorContext.setFinishedFuture(finished);
        }

        @Override
        public boolean isFinished()
        {
            return finished.isDone();
        }

        void setFinished()
        {
            finished.set(null);
        }
    }

    private static class AlwaysBlockedTableScanOperator
            extends TableScanOperator
    {
        public AlwaysBlockedTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, table, columns, DynamicFilter.EMPTY);
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return SettableFuture.create();
        }
    }

    private static class AlwaysBlockedMemoryRevokingTableScanOperator
            extends TableScanOperator
    {
        public AlwaysBlockedMemoryRevokingTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, table, columns, DynamicFilter.EMPTY);
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            // this operator is always blocked and when queried by the driver
            // it triggers memory revocation so that the driver gets unblocked
            LocalMemoryContext revocableMemoryContext = getOperatorContext().localRevocableMemoryContext();
            revocableMemoryContext.setBytes(100);
            getOperatorContext().requestMemoryRevoking();
            return SettableFuture.create();
        }
    }

    private static class NotBlockedTableScanOperator
            extends TableScanOperator
    {
        public NotBlockedTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, table, columns, DynamicFilter.EMPTY);
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return NOT_BLOCKED;
        }
    }

    private static class MockSplit
            implements ConnectorSplit
    {
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
