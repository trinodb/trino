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
import io.airlift.slice.Slice;
import io.trino.RowPagesBuilder;
import io.trino.Session;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class BaseTableWriterOperatorTest
{
    protected static final TestingAggregationFunction LONG_MAX = new TestingFunctionResolution().getAggregateFunction(QualifiedName.of("max"), fromTypes(BIGINT));

    protected ExecutorService executor;
    protected ScheduledExecutorService scheduledExecutor;

    @BeforeClass(alwaysRun = true)
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testBlockedPageSink()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(BlockingPageSink::new);
        AbstractTableWriterOperator operator = createTableWriterOperator(testPageSinkProvider);

        // initial state validation
        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // blockingPageSink that will return blocked future
        operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(5).row(5).build().get(0));

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());
        assertNull(operator.getOutput());

        // complete previously blocked future
        testPageSinkProvider.getPageSinks().forEach(pageSink -> ((BlockingPageSink) pageSink).complete());

        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // add second page
        operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(5).row(5).build().get(0));

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());

        // finish operator, state hasn't changed
        operator.finish();

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());

        // complete previously blocked future
        testPageSinkProvider.getPageSinks().forEach(pageSink -> ((BlockingPageSink) pageSink).complete());

        // and getOutput which actually finishes the operator
        List<Type> expectedTypes = ImmutableList.of(BIGINT, VARBINARY);
        assertPageEquals(expectedTypes,
                operator.getOutput(),
                rowPagesBuilder(expectedTypes).row(8, null).build().get(0));

        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.isFinished());
        assertFalse(operator.needsInput());

        operator.close();
    }

    @Test
    public void addInputFailsOnBlockedOperator()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(BlockingPageSink::new);
        AbstractTableWriterOperator operator = createTableWriterOperator(testPageSinkProvider);

        operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(5).row(5).build().get(0));

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.needsInput());

        assertThatThrownBy(() -> operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(5).row(5).build().get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operator does not need input");

        operator.close();
    }

    @Test
    public void testTableWriterInfo()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(TestPageSink::new);
        AbstractTableWriterOperator tableWriterOperator = createTableWriterOperator(testPageSinkProvider);

        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
        IntStream.range(0, 100).forEach(i -> {
            IntStream.range(0, 50).forEach(j -> rowPagesBuilder.row(i));
            IntStream.range(0, 50).forEach(j -> rowPagesBuilder.row(50 + i));
            rowPagesBuilder.pageBreak();
        });
        List<Page> pages = rowPagesBuilder.build();

        long validationCpuNanos = 0;
        for (Page page : pages) {
            validationCpuNanos += page.getPositionCount();
            tableWriterOperator.addInput(page);
            long expectedPeakMemoryUsage = testPageSinkProvider.getPageSinks().stream()
                    .mapToLong(ConnectorPageSink::getMemoryUsage)
                    .sum();
            AbstractTableWriterOperator.TableWriterInfo info = tableWriterOperator.getInfo();
            assertEquals(info.getPageSinkPeakMemoryUsage(), expectedPeakMemoryUsage);
            assertEquals((long) (info.getValidationCpuTime().getValue(NANOSECONDS)), validationCpuNanos);
        }

        tableWriterOperator.close();
    }

    @Test
    public void testStatisticsAggregation()
            throws Exception
    {
        TestPageSinkProvider testPageSinkProvider = new TestPageSinkProvider(TestPageSink::new);
        ImmutableList<Type> outputTypes = ImmutableList.of(BIGINT, VARBINARY, BIGINT);
        Session session = testSessionBuilder()
                .setSystemProperty("statistics_cpu_timer_enabled", "true")
                .build();
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        AbstractTableWriterOperator operator = createTableWriterOperator(
                session,
                driverContext,
                testPageSinkProvider,
                new AggregationOperator.AggregationOperatorFactory(
                        1,
                        new PlanNodeId("test"),
                        ImmutableList.of(LONG_MAX.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()))),
                outputTypes);

        operator.addInput(rowPagesBuilder(BIGINT).row(1).row(1).row(1).row(5).row(5).build().get(0));
        operator.addInput(rowPagesBuilder(BIGINT).row(8).row(8).row(9).row(8).row(8).build().get(0));

        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.needsInput());

        assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

        operator.finish();
        assertFalse(operator.isFinished());

        assertPageEquals(outputTypes, operator.getOutput(),
                rowPagesBuilder(outputTypes)
                        .row(null, null, 9).build().get(0));

        assertPageEquals(outputTypes, operator.getOutput(),
                rowPagesBuilder(outputTypes)
                        .row(10, null, null).build().get(0));

        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.needsInput());
        assertTrue(operator.isFinished());

        operator.close();
        assertMemoryIsReleased(operator);

        AbstractTableWriterOperator.TableWriterInfo info = operator.getInfo();
        assertThat(info.getStatisticsWallTime().getValue(NANOSECONDS)).isGreaterThan(0);
        assertThat(info.getStatisticsCpuTime().getValue(NANOSECONDS)).isGreaterThan(0);
    }

    private void assertMemoryIsReleased(AbstractTableWriterOperator tableWriterOperator)
    {
        OperatorContext tableWriterOperatorOperatorContext = tableWriterOperator.getOperatorContext();
        MemoryTrackingContext tableWriterMemoryContext = tableWriterOperatorOperatorContext.getOperatorMemoryContext();
        assertEquals(tableWriterMemoryContext.getUserMemory(), 0);
        assertEquals(tableWriterMemoryContext.getRevocableMemory(), 0);

        Operator statisticAggregationOperator = tableWriterOperator.getStatisticAggregationOperator();
        assertTrue(statisticAggregationOperator instanceof AggregationOperator);
        AggregationOperator aggregationOperator = (AggregationOperator) statisticAggregationOperator;
        OperatorContext aggregationOperatorOperatorContext = aggregationOperator.getOperatorContext();
        MemoryTrackingContext aggregationOperatorMemoryContext = aggregationOperatorOperatorContext.getOperatorMemoryContext();
        assertEquals(aggregationOperatorMemoryContext.getUserMemory(), 0);
        assertEquals(aggregationOperatorMemoryContext.getRevocableMemory(), 0);
    }

    protected AbstractTableWriterOperator createTableWriterOperator(TestPageSinkProvider pageSinkProvider)
    {
        Session session = TEST_SESSION;
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        return createTableWriterOperator(
                session,
                driverContext,
                pageSinkProvider,
                new DevNullOperator.DevNullOperatorFactory(1, new PlanNodeId("test")),
                ImmutableList.of(BIGINT, VARBINARY));
    }

    protected abstract AbstractTableWriterOperator createTableWriterOperator(
            Session session,
            DriverContext driverContext,
            TestPageSinkProvider pageSinkProvider,
            OperatorFactory statisticsAggregation,
            List<Type> outputTypes);

    protected static class TestPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        private final Supplier<ConnectorPageSink> pageSinkSupplier;
        private final List<ConnectorPageSink> pageSinks = new ArrayList<>();

        protected TestPageSinkProvider(Supplier<ConnectorPageSink> pageSink)
        {
            this.pageSinkSupplier = requireNonNull(pageSink, "pageSink is null");
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            ConnectorPageSink pageSink = pageSinkSupplier.get();
            pageSinks.add(pageSink);
            return pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            ConnectorPageSink pageSink = pageSinkSupplier.get();
            pageSinks.add(pageSink);
            return pageSink;
        }

        public List<ConnectorPageSink> getPageSinks()
        {
            return pageSinks;
        }
    }

    private static class BlockingPageSink
            implements ConnectorPageSink
    {
        private CompletableFuture<?> future = new CompletableFuture<>();
        private CompletableFuture<Collection<Slice>> finishFuture = new CompletableFuture<>();

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            future = new CompletableFuture<>();
            return future;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            finishFuture = new CompletableFuture<>();
            return finishFuture;
        }

        @Override
        public void abort() {}

        void complete()
        {
            future.complete(null);
            finishFuture.complete(ImmutableList.of());
        }
    }

    protected static class TestPageSink
            implements ConnectorPageSink
    {
        private final List<Page> pages = new ArrayList<>();

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            // Compact the input page avoid over-retaining memory
            page.compact();

            pages.add(page);
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of());
        }

        @Override
        public long getMemoryUsage()
        {
            long memoryUsage = 0;
            for (Page page : pages) {
                memoryUsage += page.getRetainedSizeInBytes();
            }
            return memoryUsage;
        }

        @Override
        public long getValidationCpuNanos()
        {
            long validationCpuNanos = 0;
            for (Page page : pages) {
                validationCpuNanos += page.getPositionCount();
            }
            return validationCpuNanos;
        }

        @Override
        public long getCompletedBytes()
        {
            long physicalWrittenBytes = 0;
            for (Page page : pages) {
                physicalWrittenBytes += page.getSizeInBytes();
            }
            return physicalWrittenBytes;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        @Override
        public void abort() {}
    }
}
