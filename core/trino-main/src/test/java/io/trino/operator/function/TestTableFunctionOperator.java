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
package io.trino.operator.function;

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.PagesIndex;
import io.trino.operator.function.TableFunctionOperator.TableFunctionOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTableFunctionOperator
{
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testDataProcessorClosedForEachPartition()
            throws Exception
    {
        List<TestingDataProcessor> processors = new ArrayList<>();
        Operator operator = createOperator(() -> {
            TestingDataProcessor processor = new TestingDataProcessor(false);
            processors.add(processor);
            return processor;
        });

        // two partitions
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(1L)
                .row(2L)
                .row(2L)
                .build();
        assertThat(toPages(operator, input.iterator())).isEmpty();
        assertThat(processors).hasSize(2);
        assertThat(processors.get(0).closeCount).isEqualTo(1);
        assertThat(processors.get(1).closeCount).isEqualTo(0);

        operator.close();
        assertThat(processors.get(0).closeCount).isEqualTo(1);
        assertThat(processors.get(1).closeCount).isEqualTo(1);
    }

    @Test
    public void testDataProcessorClosedOnceWhenNextProcessorCreationFails()
            throws Exception
    {
        List<TestingDataProcessor> processors = new ArrayList<>();
        Operator operator = createOperator(() -> {
            if (!processors.isEmpty()) {
                throw new IllegalStateException("cannot create processor");
            }
            TestingDataProcessor processor = new TestingDataProcessor(false);
            processors.add(processor);
            return processor;
        });

        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(2L)
                .build();
        assertThatThrownBy(() -> toPages(operator, input.iterator()))
                .hasMessage("cannot create processor");

        operator.close();
        assertThat(processors.getFirst().closeCount).isEqualTo(1);
    }

    @Test
    public void testDataProcessorClosedWhenOperatorClosedBeforeFinish()
            throws Exception
    {
        List<TestingDataProcessor> processors = new ArrayList<>();
        Operator operator = createOperator(() -> {
            TestingDataProcessor processor = new TestingDataProcessor(true);
            processors.add(processor);
            return processor;
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(1L)
                .buildPage());
        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isFinished()).isFalse();
        assertThat(processors).hasSize(1);
        assertThat(processors.getFirst().closeCount).isEqualTo(0);

        operator.close();
        assertThat(processors.getFirst().closeCount).isEqualTo(1);
    }

    @Test
    public void testDataProcessorMemoryAccounted()
            throws Exception
    {
        List<MemoryContext> memoryContexts = new ArrayList<>();
        Operator operator = createOperator(memoryContext -> {
            memoryContexts.add(memoryContext);
            return new TestingDataProcessor(true);
        });
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(1L)
                .buildPage());
        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(memoryContexts).hasSize(1);

        // the operator also accounts for the buffered input
        long memoryUsage = driverContext.getMemoryUsage();
        memoryContexts.get(0).setBytes(1024);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(memoryUsage + 1024);

        operator.close();
        assertThat(driverContext.getMemoryUsage()).isEqualTo(memoryUsage);
    }

    @Test
    public void testDataProcessorMemoryReleasedWithPartition()
            throws Exception
    {
        List<MemoryContext> memoryContexts = new ArrayList<>();
        Operator operator = createOperator(memoryContext -> {
            // the processor of the second partition does not report memory
            if (memoryContexts.isEmpty()) {
                memoryContext.setBytes(1024);
            }
            memoryContexts.add(memoryContext);
            return new TestingDataProcessor(false);
        });
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        // two partitions
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(2L)
                .build();
        assertThat(toPages(operator, input.iterator())).isEmpty();
        assertThat(memoryContexts).hasSize(2);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);

        operator.close();
    }

    @Test
    public void testStreamingOfSourceWithRowSemantics()
            throws Exception
    {
        List<PassThroughProcessor> processors = new ArrayList<>();
        Operator operator = createStreamingOperator(() -> {
            PassThroughProcessor processor = new PassThroughProcessor(true);
            processors.add(processor);
            return processor;
        });

        // output is produced for each input page, before the input is finished
        Page firstPage = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .buildPage();
        operator.addInput(firstPage);
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), firstPage);

        // pass-through indexes of the second page are counted from the beginning of the input
        Page secondPage = rowPagesBuilder(BIGINT)
                .row(12L)
                .row(13L)
                .row(14L)
                .buildPage();
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(secondPage);
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), secondPage);

        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isFinished()).isTrue();

        // a single processor is used for the whole input
        assertThat(processors).hasSize(1);
        assertThat(processors.get(0).closeCount).isEqualTo(0);
        operator.close();
        assertThat(processors.get(0).closeCount).isEqualTo(1);
    }

    @Test
    public void testStreamingPassThroughIndexOutsideOfCurrentPage()
            throws Exception
    {
        Operator operator = createStreamingOperator(() -> new PassThroughProcessor(false));

        Page firstPage = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .buildPage();
        operator.addInput(firstPage);
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), firstPage);

        // the processor restarts indexes for the second page, so they refer to the first page
        operator.addInput(rowPagesBuilder(BIGINT)
                .row(12L)
                .row(13L)
                .buildPage());
        assertThatThrownBy(operator::getOutput)
                .isInstanceOf(TrinoException.class)
                .hasMessage("Index of a pass-through row: 0 out of the input page being processed [2, 3]");

        operator.close();
    }

    @Test
    public void testStreamingWithoutInput()
            throws Exception
    {
        List<TableFunctionDataProcessor> processors = new ArrayList<>();
        Operator operator = createStreamingOperator(() -> {
            PassThroughProcessor processor = new PassThroughProcessor(true);
            processors.add(processor);
            return processor;
        });

        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isFinished()).isTrue();
        assertThat(processors).isEmpty();

        operator.close();
    }

    @Test
    public void testStreamingResultWithoutUsingInput()
            throws Exception
    {
        // the processor returns the first row of each input page before using the page
        Operator operator = createStreamingOperator(() -> new TableFunctionDataProcessor()
        {
            private long processedPositions;
            private boolean firstRowProduced;

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    return FINISHED;
                }
                Page page = getOnlyElement(input).orElseThrow();
                if (!firstRowProduced) {
                    firstRowProduced = true;
                    return produced(createIndexPage(processedPositions, 1));
                }
                firstRowProduced = false;
                TableFunctionProcessorState result = usedInputAndProduced(createIndexPage(processedPositions + 1, page.getPositionCount() - 1));
                processedPositions += page.getPositionCount();
                return result;
            }
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .row(12L)
                .buildPage());
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), rowPagesBuilder(BIGINT).row(10L).buildPage());
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), rowPagesBuilder(BIGINT).row(11L).row(12L).buildPage());

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(13L)
                .row(14L)
                .buildPage());
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), rowPagesBuilder(BIGINT).row(13L).buildPage());
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), rowPagesBuilder(BIGINT).row(14L).buildPage());

        operator.close();
    }

    @Test
    public void testStreamingRepeatedPassThroughRows()
            throws Exception
    {
        // the processor returns every input row three times
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return FINISHED;
            }
            Page page = getOnlyElement(input).orElseThrow();
            BlockBuilder indexes = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount() * 3);
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int repetition = 0; repetition < 3; repetition++) {
                    BIGINT.writeLong(indexes, position);
                }
            }
            return usedInputAndProduced(new Page(indexes.build()));
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .buildPage());
        Page output = operator.getOutput();
        assertPageEquals(ImmutableList.of(BIGINT), output, rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(11L)
                .row(11L)
                .row(11L)
                .buildPage());
        // the input rows are referenced, not copied
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);

        operator.close();
    }

    @Test
    public void testStreamingPartialPassThroughRows()
            throws Exception
    {
        // the processor returns only the last input row
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return FINISHED;
            }
            Page page = getOnlyElement(input).orElseThrow();
            return usedInputAndProduced(createIndexPage(page.getPositionCount() - 1, 1));
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .row(12L)
                .buildPage());
        Page output = operator.getOutput();
        assertPageEquals(ImmutableList.of(BIGINT), output, rowPagesBuilder(BIGINT)
                .row(12L)
                .buildPage());
        // the input row is copied, so that the output does not retain the whole input page
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);

        operator.close();
    }

    @Test
    public void testStreamingPassThroughRowsAroundCopyThreshold()
            throws Exception
    {
        // the processor returns the first three rows of the first input page, and the first two rows of the second one
        int[] returnedRows = {3, 2};
        int[] processedPages = {0};
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return FINISHED;
            }
            int page = processedPages[0]++;
            return usedInputAndProduced(createIndexPage(page * 4L, returnedRows[page]));
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .row(12L)
                .row(13L)
                .buildPage());
        Page output = operator.getOutput();
        assertPageEquals(ImmutableList.of(BIGINT), output, rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .row(12L)
                .buildPage());
        // three quarters of the input rows are referenced
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(14L)
                .row(15L)
                .row(16L)
                .row(17L)
                .buildPage());
        output = operator.getOutput();
        assertPageEquals(ImmutableList.of(BIGINT), output, rowPagesBuilder(BIGINT)
                .row(14L)
                .row(15L)
                .buildPage());
        // fewer than three quarters of the input rows are copied
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);

        operator.close();
    }

    @Test
    public void testStreamingFinishedBeforeInputIsFinished()
            throws Exception
    {
        // the processor finishes without using the first input page
        Operator operator = createStreamingOperator(() -> _ -> FINISHED);

        assertThat(operator.needsInput()).isTrue();
        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .buildPage());
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isFinished()).isTrue();
        assertThat(operator.needsInput()).isFalse();

        operator.close();
    }

    @Test
    public void testStreamingNullPassThroughIndex()
            throws Exception
    {
        // the processor returns a row of nulls between the input rows
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return FINISHED;
            }
            BlockBuilder indexes = BIGINT.createFixedSizeBlockBuilder(3);
            BIGINT.writeLong(indexes, 0);
            indexes.appendNull();
            BIGINT.writeLong(indexes, 1);
            return usedInputAndProduced(new Page(indexes.build()));
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .buildPage());
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), rowPagesBuilder(BIGINT)
                .row(10L)
                .row((Object) null)
                .row(11L)
                .buildPage());

        operator.close();
    }

    @Test
    public void testStreamingBlockedProcessor()
            throws Exception
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        PassThroughProcessor delegate = new PassThroughProcessor(true);
        Operator operator = createStreamingOperator(() -> input -> {
            if (!future.isDone()) {
                return blocked(future);
            }
            return delegate.process(input);
        });

        Page page = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(11L)
                .buildPage();
        operator.addInput(page);
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isBlocked().isDone()).isFalse();

        future.complete(null);
        assertThat(operator.isBlocked().isDone()).isTrue();
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), page);

        operator.close();
    }

    @Test
    public void testStreamingPassThroughIndexAfterInputFinished()
            throws Exception
    {
        // the processor returns an index of an input row after the input is finished
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return produced(createIndexPage(0, 1));
            }
            return usedInput();
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .buildPage());
        assertThat(operator.getOutput()).isNull();

        operator.finish();
        assertThatThrownBy(operator::getOutput)
                .isInstanceOf(TrinoException.class)
                .hasMessage("Index of a pass-through row must be null when no input data is being processed. Actual: 0");

        operator.close();
    }

    @Test
    public void testStreamingEmptyResultAfterInputFinished()
            throws Exception
    {
        // the processor returns a page without rows after the input is finished
        AtomicBoolean produced = new AtomicBoolean();
        Operator operator = createStreamingOperator(() -> input -> {
            if (input != null) {
                return usedInput();
            }
            if (produced.getAndSet(true)) {
                return FINISHED;
            }
            return produced(createIndexPage(0, 0));
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(10L)
                .buildPage());
        assertThat(operator.getOutput()).isNull();

        operator.finish();
        Page output = operator.getOutput();
        assertThat(output.getPositionCount()).isZero();
        assertThat(output.getChannelCount()).isEqualTo(1);

        operator.close();
    }

    @Test
    public void testStreamingInputPageMemoryAccounted()
            throws Exception
    {
        Operator operator = createStreamingOperator(() -> new TestingDataProcessor(true));
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        Page page = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(2L)
                .buildPage();
        operator.addInput(page);
        assertThat(operator.getOutput()).isNull();
        // the input page is retained until the function uses it
        assertThat(driverContext.getMemoryUsage()).isEqualTo(page.getRetainedSizeInBytes());

        operator.close();
    }

    @Test
    public void testStreamingInputPageMemoryReleased()
            throws Exception
    {
        // the processor returns the first input row before using the input page
        boolean[] produced = {false};
        Operator operator = createStreamingOperator(() -> input -> {
            if (input == null) {
                return FINISHED;
            }
            if (!produced[0]) {
                produced[0] = true;
                return produced(createIndexPage(0, 1));
            }
            return usedInput();
        });
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        Page page = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(2L)
                .buildPage();
        operator.addInput(page);
        assertThat(operator.getOutput()).isNotNull();
        assertThat(driverContext.getMemoryUsage()).isEqualTo(page.getRetainedSizeInBytes());

        // the input page is not accounted for after the function uses it
        assertThat(operator.getOutput()).isNull();
        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);

        operator.close();
    }

    private static Page createIndexPage(long firstIndex, int positions)
    {
        BlockBuilder indexes = BIGINT.createFixedSizeBlockBuilder(positions);
        for (int position = 0; position < positions; position++) {
            BIGINT.writeLong(indexes, firstIndex + position);
        }
        return new Page(indexes.build());
    }

    private Operator createOperator(Supplier<TableFunctionDataProcessor> processorSupplier)
    {
        return createOperator(_ -> processorSupplier.get());
    }

    /**
     * Creates an operator for a table function with a single pass-through source with row semantics.
     */
    private Operator createStreamingOperator(Supplier<TableFunctionDataProcessor> processorSupplier)
    {
        TableFunctionProcessorProvider provider = new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, MemoryContext memoryContext)
            {
                return processorSupplier.get();
            }
        };
        return new TableFunctionOperatorFactory(
                0,
                new PlanNodeId("test"),
                provider,
                TEST_CATALOG_HANDLE,
                new ConnectorTableFunctionHandle() {},
                0,
                1,
                ImmutableList.of(ImmutableList.of(0)),
                Optional.empty(),
                ImmutableList.of(new PassThroughColumnSpecification(false, 0, 0)),
                true,
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                0,
                ImmutableList.of(BIGINT),
                10,
                new PagesIndex.TestingFactory(false))
                .createOperator(newDriverContext());
    }

    private Operator createOperator(Function<MemoryContext, TableFunctionDataProcessor> processorFactory)
    {
        TableFunctionProcessorProvider provider = new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, MemoryContext memoryContext)
            {
                return processorFactory.apply(memoryContext);
            }
        };
        return new TableFunctionOperatorFactory(
                0,
                new PlanNodeId("test"),
                provider,
                TEST_CATALOG_HANDLE,
                new ConnectorTableFunctionHandle() {},
                0,
                0,
                ImmutableList.of(ImmutableList.of(0)),
                Optional.empty(),
                ImmutableList.of(),
                true,
                Optional.of(ImmutableList.of(0)),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                0,
                ImmutableList.of(BIGINT),
                10,
                new PagesIndex.TestingFactory(false))
                .createOperator(newDriverContext());
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static class TestingDataProcessor
            implements TableFunctionDataProcessor
    {
        private final boolean blocked;
        private int closeCount;

        TestingDataProcessor(boolean blocked)
        {
            this.blocked = blocked;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> input)
        {
            if (blocked) {
                return blocked(new CompletableFuture<>());
            }
            if (input == null) {
                return FINISHED;
            }
            return usedInput();
        }

        @Override
        public void close()
        {
            closeCount++;
        }
    }

    /**
     * Passes every input row through, like an identity function.
     */
    private static class PassThroughProcessor
            implements TableFunctionDataProcessor
    {
        private final boolean indexesFromInputStart;
        private long processedPositions;
        private int closeCount;

        PassThroughProcessor(boolean indexesFromInputStart)
        {
            this.indexesFromInputStart = indexesFromInputStart;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> input)
        {
            if (input == null) {
                return FINISHED;
            }
            Page page = getOnlyElement(input).orElseThrow();
            BlockBuilder indexes = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                BIGINT.writeLong(indexes, indexesFromInputStart ? processedPositions + position : position);
            }
            processedPositions += page.getPositionCount();
            return usedInputAndProduced(new Page(indexes.build()));
        }

        @Override
        public void close()
        {
            closeCount++;
        }
    }
}
