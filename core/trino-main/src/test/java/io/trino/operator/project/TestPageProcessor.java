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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.CompletedWork;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.TestingSourcePage;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionProfiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.relational.CallExpression;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.execution.executor.timesharing.PrioritizedSplitRunner.SPLIT_RUN_QUANTA;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPageProcessor
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testProjectNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.empty(), Optional.empty(), ImmutableList.of(), OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertThat(outputPage.getChannelCount()).isEqualTo(0);
        assertThat(outputPage.getPositionCount()).isEqualTo(inputPage.getPositionCount());
    }

    @Test
    public void testFilterNoColumns()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new TestingPageFilter(positionsRange(0, 50)))), ImmutableList.of());

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(1);
        Page outputPage = outputPages.get(0).orElse(null);
        assertThat(outputPage.getChannelCount()).isEqualTo(0);
        assertThat(outputPage.getPositionCount()).isEqualTo(50);
    }

    @Test
    public void testPartialFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new TestingPageFilter(positionsRange(25, 50)))),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, BIGINT)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(25, 75)));
    }

    @Test
    public void testSelectAllFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, BIGINT)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testSelectNoneFilter()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectNoneFilter())), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 100));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testProjectEmptyPage()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectAllFilter())), ImmutableList.of(new InputPageProjection(0, BIGINT)));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, 0));

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        // output should be one page containing no columns (only a count)
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testSelectNoneFilterLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectNoneFilter())), ImmutableList.of(new InputPageProjection(1, BIGINT)));

        // if channel 1 is loaded, test will fail
        SourcePage inputPage = new TestingSourcePage(100, createLongSequenceBlock(0, 100), null);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext, inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testProjectLazyLoad()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new LazyPagePageProjection()),
                OptionalInt.of(MAX_BATCH_SIZE));

        // if channel 1 is loaded, test will fail
        SourcePage inputPage = new TestingSourcePage(100, createLongSequenceBlock(0, 100), null);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        Iterator<Optional<Page>> output = pageProcessor.process(SESSION, new DriverYieldSignal(), memoryContext, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(1);
        assertPageEquals(ImmutableList.of(BIGINT), outputPages.get(0).orElse(null), new Page(createLongSequenceBlock(0, 100)));
    }

    @Test
    public void testBatchedOutput()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, BIGINT)),
                OptionalInt.of(MAX_BATCH_SIZE));

        SourcePage inputPage = SourcePage.create(createLongSequenceBlock(0, (int) (MAX_BATCH_SIZE * 2.5)));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        assertThat(outputPages).hasSize(3);
        for (int i = 0; i < outputPages.size(); i++) {
            Page actualPage = outputPages.get(i).orElse(null);
            int offset = i * MAX_BATCH_SIZE;
            Page expectedPage = new Page(createLongSequenceBlock(offset, offset + Math.min(inputPage.getPositionCount() - offset, MAX_BATCH_SIZE)));
            assertPageEquals(ImmutableList.of(BIGINT), actualPage, expectedPage);
        }
    }

    @Test
    public void testAdaptiveBatchSize()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, VARCHAR)),
                OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(4096));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);

        List<Optional<Page>> outputPages = ImmutableList.copyOf(output);
        int batchSize = MAX_BATCH_SIZE;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize)));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }

        // process small page which will increase batch size
        Arrays.fill(slices, Slices.allocate(128));
        inputPage = SourcePage.create(createSlicesBlock(slices));

        output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);

        outputPages = ImmutableList.copyOf(output);
        int offset = 0;
        for (Optional<Page> actualPage : outputPages) {
            Page expectedPage = new Page(createSlicesBlock(Arrays.copyOfRange(slices, 0, Math.min(inputPage.getPositionCount() - offset, batchSize))));
            assertPageEquals(ImmutableList.of(VARCHAR), actualPage.orElse(null), expectedPage);
            offset += actualPage.orElseThrow(() -> new AssertionError("page is not present")).getPositionCount();
            if (actualPage.orElseThrow(() -> new AssertionError("page is not present")).getSizeInBytes() < MIN_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize * 2;
            }
        }
    }

    @Test
    public void testOptimisticProcessing()
    {
        InvocationCountPageProjection firstProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        InvocationCountPageProjection secondProjection = new InvocationCountPageProjection(new InputPageProjection(0, VARCHAR));
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(firstProjection, secondProjection),
                OptionalInt.of(MAX_BATCH_SIZE));

        // process large page which will reduce batch size
        Slice[] slices = new Slice[(int) (MAX_BATCH_SIZE * 2.5)];
        Arrays.fill(slices, Slices.allocate(4096));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        // batch size will be reduced before the first page is produced until the first block is within the page size bounds
        int batchSize = MAX_BATCH_SIZE;
        while (inputPage.getBlock(0).getRegionSizeInBytes(0, batchSize) > MAX_PAGE_SIZE_IN_BYTES) {
            batchSize /= 2;
        }

        int pageCount = 0;
        while (output.hasNext()) {
            Page actualPage = output.next().orElse(null);
            Block sliceBlock = createSlicesBlock(Arrays.copyOfRange(slices, 0, batchSize));
            Page expectedPage = new Page(sliceBlock, sliceBlock);
            assertPageEquals(ImmutableList.of(VARCHAR, VARCHAR), actualPage, expectedPage);
            pageCount++;

            // batch size will be further reduced to fit within the bounds
            if (actualPage.getSizeInBytes() > MAX_PAGE_SIZE_IN_BYTES) {
                batchSize = batchSize / 2;
            }
        }
        // second project is invoked once per output page
        assertThat(secondProjection.getInvocationCount()).isEqualTo(pageCount);

        // the page processor saves the results when the page size is exceeded, so the first projection
        // will be invoked less times
        assertThat(firstProjection.getInvocationCount() < secondProjection.getInvocationCount()).isTrue();
    }

    @Test
    public void testRetainedSize()
    {
        PageProcessor pageProcessor = new PageProcessor(
                Optional.of(new PageFilterEvaluator(new SelectAllFilter())),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, VARCHAR), new InputPageProjection(1, VARCHAR)),
                OptionalInt.of(MAX_BATCH_SIZE));

        // create 2 columns X 800 rows of strings with each string's size = 30KB
        // this can force previouslyComputedResults to be saved given the page is 48MB in size
        String value = join("", nCopies(30_000, "a"));
        List<String> values = nCopies(800, value);
        SourcePage inputPage = SourcePage.create(new Page(createStringsBlock(values), createStringsBlock(values)));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), memoryContext, inputPage);

        // force a compute
        // one block of previouslyComputedResults will be saved given the first column is with 8MB
        assertThat(output.hasNext()).isTrue();

        // verify we do not count block sizes twice
        // comparing with the input page, the output page also contains an extra instance size for previouslyComputedResults
        assertThat(memoryContext.getBytes() - instanceSize(VariableWidthBlock.class)).isCloseTo(inputPage.getRetainedSizeInBytes(), Offset.offset(200L));
    }

    @Test
    public void testYieldProjection()
    {
        // each projection can finish without yield
        // while between two projections, there is a yield
        int rows = 128;
        int columns = 20;
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                Collections.nCopies(columns, new YieldPageProjection(new InputPageProjection(0, VARCHAR))),
                OptionalInt.of(MAX_BATCH_SIZE));

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));

        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, yieldSignal, inputPage);

        // Test yield signal works for page processor.
        // The purpose of this test is NOT to test the yield signal in page projection; we have other tests to cover that.
        // In page processor, we check yield signal after a column has been completely processed.
        // So we would like to set yield signal when the column has just finished processing in order to let page processor capture the yield signal when the block is returned.
        // Also, we would like to reset the yield signal before starting to process the next column in order NOT to yield per position inside the column.
        for (int i = 0; i < columns - 1; i++) {
            assertThat(output.hasNext()).isTrue();
            assertThat(output.next().orElse(null)).isNull();
            assertThat(yieldSignal.isSet()).isTrue();
            yieldSignal.reset();
        }
        assertThat(output.hasNext()).isTrue();
        Page actualPage = output.next().orElse(null);
        assertThat(actualPage).isNotNull();
        assertThat(yieldSignal.isSet()).isTrue();
        yieldSignal.reset();

        Block[] blocks = new Block[columns];
        Arrays.fill(blocks, createSlicesBlock(Arrays.copyOfRange(slices, 0, rows)));
        Page expectedPage = new Page(blocks);
        assertPageEquals(Collections.nCopies(columns, VARCHAR), actualPage, expectedPage);
        assertThat(output.hasNext()).isFalse();
    }

    @Test
    public void testExpressionProfiler()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        CallExpression add10Expression = call(
                functionResolution.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                field(0, BIGINT),
                constant(10L, BIGINT));

        TestingTicker testingTicker = new TestingTicker();
        PageFunctionCompiler functionCompiler = functionResolution.getPageFunctionCompiler();
        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(add10Expression, Optional.empty());
        PageProjection projection = projectionSupplier.get();
        SourcePage page = SourcePage.create(createLongSequenceBlock(1, 11));
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, SPLIT_RUN_QUANTA);
        for (int i = 0; i < 100; i++) {
            profiler.start();
            Work<Block> work = projection.project(SESSION, new DriverYieldSignal(), page, SelectedPositions.positionsRange(0, page.getPositionCount()));
            if (i < 10) {
                // increment the ticker with a large value to mark the expression as expensive
                testingTicker.increment(10, SECONDS);
                profiler.stop(page.getPositionCount());
                assertThat(profiler.isExpressionExpensive()).isTrue();
            }
            else {
                testingTicker.increment(0, NANOSECONDS);
                profiler.stop(page.getPositionCount());
                assertThat(profiler.isExpressionExpensive()).isFalse();
            }
            work.process();
        }
    }

    @Test
    public void testIncreasingBatchSize()
    {
        int rows = 1024;

        // We deliberately do not set the ticker, so that the expression is always cheap and the batch size gets doubled until other limits are hit
        TestingTicker testingTicker = new TestingTicker();
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, SPLIT_RUN_QUANTA);
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, BIGINT)),
                OptionalInt.of(1),
                profiler);

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        long previousPositionCount = 1;
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertThat(page).isPresent();
            long positionCount = page.get().getPositionCount();
            totalPositionCount += positionCount;
            // skip the first read && skip the last read, which can be a partial page
            if (positionCount > 1 && totalPositionCount != rows) {
                assertThat(positionCount).isEqualTo(previousPositionCount * 2);
            }
            previousPositionCount = positionCount;
        }
    }

    @Test
    public void testDecreasingBatchSize()
    {
        int rows = 1024;

        // We set the expensive expression threshold to 0, so the expression is always considered expensive and the batch size gets halved until it becomes 1
        TestingTicker testingTicker = new TestingTicker();
        ExpressionProfiler profiler = new ExpressionProfiler(testingTicker, new Duration(0, MILLISECONDS));
        PageProcessor pageProcessor = new PageProcessor(
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new InputPageProjection(0, BIGINT)),
                OptionalInt.of(512),
                profiler);

        Slice[] slices = new Slice[rows];
        Arrays.fill(slices, Slices.allocate(rows));
        SourcePage inputPage = SourcePage.create(createSlicesBlock(slices));
        Iterator<Optional<Page>> output = processAndAssertRetainedPageSize(pageProcessor, inputPage);

        long previousPositionCount = 1;
        long totalPositionCount = 0;
        while (totalPositionCount < rows) {
            Optional<Page> page = output.next();
            assertThat(page).isPresent();
            long positionCount = page.get().getPositionCount();
            totalPositionCount += positionCount;
            // the batch size doesn't get smaller than 1
            if (positionCount > 1 && previousPositionCount != 1) {
                assertThat(positionCount).isEqualTo(previousPositionCount / 2);
            }
            previousPositionCount = positionCount;
        }
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, SourcePage inputPage)
    {
        return processAndAssertRetainedPageSize(pageProcessor, new DriverYieldSignal(), inputPage);
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, DriverYieldSignal yieldSignal, SourcePage inputPage)
    {
        return processAndAssertRetainedPageSize(pageProcessor, yieldSignal, newSimpleAggregatedMemoryContext(), inputPage);
    }

    private Iterator<Optional<Page>> processAndAssertRetainedPageSize(PageProcessor pageProcessor, DriverYieldSignal yieldSignal, AggregatedMemoryContext memoryContext, SourcePage inputPage)
    {
        Iterator<Optional<Page>> output = pageProcessor.process(
                SESSION,
                yieldSignal,
                memoryContext.newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                inputPage);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        return output;
    }

    private static class InvocationCountPageProjection
            implements PageProjection
    {
        protected final PageProjection delegate;
        private int invocationCount;

        public InvocationCountPageProjection(PageProjection delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public Type getType()
        {
            return delegate.getType();
        }

        @Override
        public boolean isDeterministic()
        {
            return delegate.isDeterministic();
        }

        @Override
        public InputChannels getInputChannels()
        {
            return delegate.getInputChannels();
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, SourcePage page, SelectedPositions selectedPositions)
        {
            setInvocationCount(getInvocationCount() + 1);
            return delegate.project(session, yieldSignal, page, selectedPositions);
        }

        public int getInvocationCount()
        {
            return invocationCount;
        }

        public void setInvocationCount(int invocationCount)
        {
            this.invocationCount = invocationCount;
        }
    }

    private class YieldPageProjection
            extends InvocationCountPageProjection
    {
        public YieldPageProjection(PageProjection delegate)
        {
            super(delegate);
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, SourcePage page, SelectedPositions selectedPositions)
        {
            return new YieldPageProjectionWork(session, yieldSignal, page, selectedPositions);
        }

        private class YieldPageProjectionWork
                implements Work<Block>
        {
            private final DriverYieldSignal yieldSignal;
            private final Work<Block> work;

            public YieldPageProjectionWork(ConnectorSession session, DriverYieldSignal yieldSignal, SourcePage page, SelectedPositions selectedPositions)
            {
                this.yieldSignal = yieldSignal;
                this.work = delegate.project(session, yieldSignal, page, selectedPositions);
            }

            @Override
            public boolean process()
            {
                assertThat(work.process()).isTrue();
                yieldSignal.setWithDelay(1, executor);
                yieldSignal.forceYieldForTesting();
                return true;
            }

            @Override
            public Block getResult()
            {
                return work.getResult();
            }
        }
    }

    public static class LazyPagePageProjection
            implements PageProjection
    {
        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0, 1);
        }

        @Override
        public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, SourcePage page, SelectedPositions selectedPositions)
        {
            return new CompletedWork<>(page.getBlock(0).getLoadedBlock());
        }
    }

    private static class TestingPageFilter
            implements PageFilter
    {
        private final SelectedPositions selectedPositions;

        public TestingPageFilter(SelectedPositions selectedPositions)
        {
            this.selectedPositions = selectedPositions;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return selectedPositions;
        }
    }

    public static class SelectAllFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return positionsRange(0, page.getPositionCount());
        }
    }

    private static class SelectNoneFilter
            implements PageFilter
    {
        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return new InputChannels(0);
        }

        @Override
        public SelectedPositions filter(ConnectorSession session, SourcePage page)
        {
            return positionsRange(0, 0);
        }
    }
}
