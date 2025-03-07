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
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.block.BlockAssertions;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.Split;
import io.trino.metadata.SqlScalarFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.index.PageRecordSet;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.TestPageProcessor.LazyPagePageProjection;
import io.trino.operator.project.TestPageProcessor.SelectAllFilter;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.RecordPageSource;
import io.trino.sql.gen.CursorProcessorCompiler;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.toValues;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestScanFilterAndProjectOperator
{
    private final Session session = TEST_SESSION;

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private QueryRunner runner;
    private ExpressionCompiler expressionCompiler;

    @BeforeAll
    public final void initTestFunctions()
    {
        runner = new StandaloneQueryRunner(session);
        FunctionManager functionManager = runner.getPlannerContext().getFunctionManager();
        expressionCompiler = new ExpressionCompiler(
                new CursorProcessorCompiler(functionManager),
                new PageFunctionCompiler(functionManager, 0),
                new ColumnarFilterCompiler(functionManager, 0));
    }

    @AfterAll
    public void tearDown()
    {
        closeAllRuntimeException(runner);
        runner = null;

        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testPageSource()
    {
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                (_) -> pageProcessor.get(),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(VARCHAR),
                DataSize.ofBytes(0),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testPageSourceMergeOutput()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        RowExpression filter = call(
                new TestingFunctionResolution(runner).resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                field(0, BIGINT),
                constant(10L, BIGINT));
        List<RowExpression> projections = ImmutableList.of(field(0, BIGINT));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.of(filter), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.of(filter), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new FixedPageSource(input),
                cursorProcessor,
                (_) -> pageProcessor.get(),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(BIGINT),
                DataSize.of(64, KILOBYTE),
                2);

        SourceOperator operator = factory.createOperator(newDriverContext());
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        List<Page> actual = toPages(operator);
        assertThat(actual).hasSize(1);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertPageEquals(ImmutableList.of(BIGINT), actual.get(0), expected.get(0));
    }

    @Test
    public void testPageSourceLazyLoad()
    {
        Block inputBlock = BlockAssertions.createLongSequenceBlock(0, 100);
        // If column 1 is loaded, test will fail
        Page input = new Page(100, inputBlock, new LazyBlock(100, () -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new PageFilterEvaluator(new SelectAllFilter())), ImmutableList.of(new LazyPagePageProjection()));

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new SinglePagePageSource(input),
                cursorProcessor,
                (_) -> pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(BIGINT),
                DataSize.ofBytes(0),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), ImmutableList.of(new Page(inputBlock)));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), toPages(operator));

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testRecordCursorSource()
    {
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(VARCHAR), input)),
                cursorProcessor,
                (_) -> pageProcessor.get(),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(VARCHAR),
                DataSize.ofBytes(0),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testPageYield()
    {
        int totalRows = 1000;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), totalRows, 1);
        DriverContext driverContext = newDriverContext();

        // 20 columns; each column is associated with a function that will force yield per projection
        int totalColumns = 20;
        ImmutableList.Builder<SqlScalarFunction> functions = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            functions.add(new GenericLongFunction("page_col" + i, value -> {
                driverContext.getYieldSignal().forceYieldForTesting();
                return value;
            }));
        }
        runner.addFunctions(new InternalFunctionBundle(functions.build()));

        // match each column with a projection
        FunctionManager functionManager = runner.getPlannerContext().getFunctionManager();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(
                new CursorProcessorCompiler(functionManager),
                new PageFunctionCompiler(functionManager, 0),
                new ColumnarFilterCompiler(functionManager, 0));
        ImmutableList.Builder<RowExpression> projections = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            projections.add(call(runner.getPlannerContext().getMetadata().resolveBuiltinFunction("generic_long_page_col" + i, fromTypes(BIGINT)), field(0, BIGINT)));
        }
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections.build(), "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections.build(), MAX_BATCH_SIZE);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                (_) -> pageProcessor.get(),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(BIGINT),
                DataSize.ofBytes(0),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        // In the below loop we yield for every cell: 20 X 1000 times
        // Currently we don't check for the yield signal in the generated projection loop, we only check for the yield signal
        // in the PageProcessor.PositionsPageProcessorIterator::computeNext() method. Therefore, after 20 calls we will have
        // exactly 20 blocks (one for each column) and the PageProcessor will be able to create a Page out of it.
        for (int i = 1; i <= totalRows * totalColumns; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            Page page = operator.getOutput();
            if (i == totalColumns) {
                assertThat(page).isNotNull();
                assertThat(page.getPositionCount()).isEqualTo(totalRows);
                assertThat(page.getChannelCount()).isEqualTo(totalColumns);
                for (int j = 0; j < totalColumns; j++) {
                    assertThat(toValues(BIGINT, page.getBlock(j))).isEqualTo(toValues(BIGINT, input.getBlock(0)));
                }
            }
            else {
                assertThat(page).isNull();
            }
            driverContext.getYieldSignal().reset();
        }
    }

    @Test
    public void testRecordCursorYield()
    {
        // create a generic long function that yields for projection on every row
        // verify we will yield #row times totally

        // create a table with 15 rows
        int length = 15;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), length, 0);
        DriverContext driverContext = newDriverContext();

        // set up generic long function with a callback to force yield
        runner.addFunctions(new InternalFunctionBundle(new GenericLongFunction("record_cursor", value -> {
            driverContext.getYieldSignal().forceYieldForTesting();
            return value;
        })));
        FunctionManager functionManager = runner.getPlannerContext().getFunctionManager();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(
                new CursorProcessorCompiler(functionManager),
                new PageFunctionCompiler(functionManager, 0),
                new ColumnarFilterCompiler(functionManager, 0));

        List<RowExpression> projections = ImmutableList.of(call(
                runner.getPlannerContext().getMetadata().resolveBuiltinFunction("generic_long_record_cursor", fromTypes(BIGINT)),
                field(0, BIGINT)));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (catalog) -> (session, split, table, columns, dynamicFilter) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(BIGINT), input)),
                cursorProcessor,
                (_) -> pageProcessor.get(),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                ImmutableList.of(BIGINT),
                DataSize.ofBytes(0),
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        // start driver; get null value due to yield for the first 15 times
        for (int i = 0; i < length; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            assertThat(operator.getOutput()).isNull();
            driverContext.getYieldSignal().reset();
        }

        // the 16th yield is not going to prevent the operator from producing a page
        driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
        Page output = operator.getOutput();
        driverContext.getYieldSignal().reset();
        assertThat(output).isNotNull();
        assertThat(toValues(BIGINT, output.getBlock(0))).isEqualTo(toValues(BIGINT, input.getBlock(0)));
    }

    @Test
    public void testRecordMaterializedBytes()
    {
        Block first = createIntsBlock(1, 2, 3);
        LazyBlock second = lazyWrapper(first);
        LazyBlock third = lazyWrapper(first);
        Page page = new Page(3, first, second, third);

        second.getLoadedBlock();

        AtomicLong sizeInBytes = new AtomicLong();
        ScanFilterAndProjectOperator.ProcessedBytesMonitor monitor = new ScanFilterAndProjectOperator.ProcessedBytesMonitor(page, sizeInBytes::getAndAdd);

        assertThat(sizeInBytes.get()).isEqualTo(first.getSizeInBytes() * 2);

        page.getBlock(2).getLoadedBlock();
        monitor.update();
        assertThat(sizeInBytes.get()).isEqualTo(first.getSizeInBytes() * 3);
    }

    @Test
    public void testNestedBlocks()
    {
        Block elements = lazyWrapper(createIntsBlock(1, 2, 3));
        Block arrayBlock = ArrayBlock.fromElementBlock(2, Optional.empty(), new int[] {0, 1, 3}, elements);
        long initialArraySize = arrayBlock.getSizeInBytes();
        Page page = new Page(2, arrayBlock);

        AtomicLong sizeInBytes = new AtomicLong();
        ScanFilterAndProjectOperator.ProcessedBytesMonitor monitor = new ScanFilterAndProjectOperator.ProcessedBytesMonitor(page, sizeInBytes::getAndAdd);

        assertThat(arrayBlock.getSizeInBytes()).isEqualTo(initialArraySize);
        assertThat(sizeInBytes.get()).isEqualTo(arrayBlock.getSizeInBytes());

        // dictionary block caches size in bytes
        arrayBlock.getLoadedBlock();
        monitor.update();
        assertThat(sizeInBytes.get()).isEqualTo(arrayBlock.getSizeInBytes());
        assertThat(sizeInBytes.get()).isEqualTo(initialArraySize + elements.getSizeInBytes());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }

    private static List<Page> toPages(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // read output until input is needed or operator is finished
        int nullPages = 0;
        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                // break infinite loop due to null pages
                assertThat(nullPages < 1_000_000)
                        .describedAs("Too many null pages; infinite loop?")
                        .isTrue();
                nullPages++;
            }
            else {
                outputPages.add(outputPage);
                nullPages = 0;
            }
        }

        return outputPages.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    public static class SinglePagePageSource
            implements ConnectorPageSource
    {
        private Page page;

        public SinglePagePageSource(Page page)
        {
            this.page = page;
        }

        @Override
        public void close()
        {
            page = null;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return page == null;
        }

        @Override
        public Page getNextPage()
        {
            Page page = this.page;
            this.page = null;
            return page;
        }
    }
}
