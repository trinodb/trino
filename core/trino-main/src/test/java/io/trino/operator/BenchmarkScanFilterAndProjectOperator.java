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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Expression;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTaskContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ExpressionTestUtils.createExpression;
import static io.trino.sql.ExpressionTestUtils.getTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSplit.createLocalSplit;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(5)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkScanFilterAndProjectOperator
{
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);

    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();

    private static final int TOTAL_POSITIONS = 1_000_000;
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = DataSize.of(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    @State(Thread)
    public static class Context
    {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        @Param({"32", "1024"})
        int positionsPerPage = 32;

        @Param({"2", "4", "8", "16", "32"})
        int columnCount = 2;

        @Param({"varchar", "bigint"})
        String type = "varchar";

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            Type type = TYPE_MAP.get(this.type);

            for (int i = 0; i < columnCount; i++) {
                Symbol symbol = new Symbol(type.getDisplayName().toLowerCase(ENGLISH) + i);
                symbolTypes.put(symbol, type);
                sourceLayout.put(symbol, i);
            }

            List<RowExpression> projections = getProjections(type);
            List<Type> types = projections.stream().map(RowExpression::getType).collect(toList());
            List<ColumnHandle> columnHandles = IntStream.range(0, columnCount)
                    .mapToObj(i -> new TestingColumnHandle(Integer.toString(i)))
                    .collect(toImmutableList());

            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
            PageProcessor pageProcessor = new ExpressionCompiler(PLANNER_CONTEXT.getFunctionManager(), pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections).get();
            CursorProcessor cursorProcessor = new ExpressionCompiler(PLANNER_CONTEXT.getFunctionManager(), pageFunctionCompiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();

            createTaskContext();
            createScanFilterAndProjectOperatorFactories(createInputPages(types), pageProcessor, cursorProcessor, columnHandles, types);
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private void createScanFilterAndProjectOperatorFactories(List<Page> inputPages, PageProcessor pageProcessor, CursorProcessor cursorProcessor, List<ColumnHandle> columnHandles, List<Type> types)
        {
            operatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("test_source"),
                    (session, split, table, columns, dynamicFilter) -> new FixedPageSource(inputPages),
                    () -> cursorProcessor,
                    () -> pageProcessor,
                    TEST_TABLE_HANDLE,
                    columnHandles,
                    DynamicFilter.EMPTY,
                    types,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        private List<Page> createInputPages(List<Type> types)
        {
            ImmutableList.Builder<Page> inputPagesBuilder = ImmutableList.builder();
            for (int i = 0; i < TOTAL_POSITIONS / positionsPerPage; ++i) {
                inputPagesBuilder.add(createPage(types, positionsPerPage, dictionaryBlocks));
            }
            return inputPagesBuilder.build();
        }

        private RowExpression getFilter(Type type)
        {
            if (type == VARCHAR) {
                return rowExpression("cast(varchar0 as bigint) % 2 = 0");
            }
            if (type == BIGINT) {
                return rowExpression("bigint0 % 2 = 0");
            }
            throw new IllegalArgumentException("filter not supported for type : " + type);
        }

        private List<RowExpression> getProjections(Type type)
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            if (type == BIGINT) {
                for (int i = 0; i < columnCount; i++) {
                    builder.add(rowExpression("bigint" + i + " + 5"));
                }
            }
            else if (type == VARCHAR) {
                for (int i = 0; i < columnCount; i++) {
                    // alternatively use identity expression rowExpression("varchar" + i, type) or
                    // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                    builder.add(rowExpression("concat(varchar" + i + ", 'foo')"));
                }
            }
            return builder.build();
        }

        private RowExpression rowExpression(String value)
        {
            Expression expression = createExpression(value, PLANNER_CONTEXT, TypeProvider.copyOf(symbolTypes));

            return SqlToRowExpressionTranslator.translate(
                    expression,
                    getTypes(TEST_SESSION, PLANNER_CONTEXT, TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getFunctionManager(),
                    TEST_SESSION,
                    true);
        }

        private static Page createPage(List<? extends Type> types, int positions, boolean dictionary)
        {
            if (dictionary) {
                return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, positions);
            }
            return SequencePageBuilder.createSequencePage(types, positions);
        }
    }

    @Benchmark
    public List<Page> benchmarkColumnOriented(Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        SourceOperator operator = (SourceOperator) context.getOperatorFactory().createOperator(driverContext);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, createLocalSplit()));
        operator.noMoreSplits();

        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void testBenchmark()
    {
        Context context = new Context();
        context.setup();
        benchmarkColumnOriented(context);
        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkScanFilterAndProjectOperator.class).run();
    }
}
