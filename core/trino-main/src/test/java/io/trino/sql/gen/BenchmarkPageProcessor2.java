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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.SequencePageBuilder;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.index.PageRecordSet;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.Test;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(5)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPageProcessor2
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction CONCAT = FUNCTIONS.resolveFunction("concat", fromTypes(VARCHAR, VARCHAR));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);
    private static final int POSITIONS = 1024;

    private final DriverYieldSignal yieldSignal = new DriverYieldSignal();
    private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

    private CursorProcessor cursorProcessor;
    private PageProcessor pageProcessor;
    private Page inputPage;
    private RecordSet recordSet;
    private List<Type> types;

    @Param({"2", "4", "8", "16", "32", "1024", "4000"})
    int columnCount;

    @Param({"varchar", "bigint"})
    String type;

    @Param({"false", "true"})
    boolean dictionaryBlocks;

    @Setup
    public void setup()
    {
        Type type = TYPE_MAP.get(this.type);

        for (int i = 0; i < columnCount; i++) {
            Symbol symbol = new Symbol(type, type.getDisplayName().toLowerCase(ENGLISH) + i);
            sourceLayout.put(symbol, i);
        }

        List<RowExpression> projections = getProjections(type);
        types = projections.stream().map(RowExpression::type).collect(toList());

        FunctionManager functionManager = createTestingFunctionManager();
        CursorProcessorCompiler cursorProcessorCompiler = new CursorProcessorCompiler(functionManager);
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(functionManager, 0);
        ColumnarFilterCompiler columnarFilterCompiler = new ColumnarFilterCompiler(functionManager, 0);

        inputPage = createPage(types, dictionaryBlocks);
        pageProcessor = new ExpressionCompiler(cursorProcessorCompiler, pageFunctionCompiler, columnarFilterCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections).get();

        recordSet = new PageRecordSet(types, inputPage);
        cursorProcessor = new ExpressionCompiler(cursorProcessorCompiler, pageFunctionCompiler, columnarFilterCompiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();
    }

    @Benchmark
    public Page rowOriented()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        cursorProcessor.process(null, yieldSignal, recordSet.cursor(), pageBuilder);
        return pageBuilder.build();
    }

    @Benchmark
    public List<Optional<Page>> columnOriented()
    {
        return ImmutableList.copyOf(
                pageProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(inputPage)));
    }

    private RowExpression getFilter(Type type)
    {
        if (type == VARCHAR) {
            return rowExpression(new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Cast(new Reference(VARCHAR, "varchar0"), BIGINT), new Constant(BIGINT, 2L))), new Constant(BIGINT, 0L)));
        }
        if (type == BIGINT) {
            return rowExpression(new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 0L)));
        }
        throw new IllegalArgumentException("filter not supported for type : " + type);
    }

    private List<RowExpression> getProjections(Type type)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (type == BIGINT) {
            for (int i = 0; i < columnCount; i++) {
                builder.add(rowExpression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint" + i), new Constant(BIGINT, 5L)))));
            }
        }
        else if (type == VARCHAR) {
            for (int i = 0; i < columnCount; i++) {
                // alternatively use identity expression rowExpression("varchar" + i, type) or
                // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                builder.add(rowExpression(new Call(CONCAT, ImmutableList.of(new Reference(VARCHAR, "varchar" + i), new Constant(VARCHAR, Slices.utf8Slice("foo"))))));
            }
        }
        return builder.build();
    }

    private RowExpression rowExpression(Expression expression)
    {
        return SqlToRowExpressionTranslator.translate(
                expression,
                sourceLayout,
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getTypeManager());
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        if (dictionary) {
            return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, POSITIONS);
        }
        return SequencePageBuilder.createSequencePage(types, POSITIONS);
    }

    @Test
    void testBenchmark()
    {
        BenchmarkPageProcessor2 benchmark = new BenchmarkPageProcessor2();
        for (int columnCount : ImmutableList.of(2, 4, 8, 16, 32, 1024, 4000)) {
            benchmark.columnCount = columnCount;

            for (boolean dictionaryBlocks : ImmutableList.of(true, false)) {
                benchmark.dictionaryBlocks = dictionaryBlocks;

                for (String type : ImmutableList.of("varchar", "bigint")) {
                    benchmark.type = type;

                    benchmark.setup();
                    benchmark.rowOriented();
                    benchmark.columnOriented();
                }
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkPageProcessor2.class).run();
    }
}
