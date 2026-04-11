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
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static java.lang.Math.toIntExact;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 15, time = 1)
@Measurement(iterations = 15, time = 1)
public class BenchmarkColumnarFilter
{
    private static final Random RANDOM = new Random(5376453765L);
    private static final long CONSTANT = 8456;
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final String COL_0 = "$col_0";
    private static final Map<Symbol, Integer> LAYOUT_BIGINT = ImmutableMap.of(new Symbol(BIGINT, COL_0), 0);
    private static final Map<Symbol, Integer> LAYOUT_INTEGER = ImmutableMap.of(new Symbol(INTEGER, COL_0), 0);
    private static final Map<Symbol, Integer> LAYOUT_SMALLINT = ImmutableMap.of(new Symbol(SMALLINT, COL_0), 0);

    private PageProcessor compiledProcessor;
    private final List<Page> inputPages = new ArrayList<>();
    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;
    @Param({"0", "10"})
    public int nullsPercentage;
    @Param
    public FilterProvider filterProvider;
    public String dataType = StandardTypes.INTEGER;

    public enum FilterProvider
    {
        BETWEEN {
            @Override
            Expression getExpression(Type type)
            {
                return new Between(
                        new Reference(type, COL_0),
                        new Constant(type, CONSTANT - 5),
                        new Constant(type, CONSTANT + 5));
            }
        },
        LESS_THAN {
            @Override
            Expression getExpression(Type type)
            {
                return call(
                        FUNCTION_RESOLUTION.resolveOperator(OperatorType.LESS_THAN, ImmutableList.of(type, type)),
                        new Constant(type, CONSTANT), new Reference(type, COL_0));
            }
        },
        IS_NULL {
            @Override
            Expression getExpression(Type type)
            {
                return new IsNull(new Reference(type, COL_0));
            }
        },
        IS_NOT_NULL {
            @Override
            Expression getExpression(Type type)
            {
                return call(
                        FUNCTION_RESOLUTION.resolveFunction("$not", fromTypes(BOOLEAN)),
                        new IsNull(new Reference(type, COL_0)));
            }
        }
        /**/;

        abstract Expression getExpression(Type type);
    }

    @Setup
    public void setup()
    {
        for (int pageCount = 0; pageCount < 20; pageCount++) {
            Block block = switch (dataType) {
                case StandardTypes.BIGINT -> createLongsBlock(8192, nullsPercentage);
                case StandardTypes.INTEGER -> createIntsBlock(8192, nullsPercentage);
                case StandardTypes.SMALLINT -> createShortsBlock(8192, nullsPercentage);
                default -> throw new UnsupportedOperationException();
            };
            inputPages.add(new Page(block.getPositionCount(), block));
        }

        Type type = switch (dataType) {
            case StandardTypes.BIGINT -> BIGINT;
            case StandardTypes.INTEGER -> INTEGER;
            case StandardTypes.SMALLINT -> SMALLINT;
            default -> throw new UnsupportedOperationException();
        };
        Map<Symbol, Integer> layout = switch (dataType) {
            case StandardTypes.BIGINT -> LAYOUT_BIGINT;
            case StandardTypes.INTEGER -> LAYOUT_INTEGER;
            case StandardTypes.SMALLINT -> LAYOUT_SMALLINT;
            default -> throw new UnsupportedOperationException();
        };
        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        compiledProcessor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        Optional.of(filterProvider.getExpression(type)),
                        Optional.empty(),
                        ImmutableList.of(new Reference(type, COL_0)),
                        layout,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
    }

    @Benchmark
    public long evaluateFilter()
    {
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        long outputRows = 0;
        for (Page inputPage : inputPages) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    null,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    SourcePage.create(inputPage));
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
        }
        return outputRows;
    }

    public static void runAllCombinations()
    {
        for (boolean columnarEvaluationEnabled : ImmutableList.of(false, true)) {
            for (FilterProvider filterProvider : FilterProvider.values()) {
                for (String dataType : ImmutableList.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.SMALLINT)) {
                    for (int nullsPercentage : ImmutableList.of(0, 10)) {
                        BenchmarkColumnarFilter benchmark = new BenchmarkColumnarFilter();
                        benchmark.filterProvider = filterProvider;
                        benchmark.dataType = dataType;
                        benchmark.columnarEvaluationEnabled = columnarEvaluationEnabled;
                        benchmark.nullsPercentage = nullsPercentage;
                        benchmark.setup();
                        benchmark.evaluateFilter();
                    }
                }
            }
        }
    }

    private static Block createShortsBlock(int positionsCount, int nullsPercentage)
    {
        short[] values = new short[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = (short) RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new ShortArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    private static Block createIntsBlock(int positionsCount, int nullsPercentage)
    {
        int[] values = new int[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new IntArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    private static Block createLongsBlock(int positionsCount, int nullsPercentage)
    {
        long[] values = new long[positionsCount];
        boolean[] isNull = new boolean[positionsCount];
        for (int i = 0; i < positionsCount; i++) {
            if (RANDOM.nextInt(100) < nullsPercentage) {
                isNull[i] = true;
            }
            else {
                values[i] = RANDOM.nextInt(toIntExact(CONSTANT - 10), toIntExact(CONSTANT + 10));
            }
        }
        return new LongArrayBlock(positionsCount, Optional.of(isNull), values);
    }

    static {
        try {
            // pollute the profile
            runAllCombinations();
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    static void main()
            throws Throwable
    {
        benchmark(BenchmarkColumnarFilter.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
