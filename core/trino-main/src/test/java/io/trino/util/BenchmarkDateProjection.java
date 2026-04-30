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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import io.trino.SequencePageBuilder;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.TypeManager;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
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
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDateProjection
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(
            InternalFunctionBundle.builder()
                    .scalars(LegacyJodaDateTimeFunctions.class)
                    .scalars(JdkDateTimeFunctions.class)
                    .build());
    private static final Metadata METADATA = FUNCTIONS.getMetadata();
    private static final TypeManager TYPE_MANAGER = FUNCTIONS.getPlannerContext().getTypeManager();
    private static final FunctionManager FUNCTION_MANAGER = FUNCTIONS.getPlannerContext().getFunctionManager();

    private static final ResolvedFunction YEAR_FAST = FUNCTIONS.resolveFunction("year", fromTypes(DATE));
    private static final ResolvedFunction MONTH_FAST = FUNCTIONS.resolveFunction("month", fromTypes(DATE));
    private static final ResolvedFunction DAY_FAST = FUNCTIONS.resolveFunction("day", fromTypes(DATE));
    private static final ResolvedFunction YEAR_JODA = FUNCTIONS.resolveFunction("legacy_year", fromTypes(DATE));
    private static final ResolvedFunction MONTH_JODA = FUNCTIONS.resolveFunction("legacy_month", fromTypes(DATE));
    private static final ResolvedFunction DAY_JODA = FUNCTIONS.resolveFunction("legacy_day", fromTypes(DATE));
    private static final ResolvedFunction YEAR_JDK = FUNCTIONS.resolveFunction("jdk_year", fromTypes(DATE));
    private static final ResolvedFunction MONTH_JDK = FUNCTIONS.resolveFunction("jdk_month", fromTypes(DATE));
    private static final ResolvedFunction DAY_JDK = FUNCTIONS.resolveFunction("jdk_day", fromTypes(DATE));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private static final int POSITIONS = 1024;

    @Param({"identity", "year", "yearMonthDay", "combined"})
    String projection;

    @Param({"fast", "joda", "jdk"})
    String impl;

    private PageProcessor pageProcessor;
    private Page inputPage;

    @Setup
    public void setup()
    {
        Reference dateRef = new Reference(DATE, "d");
        ResolvedFunction year;
        ResolvedFunction month;
        ResolvedFunction day;
        switch (impl) {
            case "fast" -> {
                year = YEAR_FAST;
                month = MONTH_FAST;
                day = DAY_FAST;
            }
            case "joda" -> {
                year = YEAR_JODA;
                month = MONTH_JODA;
                day = DAY_JODA;
            }
            case "jdk" -> {
                year = YEAR_JDK;
                month = MONTH_JDK;
                day = DAY_JDK;
            }
            default -> throw new IllegalArgumentException(impl);
        }

        List<Expression> projections = switch (projection) {
            case "identity" -> ImmutableList.of(dateRef);
            case "year" -> ImmutableList.of(call(year, dateRef));
            case "yearMonthDay" -> ImmutableList.of(call(year, dateRef), call(month, dateRef), call(day, dateRef));
            // year * 10000 + month * 100 + day — single column, all three functions in one expression.
            case "combined" -> ImmutableList.of(
                    call(ADD_BIGINT,
                            call(ADD_BIGINT,
                                    call(MULTIPLY_BIGINT, call(year, dateRef), new Constant(BIGINT, 10000L)),
                                    call(MULTIPLY_BIGINT, call(month, dateRef), new Constant(BIGINT, 100L))),
                            call(day, dateRef)));
            default -> throw new IllegalArgumentException(projection);
        };

        Map<Symbol, Integer> sourceLayout = new HashMap<>();
        sourceLayout.put(new Symbol(DATE, "d"), 0);

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(FUNCTION_MANAGER, METADATA, TYPE_MANAGER, 0);
        ColumnarFilterCompiler columnarFilterCompiler = new ColumnarFilterCompiler(FUNCTIONS.getPlannerContext(), 0);

        inputPage = SequencePageBuilder.createSequencePage(ImmutableList.of(DATE), POSITIONS);
        pageProcessor = new ExpressionCompiler(pageFunctionCompiler, columnarFilterCompiler)
                .compilePageProcessor(true, true, Optional.empty(), Optional.empty(), projections, sourceLayout, Optional.empty(), OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
    }

    @Benchmark
    public List<Optional<Page>> project()
    {
        return ImmutableList.copyOf(
                pageProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(inputPage)));
    }

    @Test
    void testBenchmark()
    {
        for (String p : ImmutableList.of("identity", "year", "yearMonthDay", "combined")) {
            for (String i : ImmutableList.of("fast", "joda", "jdk")) {
                BenchmarkDateProjection bench = new BenchmarkDateProjection();
                bench.projection = p;
                bench.impl = i;
                bench.setup();
                bench.project();
            }
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkDateProjection.class).run();
    }
}
