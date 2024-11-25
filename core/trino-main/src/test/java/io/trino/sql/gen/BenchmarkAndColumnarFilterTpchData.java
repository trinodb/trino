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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.IrExpressions.call;
import static java.nio.charset.StandardCharsets.UTF_8;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 15, time = 1)
@Measurement(iterations = 10, time = 1)
public class BenchmarkAndColumnarFilterTpchData
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    private static final String COL_EXTENDED_PRICE = "$col_" + EXTENDED_PRICE;
    private static final String COL_DISCOUNT = "$col_" + DISCOUNT;
    private static final String COL_SHIP_DATE = "$col_" + SHIP_DATE;
    private static final String COL_QUANTITY = "$col_" + QUANTITY;

    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(
            new Symbol(DOUBLE, COL_EXTENDED_PRICE), EXTENDED_PRICE,
            new Symbol(DOUBLE, COL_DISCOUNT), DISCOUNT,
            new Symbol(VARCHAR, COL_SHIP_DATE), SHIP_DATE,
            new Symbol(DOUBLE, COL_QUANTITY), QUANTITY);

    private static final Slice MIN_SHIP_DATE = utf8Slice("1994-01-01");
    private static final Slice MAX_SHIP_DATE = utf8Slice("1995-01-01");

    private List<Page> inputPages;
    private PageProcessor processor;

    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;

    @Setup
    public void setup()
    {
        inputPages = createInputPages();

        Expression filterExpression = createFilterExpression(FUNCTION_RESOLUTION);
        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        List<? extends Expression> projections = ImmutableList.of(new Reference(DOUBLE, COL_EXTENDED_PRICE));
        processor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        true,
                        Optional.of(filterExpression),
                        Optional.empty(),
                        projections,
                        LAYOUT,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
    }

    @Benchmark
    public long compiled()
    {
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        long outputRows = 0;
        for (Page inputPage : inputPages) {
            WorkProcessor<Page> workProcessor = processor.createWorkProcessor(
                    null,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    SourcePage.create(inputPage));
            while (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
        }
        return outputRows;
    }

    private static List<Page> createInputPages()
    {
        PageBuilder pageBuilder = PageBuilder.withMaxPageSize(350 * 1024, ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, DOUBLE));
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (LineItem lineItem : new LineItemGenerator(1, 1, 1)) {
            pageBuilder.declarePosition();

            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.extendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.discount());
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(SHIP_DATE), Slices.wrappedBuffer(LineItemColumn.SHIP_DATE.getString(lineItem).getBytes(UTF_8)));
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.quantity());

            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pages.add(page);
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages.build();
    }

    // where shipdate >= '1994-01-01'
    //    and shipdate < '1995-01-01'
    //    and discount >= 0.05
    //    and discount <= 0.07
    //    and quantity < 24;
    private static Expression createFilterExpression(TestingFunctionResolution functionResolution)
    {
        return new Logical(
                Logical.Operator.AND,
                ImmutableList.of(
                        call(
                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(VARCHAR, VARCHAR)),
                                new Constant(VARCHAR, MIN_SHIP_DATE), new Reference(VARCHAR, COL_SHIP_DATE)),
                        call(
                                functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(VARCHAR, VARCHAR)),
                                new Reference(VARCHAR, COL_SHIP_DATE), new Constant(VARCHAR, MAX_SHIP_DATE)),
                        call(
                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                new Constant(DOUBLE, 0.05), new Reference(DOUBLE, COL_DISCOUNT)),
                        call(
                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                new Reference(DOUBLE, COL_DISCOUNT), new Constant(DOUBLE, 0.07)),
                        call(
                                functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)),
                                new Reference(DOUBLE, COL_QUANTITY), new Constant(DOUBLE, 24.0))));
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkAndColumnarFilterTpchData.class).run();
    }
}
