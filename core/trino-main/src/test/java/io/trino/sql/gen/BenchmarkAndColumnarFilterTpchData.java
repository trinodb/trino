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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.SpecialForm.Form;
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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
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

    private static final Slice MIN_SHIP_DATE = utf8Slice("1994-01-01");
    private static final Slice MAX_SHIP_DATE = utf8Slice("1995-01-01");

    private Page inputPage;
    private PageProcessor processor;

    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;

    @Setup
    public void setup()
    {
        inputPage = createInputPage();

        RowExpression filterExpression = createFilterExpression(FUNCTION_RESOLUTION);
        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        List<? extends RowExpression> projections = ImmutableList.of(new InputReferenceExpression(EXTENDED_PRICE, DOUBLE));
        processor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        Optional.of(filterExpression),
                        Optional.empty(),
                        projections,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
    }

    @Benchmark
    public List<Optional<Page>> compiled()
    {
        return ImmutableList.copyOf(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(inputPage)));
    }

    private static Page createInputPage()
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, DOUBLE));
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < 10_000; i++) {
            pageBuilder.declarePosition();

            LineItem lineItem = iterator.next();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.extendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.discount());
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(SHIP_DATE), Slices.wrappedBuffer(LineItemColumn.SHIP_DATE.getString(lineItem).getBytes(UTF_8)));
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.quantity());
        }
        return pageBuilder.build();
    }

    // where shipdate >= '1994-01-01'
    //    and shipdate < '1995-01-01'
    //    and discount >= 0.05
    //    and discount <= 0.07
    //    and quantity < 24;
    private static RowExpression createFilterExpression(TestingFunctionResolution functionResolution)
    {
        return new SpecialForm(
                Form.AND,
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression(
                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(VARCHAR, VARCHAR)),
                                ImmutableList.of(constant(MIN_SHIP_DATE, VARCHAR), field(SHIP_DATE, VARCHAR))),
                        new SpecialForm(
                                Form.AND,
                                BOOLEAN,
                                ImmutableList.of(
                                        new CallExpression(
                                                functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(VARCHAR, VARCHAR)),
                                                ImmutableList.of(field(SHIP_DATE, VARCHAR), constant(MAX_SHIP_DATE, VARCHAR))),
                                        new SpecialForm(
                                                Form.AND,
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new CallExpression(
                                                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                ImmutableList.of(constant(0.05, DOUBLE), field(DISCOUNT, DOUBLE))),
                                                        new SpecialForm(
                                                                Form.AND,
                                                                BOOLEAN,
                                                                ImmutableList.of(
                                                                        new CallExpression(
                                                                                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                                ImmutableList.of(field(DISCOUNT, DOUBLE), constant(0.07, DOUBLE))),
                                                                        new CallExpression(
                                                                                functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                                ImmutableList.of(field(QUANTITY, DOUBLE), constant(24.0, DOUBLE)))),
                                                                ImmutableList.of())),
                                                ImmutableList.of())),
                                ImmutableList.of())),
                ImmutableList.of());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkAndColumnarFilterTpchData.class).run();
    }
}
