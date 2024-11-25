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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.TpchColumn;
import io.trino.type.LikePattern;
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

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.BenchmarkParquetFormatUtils.createTpchDataSet;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.type.LikePatternType.LIKE_PATTERN;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
public class BenchmarkColumnarFilterParquetData
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final Symbol EXTENDED_PRICE_SYMBOL = new Symbol(DOUBLE, "extended_price");
    private static final Symbol DISCOUNT_SYMBOL = new Symbol(DOUBLE, "discount");
    private static final Symbol SHIP_DATE_SYMBOL = new Symbol(DATE, "ship_date");
    private static final Symbol QUANTITY_SYMBOL = new Symbol(DOUBLE, "quantity");
    private static final Symbol SHIP_MODE_SYMBOL = new Symbol(VARCHAR, "ship_mode");
    private static final Symbol COMMENT_SYMBOL = new Symbol(VARCHAR, "comment");

    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.<Symbol, Integer>builder()
            .put(EXTENDED_PRICE_SYMBOL, 0)
            .put(DISCOUNT_SYMBOL, 1)
            .put(SHIP_DATE_SYMBOL, 2)
            .put(QUANTITY_SYMBOL, 3)
            .put(SHIP_MODE_SYMBOL, 4)
            .put(COMMENT_SYMBOL, 5)
            .buildOrThrow();

    private static final Reference EXTENDED_PRICE = new Reference(DOUBLE, EXTENDED_PRICE_SYMBOL.name());
    private static final Reference DISCOUNT = new Reference(DOUBLE, DISCOUNT_SYMBOL.name());
    private static final Reference SHIP_DATE = new Reference(DATE, SHIP_DATE_SYMBOL.name());
    private static final Reference QUANTITY = new Reference(DOUBLE, QUANTITY_SYMBOL.name());
    private static final Reference SHIP_MODE = new Reference(VARCHAR, SHIP_MODE_SYMBOL.name());
    private static final Reference COMMENT = new Reference(VARCHAR, COMMENT_SYMBOL.name());

    private static final long MIN_SHIP_DATE = LocalDate.parse("1994-01-01").toEpochDay();
    private static final long MAX_SHIP_DATE = LocalDate.parse("1995-01-01").toEpochDay();
    private static final Slice SHIP = utf8Slice("MAIL");
    private static final Slice MAIL = utf8Slice("SHIP");

    private ParquetMetadata parquetMetadata;
    private ParquetDataSource dataSource;
    private List<String> columnNames;
    private List<Type> columnTypes;
    private PageProcessor compiledProcessor;

    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;

    @Param
    public FilterProvider filterProvider;

    public enum FilterProvider
    {
        AND {
            // where shipdate >= '1994-01-01'
            //    and shipdate < '1995-01-01'
            //    and discount >= 0.05
            //    and discount <= 0.07
            //    and quantity < 24;
            @Override
            Expression getExpression()
            {
                return new Logical(
                        Logical.Operator.AND,
                        ImmutableList.of(
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DATE, DATE)), new Constant(DATE, MIN_SHIP_DATE), SHIP_DATE),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DATE, DATE)), SHIP_DATE, new Constant(DATE, MAX_SHIP_DATE)),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)), new Constant(DOUBLE, 0.05), DISCOUNT),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)), DISCOUNT, new Constant(DOUBLE, 0.07)),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)), QUANTITY, new Constant(DOUBLE, 24.0))));
            }
        },
        BETWEEN {
            // where shipdate BETWEEN '1994-01-01' and '1995-01-01'
            @Override
            Expression getExpression()
            {
                return new Between(SHIP_DATE, new Constant(DATE, MIN_SHIP_DATE), new Constant(DATE, MAX_SHIP_DATE));
            }
        },
        IN {
            @Override
            Expression getExpression()
            {
                return new In(SHIP_MODE, ImmutableList.of(new Constant(VARCHAR, SHIP), new Constant(VARCHAR, MAIL)));
            }
        },
        OR {
            // where comment like 'fur%'
            //    or discount >= 0.01
            //    or quantity < 24;
            @Override
            Expression getExpression()
            {
                return new Logical(
                        Logical.Operator.OR,
                        ImmutableList.of(
                                call(FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)), COMMENT, new Constant(LIKE_PATTERN, LikePattern.compile("fur%", Optional.empty()))),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)), new Constant(DOUBLE, 0.01), DISCOUNT),
                                call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)), QUANTITY, new Constant(DOUBLE, 24.0))));
            }
        },
        /**/;

        abstract Expression getExpression();
    }

    @Setup
    public void setup()
            throws IOException
    {
        Expression filterExpression = filterProvider.getExpression();
        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        compiledProcessor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        true,
                        Optional.of(filterExpression),
                        Optional.empty(),
                        ImmutableList.of(EXTENDED_PRICE),
                        LAYOUT,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);

        List<TpchColumn<LineItem>> columns = ImmutableList.of(
                LineItemColumn.EXTENDED_PRICE,
                LineItemColumn.DISCOUNT,
                LineItemColumn.SHIP_DATE,
                LineItemColumn.QUANTITY,
                LineItemColumn.SHIP_MODE,
                LineItemColumn.COMMENT);
        BenchmarkParquetFormatUtils.TestData testData = createTpchDataSet(LINE_ITEM, columns);

        dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        testData.getColumnTypes(),
                        testData.getColumnNames(),
                        testData.getPages()),
                ParquetReaderOptions.defaultOptions());
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        columnNames = columns.stream()
                .map(TpchColumn::getColumnName)
                .collect(toImmutableList());
        columnTypes = columns.stream()
                .map(BenchmarkParquetFormatUtils::getColumnType)
                .collect(toImmutableList());
    }

    @Benchmark
    public long compiled()
            throws IOException
    {
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), columnTypes, columnNames);
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        SourcePage inputPage = reader.nextPage();
        long outputRows = 0;
        while (inputPage != null) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    null,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    inputPage);
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
            inputPage = reader.nextPage();
        }
        return outputRows;
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkColumnarFilterParquetData.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g", "--add-modules=jdk.incubator.vector"))
                .run();
    }
}
