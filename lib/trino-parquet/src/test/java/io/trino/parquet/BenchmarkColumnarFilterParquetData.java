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
import io.airlift.slice.Slice;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
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
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.SpecialForm.Form;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.TpchColumn;
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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.tpch.TpchTable.LINE_ITEM;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
public class BenchmarkColumnarFilterParquetData
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;
    private static final int SHIP_MODE = 4;

    private static final long MIN_SHIP_DATE = LocalDate.parse("1994-01-01").toEpochDay();
    private static final long MAX_SHIP_DATE = LocalDate.parse("1995-01-01").toEpochDay();
    private static final Slice SHIP = utf8Slice("MAIL");
    private static final Slice MAIL = utf8Slice("SHIP");
    private static final ResolvedFunction EQUALS = FUNCTION_RESOLUTION.getMetadata().resolveOperator(OperatorType.EQUAL, ImmutableList.of(VARCHAR, VARCHAR));
    private static final ResolvedFunction HASH_CODE = FUNCTION_RESOLUTION.getMetadata().resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(VARCHAR));

    private ParquetMetadata parquetMetadata;
    private ParquetDataSource dataSource;
    private List<String> columnNames;
    private List<Type> columnTypes;
    private PageProcessor compiledProcessor;

    @Param({"true", "false"})
    public boolean columnarEvaluationEnabled;

    @Param({
            "AND",
            "BETWEEN",
            "IN",
    })
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
            RowExpression getExpression()
            {
                return new SpecialForm(
                        Form.AND,
                        BOOLEAN,
                        ImmutableList.of(
                                new CallExpression(
                                        FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DATE, DATE)),
                                        ImmutableList.of(constant(MIN_SHIP_DATE, DATE), field(SHIP_DATE, DATE))),
                                new SpecialForm(
                                        Form.AND,
                                        BOOLEAN,
                                        ImmutableList.of(
                                                new CallExpression(
                                                        FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DATE, DATE)),
                                                        ImmutableList.of(field(SHIP_DATE, DATE), constant(MAX_SHIP_DATE, DATE))),
                                                new SpecialForm(
                                                        Form.AND,
                                                        BOOLEAN,
                                                        ImmutableList.of(
                                                                new CallExpression(
                                                                        FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                        ImmutableList.of(constant(0.05, DOUBLE), field(DISCOUNT, DOUBLE))),
                                                                new SpecialForm(
                                                                        Form.AND,
                                                                        BOOLEAN,
                                                                        ImmutableList.of(
                                                                                new CallExpression(
                                                                                        FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                                        ImmutableList.of(field(DISCOUNT, DOUBLE), constant(0.07, DOUBLE))),
                                                                                new CallExpression(
                                                                                        FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)),
                                                                                        ImmutableList.of(field(QUANTITY, DOUBLE), constant(24.0, DOUBLE)))),
                                                                        ImmutableList.of())),
                                                        ImmutableList.of())),
                                        ImmutableList.of())),
                        ImmutableList.of());
            }
        },
        BETWEEN {
            // where shipdate BETWEEN '1994-01-01' and '1995-01-01'
            @Override
            RowExpression getExpression()
            {
                return new SpecialForm(
                        Form.BETWEEN,
                        BOOLEAN,
                        ImmutableList.of(field(SHIP_DATE, DATE), constant(MIN_SHIP_DATE, DATE), constant(MAX_SHIP_DATE, DATE)),
                        ImmutableList.of(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DATE, DATE))));
            }
        },
        IN {
            @Override
            RowExpression getExpression()
            {
                Metadata metadata = FUNCTION_RESOLUTION.getMetadata();
                List<ResolvedFunction> functionalDependencies = ImmutableList.of(
                        EQUALS,
                        HASH_CODE,
                        metadata.resolveOperator(OperatorType.INDETERMINATE, ImmutableList.of(VARCHAR)));
                return new SpecialForm(
                        Form.IN,
                        BOOLEAN,
                        ImmutableList.of(field(SHIP_MODE, VARCHAR), constant(SHIP, VARCHAR), constant(MAIL, VARCHAR)),
                        functionalDependencies);
            }
        }
        /**/;

        abstract RowExpression getExpression();
    }

    @Setup
    public void setup()
            throws IOException
    {
        RowExpression filterExpression = filterProvider.getExpression();
        ExpressionCompiler expressionCompiler = FUNCTION_RESOLUTION.getExpressionCompiler();
        compiledProcessor = expressionCompiler.compilePageProcessor(
                        columnarEvaluationEnabled,
                        Optional.of(filterExpression),
                        Optional.empty(),
                        ImmutableList.of(new InputReferenceExpression(EXTENDED_PRICE, DOUBLE)),
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);

        List<TpchColumn<LineItem>> columns = ImmutableList.of(
                LineItemColumn.EXTENDED_PRICE,
                LineItemColumn.DISCOUNT,
                LineItemColumn.SHIP_DATE,
                LineItemColumn.QUANTITY,
                LineItemColumn.SHIP_MODE);
        BenchmarkParquetFormatUtils.TestData testData = createTpchDataSet(LINE_ITEM, columns);

        dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        testData.getColumnTypes(),
                        testData.getColumnNames(),
                        testData.getPages()),
                new ParquetReaderOptions());
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
        Page inputPage = reader.nextPage();
        long outputRows = 0;
        while (inputPage != null) {
            WorkProcessor<Page> workProcessor = compiledProcessor.createWorkProcessor(
                    null,
                    new DriverYieldSignal(),
                    context,
                    new PageProcessorMetrics(),
                    SourcePage.create(inputPage));
            if (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
            inputPage = reader.nextPage();
        }
        return outputRows;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkColumnarFilterParquetData.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g", "--add-modules=jdk.incubator.vector"))
                .run();
    }
}
