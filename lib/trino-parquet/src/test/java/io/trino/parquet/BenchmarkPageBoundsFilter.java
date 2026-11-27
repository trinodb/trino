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
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.WorkProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
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

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.ObjLongConsumer;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.IrExpressions.call;
import static java.util.Objects.requireNonNull;

/**
 * Reads the columns of TPC-H query 6 from a Parquet file through the filter and projection of a scan, with and
 * without the reader declaring the bounds of each row group, see {@link SourcePage#getValueBounds(int)}.
 * <p>
 * With {@code readerPushdown} the ship date predicate is given to the reader in both cases, so row groups and
 * pages that statistics rule out are never read. What the bounds add then is that row groups lying inside the
 * predicate skip the ship date filter and, with it, the decoding of the ship date column. Without it the reader
 * knows no predicate, as with a dynamic filter that arrives after the reader was opened, and the bounds also drop
 * the row groups lying outside the predicate before any column is read.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class BenchmarkPageBoundsFilter
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final List<String> COLUMN_NAMES = ImmutableList.of("extended_price", "discount", "ship_date", "quantity");
    private static final List<Type> COLUMN_TYPES = ImmutableList.of(DOUBLE, DOUBLE, DATE, DOUBLE);

    private static final Reference EXTENDED_PRICE = new Reference(DOUBLE, "extended_price");
    private static final Reference DISCOUNT = new Reference(DOUBLE, "discount");
    private static final Reference SHIP_DATE = new Reference(DATE, "ship_date");
    private static final Reference QUANTITY = new Reference(DOUBLE, "quantity");

    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(
            Symbol.from(EXTENDED_PRICE), 0,
            Symbol.from(DISCOUNT), 1,
            Symbol.from(SHIP_DATE), 2,
            Symbol.from(QUANTITY), 3);

    private static final long MIN_SHIP_DATE = LocalDate.parse("1994-01-01").toEpochDay();
    private static final long MAX_SHIP_DATE = LocalDate.parse("1995-01-01").toEpochDay();

    private static final TupleDomain<String> SHIP_DATE_PREDICATE = TupleDomain.withColumnDomains(ImmutableMap.of(
            "ship_date", Domain.create(ValueSet.ofRanges(Range.range(DATE, MIN_SHIP_DATE, true, MAX_SHIP_DATE, false)), false)));

    @Param("1")
    public double scaleFactor = 1;

    @Param
    public RowOrder rowOrder = RowOrder.SHIP_DATE;

    @Param
    public Query query = Query.Q6;

    @Param({"10000", "100000", "1000000"})
    public int rowGroupRows = 10_000;

    @Param({"true", "false"})
    public boolean readerPushdown = true;

    @Param({"false", "true"})
    public boolean pageBounds;

    private ParquetMetadata parquetMetadata;
    private ParquetDataSource dataSource;
    private PageProcessor processor;

    public enum RowOrder
    {
        /**
         * The order of the generator, in which every row group spans all ship dates
         */
        GENERATED,
        SHIP_DATE,
    }

    public enum Query
    {
        // select extended_price where ship_date >= date '1994-01-01' and ship_date < date '1995-01-01'
        SHIP_DATE_RANGE {
            @Override
            List<Expression> filterTerms()
            {
                return ImmutableList.of(
                        call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DATE, DATE)), new Constant(DATE, MIN_SHIP_DATE), SHIP_DATE),
                        call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DATE, DATE)), SHIP_DATE, new Constant(DATE, MAX_SHIP_DATE)));
            }

            @Override
            Expression projection()
            {
                return EXTENDED_PRICE;
            }
        },
        // select extended_price * discount where ship_date >= date '1994-01-01' and ship_date < date '1995-01-01'
        // and discount >= 0.05 and discount <= 0.07 and quantity < 24
        Q6 {
            @Override
            List<Expression> filterTerms()
            {
                return ImmutableList.<Expression>builder()
                        .addAll(SHIP_DATE_RANGE.filterTerms())
                        .add(call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)), new Constant(DOUBLE, 0.05), DISCOUNT))
                        .add(call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)), DISCOUNT, new Constant(DOUBLE, 0.07)))
                        .add(call(FUNCTION_RESOLUTION.resolveOperator(LESS_THAN, ImmutableList.of(DOUBLE, DOUBLE)), QUANTITY, new Constant(DOUBLE, 24.0)))
                        .build();
            }

            @Override
            Expression projection()
            {
                return call(FUNCTION_RESOLUTION.resolveOperator(MULTIPLY, ImmutableList.of(DOUBLE, DOUBLE)), EXTENDED_PRICE, DISCOUNT);
            }
        },
        /**/;

        abstract List<Expression> filterTerms();

        abstract Expression projection();
    }

    @Setup
    public void setup()
            throws IOException
    {
        processor = FUNCTION_RESOLUTION.getExpressionCompiler().compilePageProcessor(
                        getCharVarcharCoercion(TEST_SESSION),
                        true,
                        true,
                        Optional.of(new Logical(Logical.Operator.AND, query.filterTerms())),
                        Optional.empty(),
                        ImmutableList.of(query.projection()),
                        LAYOUT,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);

        dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxRowGroupRowCount(rowGroupRows)
                                .build(),
                        COLUMN_TYPES,
                        COLUMN_NAMES,
                        createPages()),
                ParquetReaderOptions.defaultOptions());
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
    }

    @Benchmark
    public long scan()
            throws IOException
    {
        ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                COLUMN_TYPES,
                COLUMN_NAMES,
                readerPushdown ? SHIP_DATE_PREDICATE : TupleDomain.all());
        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        long outputRows = 0;
        for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
            WorkProcessor<Page> workProcessor = processor.createWorkProcessor(
                    null,
                    context,
                    new PageProcessorMetrics(),
                    new BoundsControlSourcePage(page, pageBounds));
            while (workProcessor.process() && !workProcessor.isFinished()) {
                outputRows += workProcessor.getResult().getPositionCount();
            }
        }
        reader.close();
        return outputRows;
    }

    public int rowGroupCount()
            throws IOException
    {
        return parquetMetadata.getBlocks().size();
    }

    private List<Page> createPages()
    {
        List<LineItem> lineItems = new ArrayList<>();
        new LineItemGenerator(scaleFactor, 1, 1).forEach(lineItems::add);
        if (rowOrder == RowOrder.SHIP_DATE) {
            lineItems.sort(Comparator.comparingInt(LineItem::shipDate));
        }

        PageBuilder pageBuilder = new PageBuilder(COLUMN_TYPES);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (LineItem lineItem : lineItems) {
            pageBuilder.declarePosition();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), lineItem.extendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(1), lineItem.discount());
            DATE.writeLong(pageBuilder.getBlockBuilder(2), lineItem.shipDate());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(3), lineItem.quantity());
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages.build();
    }

    /**
     * Passes everything through to the page of the reader, except that it can withhold the bounds. Both sides of
     * the comparison go through it, so the indirection costs them the same.
     */
    private static final class BoundsControlSourcePage
            implements SourcePage
    {
        private final SourcePage delegate;
        private final boolean exposeBounds;

        public BoundsControlSourcePage(SourcePage delegate, boolean exposeBounds)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.exposeBounds = exposeBounds;
        }

        @Override
        public Optional<Domain> getValueBounds(int channel)
        {
            if (!exposeBounds) {
                return Optional.empty();
            }
            return delegate.getValueBounds(channel);
        }

        @Override
        public int getPositionCount()
        {
            return delegate.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return delegate.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return delegate.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            delegate.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return delegate.getChannelCount();
        }

        @Override
        public Block getBlock(int channel)
        {
            return delegate.getBlock(channel);
        }

        @Override
        public Page getPage()
        {
            return delegate.getPage();
        }

        @Override
        public boolean trySelectPositions(int[] positions, int offset, int size)
        {
            return delegate.trySelectPositions(positions, offset, size);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            return delegate.getColumns(channels);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            delegate.selectPositions(positions, offset, size);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkPageBoundsFilter.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx6g", "-Xms6g", "--add-modules=jdk.incubator.vector"))
                .run();
    }
}
