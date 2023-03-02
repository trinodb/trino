/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.operator.aggregation.TypedSet;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.tpch.CustomerColumn;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.INT32_FIXED_4K;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.INT64_FIXED_16K;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.INT64_RANDOM;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.REAL_RANDOM;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.VARCHAR_DICTIONARY;
import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet.VARCHAR_RANDOM;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.aggregation.TypedSet.createEqualityTypedSet;
import static io.trino.spi.predicate.Domain.DiscreteSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static java.lang.Float.floatToIntBits;
import static java.util.Objects.requireNonNull;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 15)
public class BenchmarkDynamicPageFilter
{
    private static final int MAX_ROWS = 200_000;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final IsolatedBlockFilterFactory BLOCK_FILTER_FACTORY = new IsolatedBlockFilterFactory();

    @Param(".05")
    private double inputNullChance = 0.05;

    @Param("0.2")
    private double nonNullsSelectivity = 0.2;

    @Param({"100", "1000", "5000"})
    private int filterSize = 100;

    @Param("false")
    private boolean nullsAllowed;

    @Param({
            "INT32_RANDOM",
            "INT64_RANDOM",
            "INT32_FIXED_4K",
            "INT64_FIXED_16K",
            "REAL_RANDOM",
            "VARCHAR_RANDOM",
            "VARCHAR_DICTIONARY"
    })
    private DataSet inputDataSet;

    private TestData inputData;
    private DynamicPageFilter.BlockFilter[] blockFilters;

    public BenchmarkDynamicPageFilter()
    {
    }

    public BenchmarkDynamicPageFilter(DataSet inputDataSet)
    {
        this.inputDataSet = inputDataSet;
    }

    public enum DataSet
    {
        INT32_RANDOM(INTEGER, false, (block, r, i) -> INTEGER.writeLong(block, r.nextInt())),
        INT32_FIXED_4K(INTEGER, false, (block, r, i) -> INTEGER.writeLong(block, i % 4096)), // LongRangeFilter
        INT64_RANDOM(BIGINT, false, (block, r, i) -> BIGINT.writeLong(block, r.nextLong())),
        INT64_FIXED_16K(BIGINT, false, (block, r, i) -> BIGINT.writeLong(block, r.nextLong() % 32768)), // LongBitSetFilter
        REAL_RANDOM(REAL, false, (block, r, i) -> REAL.writeLong(block, floatToIntBits(r.nextFloat()))),
        VARCHAR_DICTIONARY(VARCHAR, true, new TpchValuesWriter<>(CUSTOMER, CustomerColumn.MARKET_SEGMENT)),
        VARCHAR_RANDOM(VARCHAR, false, (block, r, i) -> {
            byte[] buffer = new byte[25];
            r.nextBytes(buffer);
            VARCHAR.writeSlice(block, Slices.wrappedBuffer(buffer, 0, buffer.length));
        });

        private final Type type;
        private final ValueWriter valueWriter;
        private final boolean convertToDictionary;

        DataSet(Type type, boolean convertToDictionary, ValueWriter valueWriter)
        {
            this.type = requireNonNull(type, "type is null");
            this.convertToDictionary = convertToDictionary;
            this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        }

        public TupleDomain<ColumnHandle> createFilterTupleDomain(int filterSize, boolean nullsAllowed)
        {
            TestData filterValues = createSingleColumnData(valueWriter, type, 0, filterSize);
            ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
            for (Page page : filterValues.getPages()) {
                Block block = page.getBlock(0).getLoadedBlock();
                for (int position = 0; position < block.getPositionCount(); position++) {
                    valuesBuilder.add(readNativeValue(type, block, position));
                }
            }
            return TupleDomain.withColumnDomains(ImmutableMap.of(
                    new TestingColumnHandle("dummy"),
                    Domain.create(ValueSet.copyOf(type, valuesBuilder.build()), nullsAllowed)));
        }

        private TestData createInputTestData(
                TupleDomain<ColumnHandle> filter,
                double inputNullChance,
                double nonNullsSelectivity,
                long inputRows)
        {
            List<Object> nonNullValues = filter.getDomains().orElseThrow()
                    .values().stream()
                    .flatMap(domain -> {
                        DiscreteSet nullableDiscreteSet = domain.getNullableDiscreteSet();
                        return nullableDiscreteSet.getNonNullValues().stream();
                    })
                    .collect(toImmutableList());

            TestData testData = createSingleColumnData(
                    (block, r, i) -> {
                        if (isTrue(r, nonNullsSelectivity)) {
                            // pick a random value from the filter
                            Object value = nonNullValues.get(r.nextInt(nonNullValues.size()));
                            writeNativeValue(type, block, value);
                            return;
                        }
                        valueWriter.write(block, r, i);
                    },
                    type,
                    inputNullChance,
                    inputRows);
            if (convertToDictionary) {
                return convertToDictionaryBlocks(testData);
            }
            return testData;
        }
    }

    public static class TpchValuesWriter<E extends TpchEntity>
            implements ValueWriter
    {
        private final TpchTable<E> tpchTable;
        private final TpchColumn<E> column;

        private Iterator<E> generator = Collections.emptyIterator();

        public TpchValuesWriter(TpchTable<E> tpchTable, TpchColumn<E> column)
        {
            this.tpchTable = requireNonNull(tpchTable, "tpchTable is null");
            this.column = requireNonNull(column, "column is null");
        }

        @Override
        public void write(BlockBuilder blockBuilder, Random r, int index)
        {
            if (!generator.hasNext()) {
                generator = tpchTable.createGenerator(1, 1, 1).iterator();
            }
            writeToBlock(blockBuilder, generator.next(), column);
        }
    }

    private static class TestData
    {
        private final Type type;
        private final List<Page> pages;

        public TestData(Type type, List<Page> pages)
        {
            this.type = requireNonNull(type, "type is null");
            this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
        }

        public Type getType()
        {
            return type;
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Setup
    public void setup()
    {
        setup(MAX_ROWS);
    }

    public void setup(long inputRows)
    {
        // Pollute the JVM profile
        for (DataSet dataSet : ImmutableList.of(INT32_FIXED_4K, REAL_RANDOM, INT64_FIXED_16K, INT64_RANDOM, VARCHAR_RANDOM, VARCHAR_DICTIONARY)) {
            TupleDomain<ColumnHandle> filterPredicate = dataSet.createFilterTupleDomain(filterSize, nullsAllowed);
            inputData = dataSet.createInputTestData(filterPredicate, inputNullChance, nonNullsSelectivity, 50_000);
            blockFilters = DynamicPageFilter.createPageFilter(
                    filterPredicate,
                    ImmutableMap.of(new TestingColumnHandle("dummy"), 0),
                    TYPE_OPERATORS,
                    BLOCK_FILTER_FACTORY).orElseThrow();
            filterPages();
        }
        TupleDomain<ColumnHandle> filterPredicate = inputDataSet.createFilterTupleDomain(filterSize, nullsAllowed);
        inputData = inputDataSet.createInputTestData(filterPredicate, inputNullChance, nonNullsSelectivity, inputRows);
        blockFilters = DynamicPageFilter.createPageFilter(
                filterPredicate,
                ImmutableMap.of(new TestingColumnHandle("dummy"), 0),
                TYPE_OPERATORS,
                BLOCK_FILTER_FACTORY).orElseThrow();
    }

    @Benchmark
    public double filterPages()
    {
        long rowsProcessed = 0;
        long rowsFiltered = 0;
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(blockFilters.length);
        for (Page page : inputData.getPages()) {
            SelectedPositions selectedPositions = filter.filterPage(page, blockFilters, blockFilters.length).getPositions();
            int selectedPositionCount = selectedPositions.size();
            rowsProcessed += page.getPositionCount();
            rowsFiltered += page.getPositionCount() - selectedPositionCount;
        }
        return ((double) rowsFiltered / rowsProcessed) * 100;
    }

    private static TestData createSingleColumnData(ValueWriter valueWriter, Type type, double nullChance, long rows)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        Random random = new Random(32167);
        int batchSize = 1;
        BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < rows; i++) {
            if (isTrue(random, nullChance)) {
                blockBuilder.appendNull();
            }
            else {
                valueWriter.write(blockBuilder, random, i);
            }

            if (blockBuilder.getPositionCount() >= batchSize) {
                Block block = blockBuilder.build();
                pages.add(new Page(new LazyBlock(block.getPositionCount(), () -> block)));
                batchSize = Math.min(1024, batchSize * 2);
                blockBuilder = type.createBlockBuilder(null, batchSize);
            }
        }
        if (blockBuilder.getPositionCount() > 0) {
            Block block = blockBuilder.build();
            pages.add(new Page(new LazyBlock(block.getPositionCount(), () -> block)));
        }
        return new TestData(type, pages.build());
    }

    private static TestData convertToDictionaryBlocks(TestData testData)
    {
        Type type = testData.getType();
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        TypedSet typedSet = createEqualityTypedSet(
                type,
                blockTypeOperators.getEqualOperator(type),
                blockTypeOperators.getHashCodeOperator(type),
                blockBuilder,
                1024,
                "BenchmarkDynamicPageFilter");

        for (Page page : testData.getPages()) {
            Block block = page.getBlock(0).getLoadedBlock();
            for (int position = 0; position < block.getPositionCount(); position++) {
                typedSet.add(block, position);
            }
            if (typedSet.size() > 1024) {
                throw new IllegalArgumentException("Number of distinct elements is too big for dictionary");
            }
        }
        Block dictionary = blockBuilder.build();

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (Page page : testData.getPages()) {
            Block block = page.getBlock(0).getLoadedBlock();
            int[] ids = new int[block.getPositionCount()];
            for (int position = 0; position < block.getPositionCount(); position++) {
                ids[position] = typedSet.positionOf(block, position);
            }
            Block dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
            pages.add(new Page(new LazyBlock(ids.length, () -> dictionaryBlock)));
        }
        return new TestData(type, pages.build());
    }

    @FunctionalInterface
    private interface ValueWriter
    {
        void write(BlockBuilder blockBuilder, Random r, int index);
    }

    private static boolean isTrue(Random random, double chance)
    {
        double value = 0;
        // chance has to be 0 to 1 exclusive.
        while (value == 0) {
            value = random.nextDouble();
        }
        return value < chance;
    }

    private static <E extends TpchEntity> void writeToBlock(BlockBuilder blockBuilder, E row, TpchColumn<E> column)
    {
        switch (column.getType().getBase()) {
            case IDENTIFIER:
                BIGINT.writeLong(blockBuilder, column.getIdentifier(row));
                break;
            case INTEGER:
                INTEGER.writeLong(blockBuilder, column.getInteger(row));
                break;
            case DATE:
                DATE.writeLong(blockBuilder, column.getDate(row));
                break;
            case DOUBLE:
                DOUBLE.writeDouble(blockBuilder, column.getDouble(row));
                break;
            case VARCHAR:
                createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(row)));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + column.getType());
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        benchmark(BenchmarkDynamicPageFilter.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
