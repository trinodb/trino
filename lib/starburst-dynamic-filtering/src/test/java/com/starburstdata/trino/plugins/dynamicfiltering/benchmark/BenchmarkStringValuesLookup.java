/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.tpch.CustomerColumn;
import io.trino.tpch.OrderColumn;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.SliceBloomFilter;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
public class BenchmarkStringValuesLookup
{
    private static final Random RANDOM = new Random(42);

    private Set<Slice> fastHashSetFilter;
    private SliceBloomFilter sliceBloomFilter;
    private BloomFilter bloomFilter;

    @Param({
            "RANDOM_8_300K",
            "RANDOM_36_300K",
            "RANDOM_120_300K",
            "CUSTOMER_PHONE",
            "ORDERS_CLERK",
            "CUSTOMER_MARKET_SEGMENT"
    })
    private InputData inputData = InputData.RANDOM_36_300K;

    @Param("0.2")
    private double selectivity = 0.2;

    public BenchmarkStringValuesLookup()
    {
    }

    public BenchmarkStringValuesLookup(InputData inputData, double selectivity)
    {
        this.inputData = requireNonNull(inputData, "inputData is null");
        this.selectivity = selectivity;
    }

    public enum InputData
    {
        RANDOM_8_300K(() -> createRandomValues(8, 300_000)),
        RANDOM_36_300K(() -> createRandomValues(36, 300_000)),
        RANDOM_120_300K(() -> createRandomValues(120, 300_000)),
        CUSTOMER_PHONE(() -> createTpchDataSet(CUSTOMER, CustomerColumn.PHONE)), // High cardinality
        ORDERS_CLERK(() -> createTpchDataSet(ORDERS, OrderColumn.CLERK)), // High cardinality with common prefix
        CUSTOMER_MARKET_SEGMENT(() -> createTpchDataSet(CUSTOMER, CustomerColumn.MARKET_SEGMENT)); // Low cardinality

        private final List<Slice> inputValues;

        InputData(Supplier<List<Slice>> valuesDomain)
        {
            requireNonNull(valuesDomain, "valuesDomain is null");
            this.inputValues = valuesDomain.get();
        }

        public List<Slice> getInputValues()
        {
            return inputValues;
        }

        public int getInputSize()
        {
            return inputValues.size();
        }

        public List<Slice> createFilterValues(double selectivity)
        {
            // For simplicity assume that every value is equally likely
            List<Slice> uniqueValues = inputValues.stream().distinct().collect(Collectors.toList());
            int selectedValuesCount = (int) Math.ceil(uniqueValues.size() * selectivity);
            Collections.shuffle(uniqueValues, RANDOM);
            return uniqueValues.stream()
                    .limit(selectedValuesCount)
                    // Copy Slice to ensure that we don't reuse cached hashCode
                    .map(Slices::copyOf)
                    .collect(toImmutableList());
        }

        private static List<Slice> createRandomValues(int length, int numValues)
        {
            List<Slice> values = new ArrayList<>(numValues);
            for (int i = 0; i < numValues; i++) {
                byte[] buffer = new byte[length];
                RANDOM.nextBytes(buffer);
                values.add(Slices.wrappedBuffer(buffer, 0, buffer.length));
            }
            return values;
        }

        public static <E extends TpchEntity> List<Slice> createTpchDataSet(TpchTable<E> tpchTable, TpchColumn<E> column)
        {
            checkArgument(
                    column.getType().getBase() == TpchColumnType.Base.VARCHAR,
                    "Base column type must be varchar");

            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            for (E row : tpchTable.createGenerator(1, 1, 1)) {
                builder.add(Slices.utf8Slice(column.getString(row)));
            }
            return builder.build();
        }
    }

    public int getInputSize()
    {
        return inputData.getInputSize();
    }

    public int getFilterDistinctValues()
    {
        return fastHashSetFilter.size();
    }

    @Setup
    public void setup()
    {
        List<Slice> filterValues = inputData.createFilterValues(selectivity);
        fastHashSetFilter = new ObjectOpenHashSet<>(ImmutableList.copyOf(filterValues), 0.25f);
        sliceBloomFilter = new SliceBloomFilter(false, VARCHAR, ImmutableList.copyOf(filterValues));
        bloomFilter = new BloomFilter(filterValues.size(), 0.05);
        for (Slice filterValue : filterValues) {
            bloomFilter.add(filterValue);
        }
    }

    @Benchmark
    public int lookupInFastHashSet()
    {
        int hits = 0;
        for (Slice value : inputData.getInputValues()) {
            if (fastHashSetFilter.contains(value)) {
                hits++;
            }
        }
        return hits;
    }

    @Benchmark
    public int lookupInSliceBloomFilter()
    {
        int hits = 0;
        for (Slice value : inputData.getInputValues()) {
            if (sliceBloomFilter.testContains(value)) {
                hits++;
            }
        }
        return hits;
    }

    @Benchmark
    public int lookupInBloomFilter()
    {
        int hits = 0;
        for (Slice value : inputData.getInputValues()) {
            if (bloomFilter.testSlice(value)) {
                hits++;
            }
        }
        return hits;
    }

    public static void main(String[] args)
            throws Throwable
    {
        benchmark(BenchmarkStringValuesLookup.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
