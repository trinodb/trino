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
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.spi.type.TypeOperators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongBitSetFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongCustomHashSetFilter;
import static com.starburstdata.trino.plugins.dynamicfiltering.TupleDomainFilter.LongHashSetFilter;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongValuesLookup
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Benchmark
    public int lookupInCustomHashSet(BenchmarkData data)
    {
        int hits = 0;
        for (Integer value : data.inputValues) {
            if (data.customHashSetFilter.testContains(value)) {
                hits++;
            }
        }

        return hits;
    }

    @Benchmark
    public int lookupInHashSet(BenchmarkData data)
    {
        int hits = 0;
        for (Integer value : data.inputValues) {
            if (data.hashSetFilter.testContains(value)) {
                hits++;
            }
        }

        return hits;
    }

    @Benchmark
    public int lookupInBitSet(BenchmarkData data)
    {
        int hits = 0;
        for (Integer value : data.inputValues) {
            if (data.bitSetFilter.testContains(value)) {
                hits++;
            }
        }

        return hits;
    }

    @Benchmark
    public int lookupInBloomFilter(BenchmarkData data)
    {
        int hits = 0;
        for (Integer value : data.inputValues) {
            if (data.bloomFilter.testLong(value)) {
                hits++;
            }
        }

        return hits;
    }

    @State(Thread)
    public static class BenchmarkData
    {
        private LongCustomHashSetFilter customHashSetFilter;
        private LongHashSetFilter hashSetFilter;
        private LongBitSetFilter bitSetFilter;
        private BloomFilter bloomFilter;

        @Param({"5000", "1000", "100", "10"})
        private int filterSize;

        private List<Integer> inputValues;

        @Setup
        public void setup()
                throws Exception
        {
            List<Long> values = new ArrayList<>(filterSize);
            bloomFilter = new BloomFilter(filterSize, 0.05);
            Random random = new Random(42);
            int upperBound = filterSize * 4;
            for (int i = 0; i < filterSize; i++) {
                long value = random.nextInt(upperBound);
                values.add(value);
                bloomFilter.addLong(value);
            }
            MethodHandle hashCodeHandle = TYPE_OPERATORS.getHashCodeOperator(BIGINT, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            MethodHandle equalsHandle = TYPE_OPERATORS.getEqualOperator(BIGINT, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            customHashSetFilter = new LongCustomHashSetFilter(false, BIGINT, hashCodeHandle, equalsHandle, ImmutableList.copyOf(values));
            hashSetFilter = new LongHashSetFilter(false, BIGINT, ImmutableList.copyOf(values), 0, upperBound);
            bitSetFilter = new LongBitSetFilter(false, BIGINT, ImmutableList.copyOf(values), 0, upperBound);
            inputValues = createRandomInputValues(random, upperBound, 100_000);
        }

        private static List<Integer> createRandomInputValues(Random random, int bound, int numValues)
        {
            return IntStream.range(0, numValues).boxed()
                    .map(i -> random.nextInt(bound))
                    .collect(toImmutableList());
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        benchmark(BenchmarkLongValuesLookup.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
