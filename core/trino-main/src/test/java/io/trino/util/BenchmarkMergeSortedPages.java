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
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Merges sources that were sorted independently, so their rows interleave like in a distributed sort.
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkMergeSortedPages
{
    private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler(new TypeOperators());

    @Param({"2", "16", "64"})
    private int sources = 16;

    @Param
    private Keys keys = Keys.BIGINT_RANDOM;

    @Param("2000000")
    private int rows = 2_000_000;

    private List<Type> types;
    private PageWithPositionComparator comparator;
    private List<List<Page>> sortedPages;

    public enum Keys
    {
        /**
         * Nearly all keys differ, so consecutive output rows rarely come from the same source
         */
        BIGINT_RANDOM(BIGINT),
        /**
         * Every key repeats many times in every source
         */
        BIGINT_FEW_DISTINCT(BIGINT),
        VARCHAR_RANDOM(VARCHAR),
        /**
         * Keys differ only after their first nine bytes
         */
        VARCHAR_COMMON_PREFIX(VARCHAR);

        private final Type type;

        Keys(Type type)
        {
            this.type = type;
        }

        Supplier<Object> generator(Random random)
        {
            return switch (this) {
                case BIGINT_RANDOM -> random::nextLong;
                case BIGINT_FEW_DISTINCT -> () -> (long) random.nextInt(1000);
                case VARCHAR_RANDOM -> () -> randomLetters(random, 16);
                case VARCHAR_COMMON_PREFIX -> () -> "Customer#%09d".formatted(random.nextInt(1_000_000_000));
            };
        }
    }

    @Setup
    public void setup()
    {
        types = ImmutableList.of(keys.type, BIGINT);
        comparator = ORDERING_COMPILER.compilePageWithPositionComparator(ImmutableList.of(keys.type), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_LAST));

        Random random = new Random(42);
        Supplier<Object> generator = keys.generator(random);
        ImmutableList.Builder<List<Page>> sortedPages = ImmutableList.builder();
        for (int source = 0; source < sources; source++) {
            Object[] sourceKeys = new Object[rows / sources];
            for (int row = 0; row < sourceKeys.length; row++) {
                sourceKeys[row] = generator.get();
            }
            // strings have only ASCII characters, so they are ordered like their UTF-8 bytes
            Arrays.sort(sourceKeys);
            sortedPages.add(toPages(sourceKeys, random));
        }
        this.sortedPages = sortedPages.build();
    }

    private List<Page> toPages(Object[] sourceKeys, Random random)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(types);
        for (Object key : sourceKeys) {
            pageBuilder.declarePosition();
            switch (key) {
                case Long value -> BIGINT.writeLong(pageBuilder.getBlockBuilder(0), value);
                case String value -> VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), utf8Slice(value));
                default -> throw new IllegalArgumentException("Unsupported key: " + key);
            }
            BIGINT.writeLong(pageBuilder.getBlockBuilder(1), random.nextLong());
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

    private static String randomLetters(Random random, int length)
    {
        StringBuilder letters = new StringBuilder(length);
        for (int index = 0; index < length; index++) {
            letters.append((char) ('a' + random.nextInt(26)));
        }
        return letters.toString();
    }

    @Benchmark
    public List<Page> mergeSortedPages()
    {
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                sortedPages.stream()
                        .map(WorkProcessor::fromIterable)
                        .collect(toImmutableList()),
                comparator,
                types,
                newSimpleAggregatedMemoryContext(),
                new DriverYieldSignal());

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        while (true) {
            mergedPages.process();
            if (mergedPages.isFinished()) {
                return pages.build();
            }
            pages.add(mergedPages.getResult());
        }
    }

    @Test
    public void verifyMergeSortedPages()
    {
        for (Keys keys : Keys.values()) {
            this.keys = keys;
            this.sources = 3;
            this.rows = 30_000;
            setup();

            int positionCount = mergeSortedPages().stream()
                    .mapToInt(Page::getPositionCount)
                    .sum();
            assertThat(positionCount).isEqualTo(rows);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkMergeSortedPages.class).run();
    }
}
