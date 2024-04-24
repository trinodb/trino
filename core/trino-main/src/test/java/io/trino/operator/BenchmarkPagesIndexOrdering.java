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
package io.trino.operator;

import io.trino.RowPagesBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkPagesIndexOrdering
{
    private static final Random RANDOM = new Random(633969769);
    private static final int ROWS_PER_PAGE = 1024;
    private static final int NUMBER_OF_PAGES = 128;

    @State(Scope.Benchmark)
    public static class Context
    {
        @Param({"1", "10"})
        protected int numberOfChannels = 1;

        private List<Type> types;
        private List<Integer> sortChannels;
        private List<SortOrder> sortOrders;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            types = nCopies(numberOfChannels, BIGINT);
            sortChannels = IntStream.range(0, numberOfChannels).boxed().collect(toImmutableList());
            sortOrders = nCopies(numberOfChannels, ASC_NULLS_FIRST);

            RowPagesBuilder pagesBuilder = rowPagesBuilder(types);
            for (int page = 0; page < NUMBER_OF_PAGES; page++) {
                for (int row = 0; row < ROWS_PER_PAGE; row++) {
                    Object[] values = new Object[numberOfChannels];
                    for (int channel = 0; channel < numberOfChannels; channel++) {
                        values[channel] = RANDOM.nextLong();
                    }
                    pagesBuilder.row(values);
                }
                pagesBuilder.pageBreak();
            }
            pages = pagesBuilder.build();
        }
    }

    @Benchmark
    public Object benchmarkQuickSort(Context context)
    {
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(true).newPagesIndex(context.types, ROWS_PER_PAGE * NUMBER_OF_PAGES);
        context.pages.forEach(pagesIndex::addPage);
        pagesIndex.sort(context.sortChannels, context.sortOrders);
        return pagesIndex.getValueAddresses();
    }

    @Test
    public void testBenchmarkBuildHash()
    {
        Context context = new Context();
        context.setup();
        benchmarkQuickSort(context);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkPagesIndexOrdering.class).run();
    }
}
