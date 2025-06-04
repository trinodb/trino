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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.tpch.LineItem;
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
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkGroupedTopNRowNumberBuilder
{
    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final List<Type> types = ImmutableList.of(DOUBLE, DOUBLE, DATE, DOUBLE);

        @Param({"1", "10", "100"})
        private int topN = 1;

        @Param({"10000", "1000000"})
        private int positions = 1000;

        // when positions is evenly divisible by groupCount, each row will end up in the same group on each processPage call,
        // which means it will stop inserting after topN is saturated which may or may not be desirable for any given benchmark scenario
        @Param({"1", "10000", "1000000"})
        private int groupCount = 1;

        @Param("100")
        private int addPageCalls = 1;

        private PageWithPositionComparator comparator;
        private Page page;

        @Setup
        public void setup()
        {
            OrderingCompiler orderingCompiler = new OrderingCompiler(new TypeOperators());
            List<Integer> sortColumns = ImmutableList.of(EXTENDED_PRICE, SHIP_DATE);
            List<Type> sortTypes = sortColumns.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            List<SortOrder> sortOrders = ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST);
            comparator = orderingCompiler.compilePageWithPositionComparator(sortTypes, sortColumns, sortOrders);
            page = createInputPage(positions, types);
        }

        public GroupedTopNRowNumberBuilder newTopNRowNumberBuilder()
        {
            return new GroupedTopNRowNumberBuilder(types, comparator, topN, false, new int[0], new CyclingGroupByHash(groupCount));
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Benchmark
    public long processTopNInput(BenchmarkData data)
    {
        GroupedTopNRowNumberBuilder topNBuilder = data.newTopNRowNumberBuilder();
        Page inputPage = data.getPage();
        for (int i = 0; i < data.addPageCalls; i++) {
            if (!topNBuilder.processPage(inputPage).process()) {
                throw new IllegalStateException("Work did not complete");
            }
        }
        return topNBuilder.getEstimatedSizeInBytes();
    }

    @Benchmark
    public List<Page> topN(BenchmarkData data)
    {
        GroupedTopNRowNumberBuilder topNBuilder = data.newTopNRowNumberBuilder();
        Page inputPage = data.getPage();
        for (int i = 0; i < data.addPageCalls; i++) {
            if (!topNBuilder.processPage(inputPage).process()) {
                throw new IllegalStateException("Work did not complete");
            }
        }
        return ImmutableList.copyOf(topNBuilder.buildResult());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        BenchmarkGroupedTopNRowNumberBuilder benchmark = new BenchmarkGroupedTopNRowNumberBuilder();
        benchmark.topN(data);
        benchmark.processTopNInput(data);

        benchmark(BenchmarkGroupedTopNRowNumberBuilder.class).run();
    }

    private static Page createInputPage(int positions, List<Type> types)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < positions; i++) {
            pageBuilder.declarePosition();

            LineItem lineItem = iterator.next();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.extendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.discount());
            DATE.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), lineItem.shipDate());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.quantity());
        }
        return pageBuilder.build();
    }
}
