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
import com.google.common.collect.ImmutableMap;
import io.trino.FullConnectorSession;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.FilterEvaluator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Domain.DiscreteSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.DynamicFiltersTestUtil.createDynamicFilterEvaluator;
import static java.lang.Float.floatToIntBits;
import static java.util.Objects.requireNonNull;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDynamicPageFilter
{
    private static final int MAX_ROWS = 200_000;
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    @Param("0.05")
    public double inputNullChance = 0.05;

    @Param("0.2")
    public double nonNullsSelectivity = 0.2;

    @Param({"100", "1000", "5000"})
    public int filterSize = 100;

    @Param("false")
    public boolean nullsAllowed;

    @Param({
            "INT32_RANDOM",
            "INT64_RANDOM",
            "INT64_FIXED_32K", // LongBitSetFilter
            "REAL_RANDOM",
    })
    public DataSet inputDataSet;

    private List<Page> inputData;
    private FilterEvaluator filterEvaluator;

    public enum DataSet
    {
        INT32_RANDOM(INTEGER, (block, r) -> INTEGER.writeLong(block, r.nextInt())),
        INT64_RANDOM(BIGINT, (block, r) -> BIGINT.writeLong(block, r.nextLong())),
        INT64_FIXED_32K(BIGINT, (block, r) -> BIGINT.writeLong(block, r.nextLong() % 32768)),
        REAL_RANDOM(REAL, (block, r) -> REAL.writeLong(block, floatToIntBits(r.nextFloat()))),
        /**/;

        private final Type type;
        private final ValueWriter valueWriter;

        DataSet(Type type, ValueWriter valueWriter)
        {
            this.type = requireNonNull(type, "type is null");
            this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        }

        public TupleDomain<ColumnHandle> createFilterTupleDomain(int filterSize, boolean nullsAllowed)
        {
            List<Page> filterValues = createSingleColumnData(valueWriter, type, 0, filterSize);
            ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
            for (Page page : filterValues) {
                Block block = page.getBlock(0).getLoadedBlock();
                for (int position = 0; position < block.getPositionCount(); position++) {
                    valuesBuilder.add(readNativeValue(type, block, position));
                }
            }
            return TupleDomain.withColumnDomains(ImmutableMap.of(
                    new TestingColumnHandle("dummy"),
                    Domain.create(ValueSet.copyOf(type, valuesBuilder.build()), nullsAllowed)));
        }

        private List<Page> createInputTestData(
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

            // pick a random value from the filter
            return createSingleColumnData(
                    (block, r) -> {
                        if (isTrue(r, nonNullsSelectivity)) {
                            // pick a random value from the filter
                            Object value = nonNullValues.get(r.nextInt(nonNullValues.size()));
                            writeNativeValue(type, block, value);
                            return;
                        }
                        valueWriter.write(block, r);
                    },
                    type,
                    inputNullChance,
                    inputRows);
        }
    }

    @Setup
    public void setup()
    {
        TupleDomain<ColumnHandle> filterPredicate = inputDataSet.createFilterTupleDomain(filterSize, nullsAllowed);
        inputData = inputDataSet.createInputTestData(filterPredicate, inputNullChance, nonNullsSelectivity, MAX_ROWS);
        filterEvaluator = createDynamicFilterEvaluator(
                filterPredicate,
                ImmutableMap.of(new TestingColumnHandle("dummy"), 0),
                1);
    }

    @Benchmark
    public double filterPages()
    {
        long rowsProcessed = 0;
        long rowsFiltered = 0;
        for (Page page : inputData) {
            FilterEvaluator.SelectionResult result = filterEvaluator.evaluate(FULL_CONNECTOR_SESSION, positionsRange(0, page.getPositionCount()), page);
            SelectedPositions selectedPositions = result.selectedPositions();
            int selectedPositionCount = selectedPositions.size();
            rowsProcessed += page.getPositionCount();
            rowsFiltered += page.getPositionCount() - selectedPositionCount;
        }
        return ((double) rowsFiltered / rowsProcessed) * 100;
    }

    private static List<Page> createSingleColumnData(ValueWriter valueWriter, Type type, double nullChance, long rows)
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
                valueWriter.write(blockBuilder, random);
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
        return pages.build();
    }

    @FunctionalInterface
    private interface ValueWriter
    {
        void write(BlockBuilder blockBuilder, Random r);
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

    public static void main(String[] args)
            throws Throwable
    {
        benchmark(BenchmarkDynamicPageFilter.class, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
