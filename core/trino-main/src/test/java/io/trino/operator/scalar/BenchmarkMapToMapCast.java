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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.MapType;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.util.StructuralTestUtil.mapType;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapToMapCast
{
    private static final int POSITION_COUNT = 100_000;
    private static final int MAP_SIZE = 10;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(data.getPage())));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();

            List<RowExpression> projections = ImmutableList.of(new CallExpression(
                    functionResolution.getCoercion(mapType(DOUBLE, BIGINT), mapType(BIGINT, DOUBLE)),
                    ImmutableList.of(field(0, mapType(DOUBLE, BIGINT)))));

            pageProcessor = functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), projections)
                    .get();

            Block keyBlock = createKeyBlock(POSITION_COUNT, MAP_SIZE);
            Block valueBlock = createValueBlock(POSITION_COUNT, MAP_SIZE);
            Block block = createMapBlock(mapType(DOUBLE, BIGINT), POSITION_COUNT, keyBlock, valueBlock);
            page = new Page(block);
        }

        private static Block createMapBlock(MapType mapType, int positionCount, Block keyBlock, Block valueBlock)
        {
            int[] offsets = new int[positionCount + 1];
            int mapSize = keyBlock.getPositionCount() / positionCount;
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = mapSize * i;
            }
            return mapType.createBlockFromKeyValue(Optional.empty(), offsets, keyBlock, valueBlock);
        }

        private static Block createKeyBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = DOUBLE.createFixedSizeBlockBuilder(positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                DOUBLE.writeDouble(valueBlockBuilder, ThreadLocalRandom.current().nextLong());
            }
            return valueBlockBuilder.build();
        }

        private static Block createValueBlock(int positionCount, int mapSize)
        {
            BlockBuilder valueBlockBuilder = BIGINT.createFixedSizeBlockBuilder(positionCount * mapSize);
            for (int i = 0; i < positionCount * mapSize; i++) {
                BIGINT.writeLong(valueBlockBuilder, ThreadLocalRandom.current().nextLong());
            }
            return valueBlockBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkMapToMapCast().benchmark(data);

        Benchmarks.benchmark(BenchmarkMapToMapCast.class, WarmupMode.INDI).run();
    }
}
