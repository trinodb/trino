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
package io.trino.block;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.MapType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10)
@Fork(10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapCopy
{
    private static final int POSITIONS = 100_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public BlockBuilder benchmarkMapCopy(BenchmarkData data)
    {
        Block block = data.getDataBlock();
        BlockBuilder blockBuilder = data.getBlockBuilder();
        MapType mapType = mapType(VARCHAR, BIGINT);

        for (int i = 0; i < POSITIONS; i++) {
            mapType.appendTo(block, i, blockBuilder);
        }

        return blockBuilder;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "2", "4", "8", "16"})
        private int mapSize;

        private Block dataBlock;
        private MapBlockBuilder blockBuilder;

        @Setup
        public void setup()
        {
            MapType mapType = mapType(VARCHAR, BIGINT);
            blockBuilder = mapType.createBlockBuilder(null, POSITIONS);
            for (int position = 0; position < POSITIONS; position++) {
                blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (int i = 0; i < mapSize; i++) {
                        VARCHAR.writeString(keyBuilder, String.valueOf(ThreadLocalRandom.current().nextInt()));
                        BIGINT.writeLong(valueBuilder, ThreadLocalRandom.current().nextInt());
                    }
                });
            }

            dataBlock = blockBuilder.build();
        }

        public Block getDataBlock()
        {
            return dataBlock;
        }

        public BlockBuilder getBlockBuilder()
        {
            return blockBuilder.newBlockBuilderLike(null);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkMapCopy().benchmarkMapCopy(data);

        benchmark(BenchmarkMapCopy.class).run();
    }
}
