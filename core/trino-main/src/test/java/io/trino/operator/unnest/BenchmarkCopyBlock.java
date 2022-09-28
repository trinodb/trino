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
package io.trino.operator.unnest;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCopyBlock
{
    private static final int POSITIONS_PER_PAGE = 10000;
    private static final int BLOCK_COUNT = 100;

    @Benchmark
    public Block[] copyBlockByLoop(BenchmarkData data)
    {
        Block[] blocks = new Block[BLOCK_COUNT];
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = data.blocks.get(i);
            int positionCount = block.getPositionCount();

            long[] values = new long[positionCount];
            for (int j = 0; j < positionCount; j++) {
                values[j] = block.getLong(j, 0);
            }

            boolean[] valueIsNull = copyIsNulls(block);
            blocks[i] = new LongArrayBlock(positionCount, Optional.of(valueIsNull), values);
        }

        return blocks;
    }

    @Benchmark
    public Block[] copyBlockByAppend(BenchmarkData data)
    {
        Block[] blocks = new Block[BLOCK_COUNT];
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = data.blocks.get(i);
            int positionCount = block.getPositionCount();

            LongArrayBlockBuilder longArrayBlockBuilder = new LongArrayBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int j = 0; j < positionCount; j++) {
                BIGINT.appendTo(block, j, longArrayBlockBuilder);
            }

            blocks[i] = longArrayBlockBuilder.build();
        }

        return blocks;
    }

    private static boolean[] copyIsNulls(Block block)
    {
        int positionCount = block.getPositionCount();
        boolean[] valueIsNull = new boolean[positionCount + 1];
        if (block.mayHaveNull()) {
            for (int i = 0; i < positionCount; i++) {
                valueIsNull[i] = block.isNull(i);
            }
        }
        return valueIsNull;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({"0.0", "0.2"})
        private float nullsRatio;

        List<Block> blocks = new ArrayList<>();

        @Setup
        public void setup()
        {
            for (int i = 0; i < BLOCK_COUNT; i++) {
                blocks.add(createRandomLongsBlock(POSITIONS_PER_PAGE, nullsRatio));
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkCopyBlock.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    private static Block createRandomLongsBlock(int positionCount, double nullRate)
    {
        if (nullRate < 0 || nullRate > 1) {
            throw new IllegalArgumentException(format("nullRate %f is not valid.", nullRate));
        }

        return createLongsBlock(IntStream.range(0, positionCount)
                .mapToObj(i -> {
                    if (ThreadLocalRandom.current().nextDouble(1) < nullRate) {
                        return null;
                    }
                    return ThreadLocalRandom.current().nextLong();
                })
                .collect(Collectors.toList()));
    }
}
