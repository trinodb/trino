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
package io.trino.operator.join;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import org.junit.jupiter.api.Test;
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

import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Measures the join build/probe null filtering that runs once per page per nullable join key
 * (BigintPagesHash, DefaultPagesHash, JoinProbe).
 */
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkNullablePositions
{
    @Benchmark
    public Optional<int[]> getNonNullPositions(BlockData data)
    {
        return NullablePositions.getNonNullPositions(data.blocks, data.blocks.length, data.positionCount);
    }

    @State(Scope.Thread)
    public static class BlockData
    {
        @Param({"1024", "8192"})
        private int positionCount = 8192;

        @Param({"1", "4"})
        private int blockCount = 1;

        @Param({"0", "0.01", "0.5"})
        private double nullRate;

        @Param({"VALUE", "DICTIONARY", "MIXED"})
        private String blockKind = "VALUE";

        private Block[] blocks;

        @Setup
        public void setup()
        {
            Random random = new Random(42);
            blocks = new Block[blockCount];
            for (int i = 0; i < blockCount; i++) {
                blocks[i] = switch (blockKind) {
                    case "VALUE" -> createLongBlock(random, positionCount, nullRate);
                    case "DICTIONARY" -> createDictionaryBlock(random, positionCount, nullRate);
                    // Alternates value and dictionary blocks within one page
                    case "MIXED" -> i % 2 == 0
                            ? createLongBlock(random, positionCount, nullRate)
                            : createDictionaryBlock(random, positionCount, nullRate);
                    default -> throw new IllegalArgumentException("Unknown blockKind: " + blockKind);
                };
            }
        }

        private static Block createLongBlock(Random random, int positionCount, double nullRate)
        {
            LongArrayBlockBuilder builder = new LongArrayBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                if (nullRate > 0 && random.nextDouble() < nullRate) {
                    builder.appendNull();
                }
                else {
                    builder.writeLong(random.nextLong());
                }
            }
            return builder.build();
        }

        private static Block createDictionaryBlock(Random random, int positionCount, double nullRate)
        {
            // Dictionaries are typically far smaller than the page they are referenced from
            int dictionarySize = Math.max(8, positionCount / 32);
            Block dictionary = createLongBlock(random, dictionarySize, nullRate);
            int[] ids = new int[positionCount];
            for (int position = 0; position < positionCount; position++) {
                ids[position] = random.nextInt(dictionarySize);
            }
            return DictionaryBlock.create(positionCount, dictionary, ids);
        }
    }

    @Test
    public void test()
    {
        for (String blockKind : new String[] {"VALUE", "DICTIONARY", "MIXED"}) {
            BlockData data = new BlockData();
            data.blockKind = blockKind;
            data.blockCount = 4;
            data.nullRate = 0.5;
            data.setup();
            getNonNullPositions(data);
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkNullablePositions.class).run();
    }
}
