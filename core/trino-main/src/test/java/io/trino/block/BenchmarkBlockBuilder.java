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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
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

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBlockBuilder
{
    public static final int POSITIONS = 1024;
    public static final int MAX_STRING_LENGTH = 64;

    @Benchmark
    public Block benchmarkAppendRange(BlockData data)
    {
        BlockBuilder builder = data.trinoType.createBlockBuilder(null, POSITIONS);
        // Append twice to simulate "merging" two adjacent blocks
        builder.appendRange(data.valueBlock, data.offset, data.length);
        builder.appendRange(data.valueBlock, data.offset, data.length);
        return builder.build();
    }

    @State(Scope.Thread)
    public static class BlockData
    {
        @Param({"VARCHAR", "BIGINT"})
        private String type = "BIGINT";
        private Type trinoType;
        @Param({"0", "0.1", "0.25", "0.5", "0.75", "0.9", "1.0"})
        private float nullRate = 0.5f;
        private ValueBlock valueBlock;
        private int offset;
        private int length;
        private Random random = new Random(1024);

        @Setup
        public void setup()
        {
            offset = 1;
            length = POSITIONS;
            switch (this.type) {
                case "VARCHAR" -> {
                    trinoType = VARCHAR;
                    valueBlock = createVarcharBlock(random, offset, length, nullRate);
                }
                case "BIGINT" -> {
                    trinoType = BIGINT;
                    valueBlock = createBigintBlock(random, offset, length, nullRate);
                }
                default -> throw new IllegalArgumentException("Unsupported type: " + this.type);
            }
        }

        private static ValueBlock createBigintBlock(Random random, int offset, int length, float nullRate)
        {
            BlockBuilder builder = BIGINT.createBlockBuilder(null, offset + length);
            int position = 0;
            // Fill the beginning range with nulls to prevent the isNull array from being suppressed
            for (; position < offset; position++) {
                builder.appendNull();
            }
            for (; position < offset + length; position++) {
                if (random.nextFloat() <= nullRate) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, random.nextLong());
                }
            }
            return builder.buildValueBlock();
        }

        private static ValueBlock createVarcharBlock(Random random, int offset, int length, float nullRate)
        {
            BlockBuilder builder = VARCHAR.createBlockBuilder(null, offset + length);
            int position = 0;
            // Fill the beginning range with nulls to prevent the isNull array from being suppressed
            for (; position < offset; position++) {
                builder.appendNull();
            }
            for (; position < offset + length; position++) {
                if (random.nextFloat() <= nullRate) {
                    builder.appendNull();
                }
                else {
                    VARCHAR.writeSlice(builder, createRandomAsciiString(random));
                }
            }
            return builder.buildValueBlock();
        }

        private static Slice createRandomAsciiString(Random random)
        {
            byte[] data = new byte[random.nextInt(MAX_STRING_LENGTH)];
            random.nextBytes(data);
            return Slices.wrappedBuffer(data);
        }
    }

    @Test
    public void test()
    {
        BlockData data = new BlockData();
        data.setup();
        benchmarkAppendRange(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkBlockBuilder.class).run();
    }
}
