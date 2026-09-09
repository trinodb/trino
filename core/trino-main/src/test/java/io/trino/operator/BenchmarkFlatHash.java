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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
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
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.FlatHashStrategyCompiler.compileFlatHashStrategy;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Measures the control byte matching of {@link FlatHash}, comparing the scalar and the vectorized
 * {@link ControlMatcher} implementations. Each JMH fork only loads the matcher under test, so the
 * calls into it are monomorphic, exactly as they are in a server that only ever loads the matcher
 * selected for its hardware.
 * <p>
 * The vector matcher is only meaningful to measure on hardware with a 256 bit vector register, which
 * is the only hardware a server selects it on. Elsewhere, such as on a CPU with 128 bit NEON registers
 * only, the JVM emulates the comparison and the result says nothing about the matcher.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFlatHash
{
    private static final int POSITIONS = 1_000_000;
    private static final int EXPECTED_SIZE = 10_000;

    /**
     * Fills the hash table from empty, which is dominated by inserts and rehashes.
     */
    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object putIfAbsent(BenchmarkData data)
    {
        FlatHash flatHash = data.createFlatHash();
        Block[] blocks = data.getBlocks();
        for (int position = 0; position < POSITIONS; position++) {
            flatHash.putIfAbsent(blocks, position);
        }
        return flatHash;
    }

    /**
     * Probes a table filled to its maximum load factor, where the probe sequences are the longest.
     */
    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long getIfPresent(BenchmarkData data)
    {
        FlatHash flatHash = data.getFilledFlatHash();
        Block[] blocks = data.getBlocks();
        long result = 0;
        for (int position = 0; position < POSITIONS; position++) {
            result += flatHash.getIfPresent(blocks, position);
        }
        return result;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"SCALAR", "VECTOR"})
        private String matcher = "SCALAR";

        @Param({"BIGINT", "VARCHAR"})
        private String dataType = "BIGINT";

        @Param({"1", "5"})
        private int channelCount = 1;

        @Param({"8000", "3000000"})
        private int groupCount = 3_000_000;

        @Param({"true", "false"})
        private boolean cacheHashValue;

        private FlatHashStrategy flatHashStrategy;
        private Block[] blocks;
        private FlatHash filledFlatHash;

        @Setup
        public void setup()
        {
            if (matcher.equals("VECTOR") && !VectorControlMatcher.isSupported()) {
                System.out.println("WARNING: this hardware has no 256 bit vector register, so the vector matcher is emulated by the JVM " +
                        "and is far slower than it is on the hardware it would actually be selected for. The numbers below are meaningless.");
            }

            List<Type> types = switch (dataType) {
                case "BIGINT" -> Collections.<Type>nCopies(channelCount, BIGINT);
                case "VARCHAR" -> Collections.<Type>nCopies(channelCount, VARCHAR);
                default -> throw new UnsupportedOperationException("Unsupported dataType: " + dataType);
            };
            flatHashStrategy = compileFlatHashStrategy(types, new TypeOperators());
            blocks = createBlocks(types);

            filledFlatHash = createFlatHash();
            for (int position = 0; position < POSITIONS; position++) {
                filledFlatHash.putIfAbsent(blocks, position);
            }
        }

        public FlatHash createFlatHash()
        {
            ControlMatcher controlMatcher = switch (matcher) {
                case "SCALAR" -> new ScalarControlMatcher();
                case "VECTOR" -> new VectorControlMatcher();
                default -> throw new UnsupportedOperationException("Unsupported matcher: " + matcher);
            };
            return new FlatHash(flatHashStrategy, cacheHashValue, EXPECTED_SIZE, NOOP, controlMatcher);
        }

        public FlatHash getFilledFlatHash()
        {
            return filledFlatHash;
        }

        public Block[] getBlocks()
        {
            return blocks;
        }

        private Block[] createBlocks(List<Type> types)
        {
            PageBuilder pageBuilder = new PageBuilder(POSITIONS, types);
            for (int position = 0; position < POSITIONS; position++) {
                int value = ThreadLocalRandom.current().nextInt(groupCount);
                pageBuilder.declarePosition();
                for (int channel = 0; channel < channelCount; channel++) {
                    switch (dataType) {
                        case "BIGINT" -> BIGINT.writeLong(pageBuilder.getBlockBuilder(channel), value);
                        case "VARCHAR" -> VARCHAR.writeSlice(pageBuilder.getBlockBuilder(channel), varcharValue(value));
                        default -> throw new UnsupportedOperationException("Unsupported dataType: " + dataType);
                    }
                }
            }
            Page page = pageBuilder.build();
            Block[] blocks = new Block[channelCount];
            for (int channel = 0; channel < channelCount; channel++) {
                blocks[channel] = page.getBlock(channel);
            }
            return blocks;
        }

        private static Slice varcharValue(int value)
        {
            return Slices.wrappedHeapBuffer(ByteBuffer.allocate(4).putInt(value).flip());
        }
    }

    static void main()
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkFlatHash().putIfAbsent(data);
        new BenchmarkFlatHash().getIfPresent(data);

        benchmark(BenchmarkFlatHash.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xmx8g"))
                .run();
    }
}
