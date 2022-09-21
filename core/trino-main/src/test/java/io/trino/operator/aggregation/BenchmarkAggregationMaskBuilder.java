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
package io.trino.operator.aggregation;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.aggregation.AggregationMaskCompiler.generateAggregationMaskBuilder;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-XX:+UnlockDiagnosticVMOptions")
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkAggregationMaskBuilder
{
    private final AggregationMaskBuilder rleNoNullsBuilder = new InterpretedAggregationMaskBuilder(0, 3, 6);
    private final AggregationMaskBuilder rleNoNullsBuilderCurrent = new CurrentAggregationMaskBuilder(0, 3, 6);
    private final AggregationMaskBuilder rleNoNullsBuilderHandCoded = new HandCodedAggregationMaskBuilder(0, 3, 6);
    private final AggregationMaskBuilder rleNoNullsBuilderCompiled = compiledMaskBuilder(0, 3, 6);

    private final AggregationMaskBuilder noNullsBuilder = new InterpretedAggregationMaskBuilder(1, 4, 7);
    private final AggregationMaskBuilder noNullsBuilderCurrent = new CurrentAggregationMaskBuilder(1, 4, 7);
    private final AggregationMaskBuilder noNullsBuilderHandCoded = new HandCodedAggregationMaskBuilder(1, 4, 7);
    private final AggregationMaskBuilder noNullsBuilderCompiled = compiledMaskBuilder(1, 4, 7);

    private final AggregationMaskBuilder someNullsBuilder = new InterpretedAggregationMaskBuilder(2, 5, 8);
    private final AggregationMaskBuilder someNullsBuilderCurrent = new CurrentAggregationMaskBuilder(2, 5, 8);
    private final AggregationMaskBuilder someNullsBuilderHandCoded = new HandCodedAggregationMaskBuilder(2, 5, 8);
    private final AggregationMaskBuilder someNullsBuilderCompiled = compiledMaskBuilder(2, 5, 8);

    private final AggregationMaskBuilder oneBlockSomeNullsBuilder = new InterpretedAggregationMaskBuilder(2);
    private final AggregationMaskBuilder oneBlockSomeNullsBuilderCurrent = new CurrentAggregationMaskBuilder(2, -1, -1);
    private final AggregationMaskBuilder oneBlockSomeNullsBuilderHandCoded = new HandCodedAggregationMaskBuilder(2, -1, -1);
    private final AggregationMaskBuilder oneBlockSomeNullsBuilderCompiled = compiledMaskBuilder(2);

    private final AggregationMaskBuilder allBlocksBuilder = new InterpretedAggregationMaskBuilder(0, 1, 2, 3, 4, 5, 6, 7, 8);
    private final AggregationMaskBuilder allBlocksBuilderCompiled = compiledMaskBuilder(0, 1, 2, 3, 4, 5, 6, 7, 8);

    private Page arguments;

    @Setup
    public void setup()
            throws Throwable
    {
        int positions = 10_000;

        Block shortRleNoNulls = RunLengthEncodedBlock.create(new ShortArrayBlock(1, Optional.empty(), new short[] {42}), positions);
        Block shortNoNulls = new ShortArrayBlock(new long[positions].length, Optional.empty(), new short[positions]);
        Block shortSomeNulls = new ShortArrayBlock(new long[positions].length, someNulls(positions, 0.3), new short[positions]);

        Block intRleNoNulls = RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {42}), positions);
        Block intNoNulls = new IntArrayBlock(new long[positions].length, Optional.empty(), new int[positions]);
        Block intSomeNulls = new IntArrayBlock(new long[positions].length, someNulls(positions, 0.3), new int[positions]);

        Block longRleNoNulls = RunLengthEncodedBlock.create(new LongArrayBlock(1, Optional.empty(), new long[] {42}), positions);
        Block longNoNulls = new LongArrayBlock(new long[positions].length, Optional.empty(), new long[positions]);
        Block longSomeNulls = new LongArrayBlock(new long[positions].length, someNulls(positions, 0.3), new long[positions]);

        Block rleAllNulls = RunLengthEncodedBlock.create(new ShortArrayBlock(1, Optional.of(new boolean[] {true}), new short[] {42}), positions);

        arguments = new Page(
                shortRleNoNulls,
                shortNoNulls,
                shortSomeNulls,
                intRleNoNulls,
                intNoNulls,
                intSomeNulls,
                longRleNoNulls,
                longNoNulls,
                longSomeNulls,
                rleAllNulls);
    }

    private static Optional<boolean[]> someNulls(int positions, double nullRatio)
    {
        boolean[] nulls = new boolean[positions];
        for (int i = 0; i < nulls.length; i++) {
            // 0.7 ^ 3 = 0.343
            nulls[i] = ThreadLocalRandom.current().nextDouble() < nullRatio;
        }
        return Optional.of(nulls);
    }

    @Benchmark
    public Object rleNoNullsBlocksInterpreted()
    {
        return rleNoNullsBuilder.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object rleNoNullsBlocksCurrent()
    {
        return rleNoNullsBuilderCurrent.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object rleNoNullsBlocksHandCoded()
    {
        return rleNoNullsBuilderHandCoded.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object rleNoNullsBlocksCompiled()
    {
        return rleNoNullsBuilderCompiled.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object noNullsBlocksInterpreted()
    {
        return noNullsBuilder.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object noNullsBlocksCurrent()
    {
        return noNullsBuilderCurrent.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object noNullsBlocksHandCoded()
    {
        return noNullsBuilderHandCoded.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object noNullsBlocksCompiled()
    {
        return noNullsBuilderCompiled.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object someNullsBlocksInterpreted()
    {
        return someNullsBuilder.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object someNullsBlocksCurrent()
    {
        return someNullsBuilderCurrent.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object someNullsBlocksHandCoded()
    {
        return someNullsBuilderHandCoded.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object someNullsBlocksCompiled()
    {
        return someNullsBuilderCompiled.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object oneBlockSomeNullsInterpreted()
    {
        return oneBlockSomeNullsBuilder.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object oneBlockSomeNullsCurrent()
    {
        return oneBlockSomeNullsBuilderCurrent.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object oneBlockSomeNullsHandCoded()
    {
        return oneBlockSomeNullsBuilderHandCoded.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object oneBlockSomeNullsCompiled()
    {
        return oneBlockSomeNullsBuilderCompiled.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object allBlocksInterpreted()
    {
        return allBlocksBuilder.buildAggregationMask(arguments, Optional.empty());
    }

    @Benchmark
    public Object allBlocksCompiled()
    {
        return allBlocksBuilderCompiled.buildAggregationMask(arguments, Optional.empty());
    }

    public static void main(String[] args)
            throws Throwable
    {
        BenchmarkAggregationMaskBuilder bench = new BenchmarkAggregationMaskBuilder();
        bench.setup();
        bench.rleNoNullsBlocksInterpreted();
        bench.noNullsBlocksInterpreted();
        bench.someNullsBlocksInterpreted();
        bench.allBlocksInterpreted();
        bench.someNullsBlocksCurrent();
        bench.someNullsBlocksHandCoded();
        bench.someNullsBlocksCompiled();

        benchmark(BenchmarkAggregationMaskBuilder.class).run();
    }

    private static class CurrentAggregationMaskBuilder
            implements AggregationMaskBuilder
    {
        private final int first;
        private final int second;
        private final int third;

        private final AggregationMask mask = AggregationMask.createSelectAll(0);

        public CurrentAggregationMaskBuilder(int first, int second, int third)
        {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        @Override
        public AggregationMask buildAggregationMask(Page arguments, Optional<Block> optionalMaskBlock)
        {
            int positionCount = arguments.getPositionCount();
            mask.reset(positionCount);
            mask.applyMaskBlock(optionalMaskBlock.orElse(null));
            if (first >= 0) {
                mask.unselectNullPositions(arguments.getBlock(first));
            }
            if (second >= 0) {
                mask.unselectNullPositions(arguments.getBlock(second));
            }
            if (third >= 0) {
                mask.unselectNullPositions(arguments.getBlock(third));
            }
            return mask;
        }
    }

    private static class HandCodedAggregationMaskBuilder
            implements AggregationMaskBuilder
    {
        private final int first;
        private final int second;
        private final int third;

        public HandCodedAggregationMaskBuilder(int first, int second, int third)
        {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        private int[] selectedPositions = new int[0];

        @Override
        public AggregationMask buildAggregationMask(Page arguments, Optional<Block> optionalMaskBlock)
        {
            int positionCount = arguments.getPositionCount();

            // if page is empty, we are done
            if (positionCount == 0) {
                return AggregationMask.createSelectNone(positionCount);
            }

            Block maskBlock = optionalMaskBlock.orElse(null);
            boolean hasMaskBlock = maskBlock != null;
            boolean maskBlockMayHaveNull = hasMaskBlock && maskBlock.mayHaveNull();
            if (maskBlock instanceof RunLengthEncodedBlock rle) {
                Block value = rle.getValue();
                if (!(value == null ||
                        ((!maskBlockMayHaveNull || !value.isNull(0)) &&
                                value.getByte(0, 0) != 0))) {
                    return AggregationMask.createSelectNone(positionCount);
                }
                // mask block is always true, so do not evaluate mask block
                hasMaskBlock = false;
                maskBlockMayHaveNull = false;
            }

            Block nonNullArg0 = first < 0 ? null : arguments.getBlock(first);
            if (isAlwaysNull(nonNullArg0)) {
                return AggregationMask.createSelectNone(positionCount);
            }
            boolean nonNullArg0MayHaveNull = nonNullArg0 != null && nonNullArg0.mayHaveNull();

            Block nonNullArg1 = third < 0 ? null : arguments.getBlock(second);
            if (isAlwaysNull(nonNullArg1)) {
                return AggregationMask.createSelectNone(positionCount);
            }
            boolean nonNullArg1MayHaveNull = nonNullArg1 != null && nonNullArg1.mayHaveNull();

            Block nonNullArgN = third < 0 ? null : arguments.getBlock(third);
            if (isAlwaysNull(nonNullArgN)) {
                return AggregationMask.createSelectNone(positionCount);
            }
            boolean nonNullArgNMayHaveNull = nonNullArgN != null && nonNullArgN.mayHaveNull();

            // if there is no mask block, and all non-null arguments do not have nulls, we are done
            if (!hasMaskBlock && !nonNullArg0MayHaveNull && !nonNullArg1MayHaveNull && !nonNullArgNMayHaveNull) {
                return AggregationMask.createSelectAll(positionCount);
            }

            // grow the selection array if necessary
            int[] selectedPositions = this.selectedPositions;
            if (selectedPositions.length < positionCount) {
                selectedPositions = new int[positionCount];
                this.selectedPositions = selectedPositions;
            }

            // add all positions that pass the tests
            int selectedPositionsIndex = 0;
            for (int position = 0; position < positionCount; position++) {
                if ((maskBlock == null || ((!maskBlockMayHaveNull || !maskBlock.isNull(position)) && maskBlock.getByte(position, 0) != 0)) &&
                        (!nonNullArg0MayHaveNull || !nonNullArg0.isNull(position)) &&
                        (!nonNullArg1MayHaveNull || !nonNullArg1.isNull(position)) &&
                        (!nonNullArgNMayHaveNull || !nonNullArgN.isNull(position))) {
                    selectedPositions[selectedPositionsIndex] = position;
                    selectedPositionsIndex++;
                }
            }
            return AggregationMask.createSelectedPositions(positionCount, selectedPositions, selectedPositionsIndex);
        }
    }

    private static boolean isAlwaysNull(Block block)
    {
        if (block instanceof RunLengthEncodedBlock rle) {
            return rle.getValue().isNull(0);
        }
        return false;
    }

    private static boolean testMaskBlock(Block block, boolean mayHaveNulls, int position)
    {
        return block == null ||
                ((!mayHaveNulls || !block.isNull(position)) &&
                        block.getByte(position, 0) != 0);
    }

    private static boolean isNotNull(Block block, boolean mayHaveNulls, int position)
    {
        return !mayHaveNulls || !block.isNull(position);
    }

    private static AggregationMaskBuilder compiledMaskBuilder(int... ints)
    {
        try {
            return generateAggregationMaskBuilder(ints).newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
