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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkVariableWidthBlock
{
    private static final int SEED = 831;
    private static final int POSITIONS = 8096;
    private static final double NULLS_CHANCE = 0.2;

    @Benchmark
    public Block copyPositions(BenchmarkData data)
    {
        int[] positionIds = data.getPositionsIds();
        return data.getVariableWidthBlock().copyPositions(positionIds, 0, positionIds.length);
    }

    @State(Thread)
    public static class BenchmarkData
    {
        @Param({"200", "1000", "8000"})
        private int selectedPositionsCount;

        @Param({"false", "true"})
        private boolean nullsAllowed;

        @Param({
                "GROUPED",
                "SEQUENCE",
                "RANDOM",
        })
        private SelectedPositions selectedPositions;

        private int[] positionsIds;
        private Block variableWidthBlock;

        public BenchmarkData(int selectedPositionsCount, boolean nullsAllowed, SelectedPositions selectedPositions)
        {
            this.selectedPositionsCount = selectedPositionsCount;
            this.nullsAllowed = nullsAllowed;
            this.selectedPositions = selectedPositions;
        }

        public BenchmarkData()
        {
            this(1000, false, SelectedPositions.SEQUENCE);
        }

        @Setup
        public void setup()
        {
            positionsIds = selectedPositions.generateIds(selectedPositionsCount);
            Slice[] slices = generateValues();
            variableWidthBlock = createBlockBuilderWithValues(slices).build();
        }

        private Slice[] generateValues()
        {
            Random random = new Random(SEED);
            Slice[] generatedValues = new Slice[POSITIONS];
            for (int position = 0; position < POSITIONS; position++) {
                if (nullsAllowed && randomNullChance(random)) {
                    generatedValues[position] = null;
                }
                else {
                    int length = random.nextInt(380) + 20;
                    byte[] buffer = new byte[length];
                    random.nextBytes(buffer);
                    generatedValues[position] = Slices.wrappedBuffer(buffer);
                }
            }
            return generatedValues;
        }

        private static boolean randomNullChance(Random random)
        {
            double value = 0;
            // chance has to be 0 to 1 exclusive.
            while (value == 0) {
                value = random.nextDouble();
            }
            return value < NULLS_CHANCE;
        }

        private static BlockBuilder createBlockBuilderWithValues(Slice[] generatedValues)
        {
            BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, generatedValues.length, 32 * generatedValues.length);
            for (Slice value : generatedValues) {
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    blockBuilder.writeBytes(value, 0, value.length()).closeEntry();
                }
            }
            return blockBuilder;
        }

        public int[] getPositionsIds()
        {
            return positionsIds;
        }

        public Block getVariableWidthBlock()
        {
            return variableWidthBlock;
        }
    }

    public enum SelectedPositions
    {
        SEQUENCE {
            @Override
            int[] generateIds(int positionsCount)
            {
                return IntStream.range(0, positionsCount).toArray();
            }
        },
        RANDOM {
            @Override
            int[] generateIds(int positionsCount)
            {
                return new Random(SEED).ints(positionsCount, 0, POSITIONS).toArray();
            }
        },
        GROUPED {
            @Override
            int[] generateIds(int positionsCount)
            {
                Random random = new Random(SEED);
                int maxGroupSize = positionsCount / 10;
                int[] ids = new int[positionsCount];
                int index = 0;
                int currentPosition = 0;
                while (index < positionsCount) {
                    checkState(
                            currentPosition < POSITIONS,
                            "Reduce maxGroupSize or positionsCount to fit the generated ids within POSITIONS");
                    int groupSize = Math.min(
                            random.nextInt(maxGroupSize),
                            Math.min(positionsCount - index, POSITIONS - currentPosition));
                    for (int i = 0; i < groupSize; i++) {
                        ids[index] = currentPosition;
                        index++;
                        currentPosition++;
                    }
                    currentPosition++; // break group
                }
                return ids;
            }
        };

        abstract int[] generateIds(int positionsCount);
    }

    @Test
    public void testCopyPositions()
    {
        for (SelectedPositions selectedPositions : SelectedPositions.values()) {
            for (boolean nullsAllowed : new boolean[] {false, true}) {
                BenchmarkData data = new BenchmarkData(1024, nullsAllowed, selectedPositions);
                data.setup();
                copyPositions(data);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkVariableWidthBlock.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
