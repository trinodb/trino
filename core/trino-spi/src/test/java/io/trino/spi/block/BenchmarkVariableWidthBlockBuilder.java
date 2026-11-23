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
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/// Appends half of the positions of a [VariableWidthBlock] that holds values of the given lengths to a builder,
/// as the partitioned output does for every partition.
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkVariableWidthBlockBuilder
{
    private static final int POSITIONS = 8192;
    private static final int SELECTED_POSITIONS = POSITIONS / 2;

    @Benchmark
    public VariableWidthBlock appendPositions(BenchmarkData data)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, SELECTED_POSITIONS, data.totalBytes / 2);
        builder.appendPositions(data.block, data.selectedPositions, 0, SELECTED_POSITIONS);
        return builder.buildValueBlock();
    }

    public enum ValueLengths
    {
        SHORT_0_TO_12(0, 12),
        MEDIUM_13_TO_32(13, 32),
        MIXED_0_TO_64(0, 64),
        LONG_100_TO_200(100, 200);

        private final int minLength;
        private final int maxLength;

        ValueLengths(int minLength, int maxLength)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        /// Returns values where a null position is a null element.
        Slice[] generate(Random random, int count, int nullPercent)
        {
            Slice[] values = new Slice[count];
            for (int i = 0; i < count; i++) {
                if (random.nextInt(100) < nullPercent) {
                    continue;
                }
                byte[] bytes = new byte[minLength + random.nextInt(maxLength - minLength + 1)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte) ('a' + random.nextInt(26));
                }
                values[i] = Slices.wrappedBuffer(bytes);
            }
            return values;
        }
    }

    @State(Thread)
    public static class BenchmarkData
    {
        @Param
        private ValueLengths valueLengths = ValueLengths.MIXED_0_TO_64;

        @Param({"0", "10"})
        private int nullPercent;

        private Slice[] values;
        private int totalBytes;
        private VariableWidthBlock block;
        private int[] selectedPositions;

        public BenchmarkData() {}

        public BenchmarkData(ValueLengths valueLengths, int nullPercent)
        {
            this.valueLengths = valueLengths;
            this.nullPercent = nullPercent;
        }

        @Setup
        public void setup()
        {
            Random random = new Random(831);
            values = valueLengths.generate(random, POSITIONS, nullPercent);
            for (Slice value : values) {
                if (value != null) {
                    totalBytes += value.length();
                }
            }

            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, POSITIONS, totalBytes);
            for (Slice value : values) {
                if (value == null) {
                    builder.appendNull();
                }
                else {
                    builder.writeEntry(value);
                }
            }
            block = builder.buildValueBlock();

            // positions in increasing order, like the positions a filter or a partitioner selects
            selectedPositions = new int[SELECTED_POSITIONS];
            int selected = 0;
            for (int position = 0; selected < SELECTED_POSITIONS; position++) {
                int remainingPositions = POSITIONS - position;
                if (random.nextInt(remainingPositions) < SELECTED_POSITIONS - selected) {
                    selectedPositions[selected] = position;
                    selected++;
                }
            }
        }
    }

    @Test
    public void testAppendPositions()
    {
        for (ValueLengths valueLengths : ValueLengths.values()) {
            for (int nullPercent : new int[] {0, 10}) {
                BenchmarkData data = new BenchmarkData(valueLengths, nullPercent);
                data.setup();
                VariableWidthBlock appended = appendPositions(data);
                assertThat(appended.getPositionCount()).isEqualTo(SELECTED_POSITIONS);
                for (int i = 0; i < SELECTED_POSITIONS; i++) {
                    Slice expected = data.values[data.selectedPositions[i]];
                    assertThat(appended.isNull(i)).isEqualTo(expected == null);
                    if (expected != null) {
                        assertThat(appended.getSlice(i)).isEqualTo(expected);
                    }
                }
            }
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkVariableWidthBlockBuilder.class).run();
    }
}
