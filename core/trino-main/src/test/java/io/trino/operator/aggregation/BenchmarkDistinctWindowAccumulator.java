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

import io.trino.spi.block.BitArrayBlock;
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
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.aggregation.DistinctWindowAccumulator.selectDistinctPositions;
import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(3)
public class BenchmarkDistinctWindowAccumulator
{
    private static final int POSITION_COUNT = 8192;

    @Param({"0.1", "0.5", "0.9"})
    public double selectivity;

    private BitArrayBlock block;
    private int[] packedSelectedPositions;
    private int[] scalarSelectedPositions;

    @Setup
    public void setup()
    {
        setup(false);
    }

    private void setup(boolean nullable)
    {
        long[] values = allocateWords(POSITION_COUNT, false);
        long[] validity = nullable ? allocateWords(POSITION_COUNT, false) : null;
        Random random = new Random(892734);
        int selectedCount = 0;
        for (int position = 0; position < POSITION_COUNT; position++) {
            boolean valid = !nullable || random.nextDouble() < 0.8;
            if (validity != null && valid) {
                set(validity, 0, position);
            }
            if (random.nextDouble() < selectivity) {
                set(values, 0, position);
                selectedCount += valid ? 1 : 0;
            }
        }
        block = new BitArrayBlock(POSITION_COUNT, Optional.ofNullable(validity), values);
        packedSelectedPositions = new int[selectedCount];
        scalarSelectedPositions = new int[selectedCount];
    }

    @Benchmark
    public int packed()
    {
        return selectDistinctPositions(block, packedSelectedPositions);
    }

    @Benchmark
    public int scalar()
    {
        int selectedIndex = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position) && block.getBoolean(position)) {
                scalarSelectedPositions[selectedIndex++] = position;
            }
        }
        return selectedIndex;
    }

    @Test
    public void verify()
    {
        for (double selectivity : new double[] {0.1, 0.5, 0.9}) {
            this.selectivity = selectivity;
            for (boolean nullable : new boolean[] {false, true}) {
                setup(nullable);
                assertThat(packed()).isEqualTo(scalar());
                assertThat(packedSelectedPositions).containsExactly(scalarSelectedPositions);
            }
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkDistinctWindowAccumulator.class).run();
    }
}
