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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkNdvSketchUpdate
{
    private static final int VALUES_PER_INVOCATION = 10_000;

    @Benchmark
    public Object thetaBigint(BigintData data)
    {
        UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setFamily(Family.ALPHA).build();
        for (long value : data.values) {
            sketch.update(value);
        }
        return sketch.compact();
    }

    @Benchmark
    public Object hllBigint(BigintData data)
    {
        HyperLogLog sketch = HyperLogLog.newInstance(4096);
        for (long value : data.values) {
            sketch.addHash(value);
        }
        return sketch;
    }

    @Benchmark
    public Object thetaVarchar(VarcharData data)
    {
        UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setFamily(Family.ALPHA).build();
        for (byte[] value : data.values) {
            sketch.update(value);
        }
        return sketch.compact();
    }

    @Benchmark
    public Object hllVarchar(VarcharData data)
    {
        HyperLogLog sketch = HyperLogLog.newInstance(4096);
        for (byte[] value : data.values) {
            sketch.add(Slices.wrappedBuffer(value));
        }
        return sketch;
    }

    @State(Scope.Thread)
    public static class BigintData
    {
        private long[] values;

        @Setup(Level.Iteration)
        public void setup()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            values = new long[VALUES_PER_INVOCATION];
            for (int i = 0; i < VALUES_PER_INVOCATION; i++) {
                values[i] = random.nextLong();
            }
        }
    }

    @State(Scope.Thread)
    public static class VarcharData
    {
        private static final int STRING_LENGTH = 64;

        private byte[][] values;

        @Setup(Level.Iteration)
        public void setup()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            values = new byte[VALUES_PER_INVOCATION][];
            for (int i = 0; i < VALUES_PER_INVOCATION; i++) {
                values[i] = randomAsciiBytes(random, STRING_LENGTH);
            }
        }

        private static byte[] randomAsciiBytes(ThreadLocalRandom random, int length)
        {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte) ('a' + random.nextInt(26));
            }
            return bytes;
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkNdvSketchUpdate.class).run();
    }
}
