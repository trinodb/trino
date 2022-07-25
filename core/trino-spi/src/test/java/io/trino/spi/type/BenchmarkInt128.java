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
package io.trino.spi.type;

import io.trino.jmh.Benchmarks;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkInt128
{
    @Benchmark
    public Int128 multiplyLongLong(Data data)
    {
        return Int128Math.multiply(data.longLeft, data.longRight);
    }

    @Benchmark
    public Int128 multiplyLongInt(Data data)
    {
        return Int128Math.multiply(data.longLeft, data.intRight);
    }

    @Benchmark
    public Int128 multiply128Int(Data data)
    {
        return Int128Math.multiply(data.int128, data.intRight);
    }

    @Benchmark
    public Int128 multiply128(Data data)
    {
        return Int128Math.multiply(data.leftHigh, data.leftLow, data.rightHigh, data.rightLow);
    }

    @State(Scope.Thread)
    public static class Data
    {
        public long longLeft;
        public long longRight;
        public int intRight;
        public long leftHigh;
        public long leftLow;
        public long rightHigh;
        public long rightLow;
        public Int128 int128;

        @Setup(Level.Iteration)
        public void setup()
        {
            longLeft = ThreadLocalRandom.current().nextLong();
            longRight = ThreadLocalRandom.current().nextLong();
            intRight = ThreadLocalRandom.current().nextInt();
            leftLow = ThreadLocalRandom.current().nextLong();
            leftHigh = leftHigh >> 63;
            rightLow = ThreadLocalRandom.current().nextLong();
            rightHigh = rightHigh >> 63;
            int128 = Int128.valueOf(ThreadLocalRandom.current().nextLong());
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkInt128.class).run();
    }
}
