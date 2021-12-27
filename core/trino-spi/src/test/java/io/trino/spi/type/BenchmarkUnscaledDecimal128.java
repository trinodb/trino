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

import io.airlift.slice.Slice;
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

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkUnscaledDecimal128
{
    @Benchmark
    public Slice multiplyLongLong(Data data)
    {
        UnscaledDecimal128Arithmetic.multiply(data.longLeft, data.longRight, data.result);

        return data.result;
    }

    @Benchmark
    public Slice multiplyLongInt(Data data)
    {
        UnscaledDecimal128Arithmetic.multiply(data.longLeft, data.intRight, data.result);

        return data.result;
    }

    @Benchmark
    public Slice multiply128Int(Data data)
    {
        UnscaledDecimal128Arithmetic.multiply(data.int128, data.intRight, data.result);

        return data.result;
    }

    @Benchmark
    public Slice multiply128(Data data)
    {
        UnscaledDecimal128Arithmetic.multiply(data.leftLow, data.leftHigh, data.rightLow, data.rightHigh, data.result);

        return data.result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        public final Slice result = io.airlift.slice.Slices.allocate(16);

        public long longLeft;
        public long longRight;
        public int intRight;
        public long leftHigh;
        public long leftLow;
        public long rightHigh;
        public long rightLow;
        public Slice int128;

        @Setup(Level.Iteration)
        public void setup()
        {
            longLeft = ThreadLocalRandom.current().nextLong();
            longRight = ThreadLocalRandom.current().nextLong();
            intRight = ThreadLocalRandom.current().nextInt();
            leftHigh = ThreadLocalRandom.current().nextBoolean() ? UnscaledDecimal128Arithmetic.SIGN_LONG_MASK : 0;
            leftLow = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            rightHigh = ThreadLocalRandom.current().nextBoolean() ? UnscaledDecimal128Arithmetic.SIGN_LONG_MASK : 0;
            rightLow = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            int128 = unscaledDecimal(BigInteger.valueOf(ThreadLocalRandom.current().nextLong()));
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkUnscaledDecimal128.class).run();
    }
}
