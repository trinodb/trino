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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.longDecimalToReal;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkLongDecimalToFloatingPoint
{
    @Benchmark
    public double toDouble(Data data)
    {
        long scale = data.scale;
        double result = 0;
        for (Int128 decimal : data.decimals) {
            result += longDecimalToDouble(decimal, scale);
        }
        return result;
    }

    @Benchmark
    public long toReal(Data data)
    {
        long scale = data.scale;
        long result = 0;
        for (Int128 decimal : data.decimals) {
            result += longDecimalToReal(decimal, scale);
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        private static final int COUNT = 1024;

        @Param
        public Distribution distribution;

        public Int128[] decimals;
        public long scale;

        @Setup
        public void setup()
        {
            scale = distribution.scale;
            decimals = distribution.decimals(COUNT);
        }
    }

    public enum Distribution
    {
        RANDOM_SCALE_0(0, 38),
        RANDOM_SCALE_10(10, 38),
        RANDOM_SCALE_20(20, 38),
        RANDOM_SCALE_38(38, 38),
        /**
         * Values below 10^10 in a wide decimal type, above 2^53 unscaled.
         */
        RANDOM_PRECISION_20_SCALE_10(10, 20),
        /**
         * Unscaled values exactly representable in float, which a divide alone rounds correctly.
         */
        RANDOM_PRECISION_7_SCALE_2(2, 7);

        private final int scale;
        private final int precision;

        Distribution(int scale, int precision)
        {
            this.scale = scale;
            this.precision = precision;
        }

        public Int128[] decimals(int count)
        {
            Random random = new Random(0);
            BigInteger bound = BigInteger.TEN.pow(precision);
            Int128[] decimals = new Int128[count];
            for (int i = 0; i < count; i++) {
                BigInteger unscaled = new BigInteger(bound.bitLength(), random).mod(bound);
                if (random.nextBoolean()) {
                    unscaled = unscaled.negate();
                }
                decimals[i] = Int128.valueOf(unscaled);
            }
            return decimals;
        }
    }

    static void main()
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkLongDecimalToFloatingPoint.class).run();
    }
}
