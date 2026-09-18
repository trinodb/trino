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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.DecimalConversions.shortDecimalToReal;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkShortDecimalToReal
{
    @Benchmark
    public long convert(Data data)
    {
        long tenToScale = data.tenToScale;
        long result = 0;
        for (long decimal : data.decimals) {
            result += shortDecimalToReal(decimal, tenToScale);
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        // Enough values to amortize the harness overhead of a sub-nanosecond conversion, while the
        // array still fits in L1.
        private static final int COUNT = 1024;

        @Param
        public Distribution distribution;

        public long[] decimals;
        public long tenToScale;

        @Setup
        public void setup()
        {
            tenToScale = distribution.tenToScale();
            decimals = distribution.decimals(COUNT);
        }
    }

    public enum Distribution
    {
        RANDOM_SCALE_0(0),
        RANDOM_SCALE_2(2),
        RANDOM_SCALE_8(8),
        RANDOM_SCALE_18(18),
        /**
         * Quotients that sit exactly on a float midpoint and are exactly representable, so the tie breaks
         * to even. Exact multiples of a power of one half qualify wherever the float spacing lines up:
         * scale 1 values ending in .5 between 2^23 and 2^24, all .25 and .75 values an octave below
         * that, and so on.
         */
        MIDPOINT_EXACT_QUOTIENT(1),
        /**
         * Quotients on a midpoint that are not exactly representable, resolved from the sign of the
         * fused multiply-add residual.
         */
        MIDPOINT_INEXACT_QUOTIENT(14),
        /**
         * The same, but with an unscaled value above 2^53, where the dividend has itself rounded and only
         * {@link BigDecimal} can settle the direction. About 1 in 2^25 uniformly distributed values.
         */
        MIDPOINT_ABOVE_MAX_EXACT(18);

        private final int scale;

        Distribution(int scale)
        {
            this.scale = scale;
        }

        public long tenToScale()
        {
            long tenToScale = 1;
            for (int i = 0; i < scale; i++) {
                tenToScale *= 10;
            }
            return tenToScale;
        }

        public long[] decimals(int count)
        {
            return switch (this) {
                case RANDOM_SCALE_0, RANDOM_SCALE_2, RANDOM_SCALE_8, RANDOM_SCALE_18 -> randomDecimals(count);
                case MIDPOINT_EXACT_QUOTIENT -> exactMidpointDecimals(count);
                // An unscaled value only lands on a midpoint once tenToScale exceeds the quotient's ulp, and the
                // midpoint is only inexact while it still has a fractional part. Both hold near 70 at scale 14.
                case MIDPOINT_INEXACT_QUOTIENT -> inexactMidpointDecimals(count, tenToScale(), 70f);
                case MIDPOINT_ABOVE_MAX_EXACT -> inexactMidpointDecimals(count, tenToScale(), 0.1f);
            };
        }

        private static long[] randomDecimals(int count)
        {
            Random random = new Random(0);
            long[] decimals = new long[count];
            for (int i = 0; i < count; i++) {
                long decimal = (long) (random.nextDouble() * 1e18);
                if (random.nextBoolean()) {
                    decimal = -decimal;
                }
                decimals[i] = decimal;
            }
            return decimals;
        }

        private static long[] exactMidpointDecimals(int count)
        {
            long[] decimals = new long[count];
            for (int i = 0; i < count; i++) {
                decimals[i] = ((1L << 23) + i) * 10 + 5;
            }
            return decimals;
        }

        private static long[] inexactMidpointDecimals(int count, long tenToScale, float from)
        {
            long[] decimals = new long[count];
            float value = from;
            for (int i = 0; i < count; i++) {
                value = Math.nextUp(value);
                double midpoint = ((double) value + (double) Math.nextUp(value)) / 2;
                decimals[i] = new BigDecimal(midpoint)
                        .multiply(BigDecimal.valueOf(tenToScale))
                        .setScale(0, RoundingMode.HALF_UP)
                        .longValueExact();
            }
            return decimals;
        }
    }

    static void main()
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkShortDecimalToReal.class).run();
    }
}
