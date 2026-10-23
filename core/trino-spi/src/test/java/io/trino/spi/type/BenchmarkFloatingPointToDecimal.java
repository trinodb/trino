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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.DecimalConversions.doubleToLongDecimal;
import static io.trino.spi.type.DecimalConversions.doubleToShortDecimal;
import static io.trino.spi.type.DecimalConversions.realToLongDecimal;
import static io.trino.spi.type.DecimalConversions.realToShortDecimal;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
public class BenchmarkFloatingPointToDecimal
{
    @Benchmark
    public long fromDoubleToShort(Data data)
    {
        int scale = data.distribution.scale;
        long result = 0;
        for (double value : data.doubles) {
            result += doubleToShortDecimal(value, MAX_SHORT_PRECISION, scale);
        }
        return result;
    }

    @Benchmark
    public long fromRealToShort(Data data)
    {
        int scale = data.distribution.scale;
        long result = 0;
        for (float value : data.floats) {
            result += realToShortDecimal(value, MAX_SHORT_PRECISION, scale);
        }
        return result;
    }

    @Benchmark
    public long fromDoubleToLong(Data data)
    {
        int scale = data.distribution.scale;
        long result = 0;
        for (double value : data.doubles) {
            result += doubleToLongDecimal(value, MAX_PRECISION, scale).getLow();
        }
        return result;
    }

    @Benchmark
    public long fromRealToLong(Data data)
    {
        int scale = data.distribution.scale;
        long result = 0;
        for (float value : data.floats) {
            result += realToLongDecimal(value, MAX_PRECISION, scale).getLow();
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        private static final int COUNT = 1024;

        @Param
        public Distribution distribution;

        public double[] doubles;
        public float[] floats;

        @Setup
        public void setup()
        {
            doubles = distribution.values(COUNT);
            floats = new float[COUNT];
            for (int i = 0; i < COUNT; i++) {
                floats[i] = (float) doubles[i];
            }
        }
    }

    public enum Distribution
    {
        /**
         * Decimals with two fractional digits, as parsed from text, rounded to the same scale.
         */
        DECIMAL_SCALE_2(2),
        /**
         * Arbitrary binary fractions.
         */
        RANDOM_SCALE_2(2),
        RANDOM_SCALE_8(8),
        /**
         * Decimals whose last digit, a 5, is the first one rounded away, so the decimal string and the binary value
         * can round apart.
         */
        TIE_SCALE_2(2);

        private final int scale;

        Distribution(int scale)
        {
            this.scale = scale;
        }

        public double[] values(int count)
        {
            Random random = new Random(0);
            double[] values = new double[count];
            for (int i = 0; i < count; i++) {
                double value = switch (this) {
                    case DECIMAL_SCALE_2 -> BigDecimal.valueOf(random.nextLong(1_000_000L), 2).doubleValue();
                    case RANDOM_SCALE_2, RANDOM_SCALE_8 -> random.nextDouble() * 1e4;
                    case TIE_SCALE_2 -> BigDecimal.valueOf(random.nextLong(1_000_000L) * 10 + 5, 3).doubleValue();
                };
                values[i] = random.nextBoolean() ? -value : value;
            }
            return values;
        }
    }

    static void main()
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkFloatingPointToDecimal.class).run();
    }
}
