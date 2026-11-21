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
package io.trino.util;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MICROSECONDS)
@BenchmarkMode(Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkShortestDecimalFormatter
{
    private static final int VALUES = 1024;

    @Param({"random", "short", "integral"})
    public String distribution;

    private double[] doubles;
    private float[] floats;
    private DecimalFormat doubleFormat;
    private DecimalFormat realFormat;

    @Setup
    public void setup()
    {
        Random random = new Random(1);
        doubles = new double[VALUES];
        floats = new float[VALUES];
        for (int i = 0; i < VALUES; i++) {
            switch (distribution) {
                case "random" -> {
                    doubles[i] = Double.longBitsToDouble(random.nextLong() & 0x7FEF_FFFF_FFFF_FFFFL);
                    floats[i] = Float.intBitsToFloat(random.nextInt() & 0x7F7F_FFFF);
                }
                case "short" -> {
                    doubles[i] = random.nextInt(1, 1_000_000) / 100.0;
                    floats[i] = random.nextInt(1, 1_000_000) / 100.0f;
                }
                case "integral" -> {
                    doubles[i] = random.nextInt(1, 1_000_000_000);
                    floats[i] = random.nextInt(1, 1_000_000);
                }
                default -> throw new IllegalArgumentException(distribution);
            }
        }
        doubleFormat = new DecimalFormat("0.0###################E0", new DecimalFormatSymbols(ENGLISH));
        realFormat = new DecimalFormat("0.0#####E0", new DecimalFormatSymbols(ENGLISH));
    }

    @Benchmark
    public void formatDouble(Blackhole blackhole)
    {
        for (double value : doubles) {
            blackhole.consume(ShortestDecimalFormatter.formatDouble(value));
        }
    }

    @Benchmark
    public void formatDoubleDecimalFormat(Blackhole blackhole)
    {
        for (double value : doubles) {
            blackhole.consume(doubleFormat.format(value));
        }
    }

    @Benchmark
    public void formatReal(Blackhole blackhole)
    {
        for (float value : floats) {
            blackhole.consume(ShortestDecimalFormatter.formatReal(value));
        }
    }

    @Benchmark
    public void formatRealDecimalFormat(Blackhole blackhole)
    {
        for (float value : floats) {
            blackhole.consume(realFormat.format(Double.parseDouble(Float.toString(value))));
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkShortestDecimalFormatter.class).run();
    }
}
