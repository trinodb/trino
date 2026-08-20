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

import org.joda.time.chrono.ISOChronology;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.RunnerException;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.DAYS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDateTimeExtraction
{
    private static final ISOChronology UTC = ISOChronology.getInstanceUTC();
    private static final int BATCH = 1024;

    @State(Scope.Thread)
    public static class Data
    {
        @Param("1024")
        public int size = BATCH;

        public int[] days;

        @Setup
        public void setup()
        {
            // ~ year 1900 .. year 2050, the bulk of analytical workloads.
            SplittableRandom random = new SplittableRandom(42);
            days = new int[size];
            int min = -25567;   // 1900-01-01
            int max = 29220;    // 2050-01-01
            for (int i = 0; i < size; i++) {
                days[i] = min + random.nextInt(max - min);
            }
        }
    }

    @Benchmark
    public void yearJoda(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(UTC.year().get(DAYS.toMillis(days)));
        }
    }

    @Benchmark
    public void yearNeriSchneider(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(NeriSchneider.yearFromDays(days));
        }
    }

    @Benchmark
    public void monthJoda(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(UTC.monthOfYear().get(DAYS.toMillis(days)));
        }
    }

    @Benchmark
    public void monthNeriSchneider(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(NeriSchneider.monthFromDays(days));
        }
    }

    @Benchmark
    public void dayOfMonthJoda(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(UTC.dayOfMonth().get(DAYS.toMillis(days)));
        }
    }

    @Benchmark
    public void dayOfMonthNeriSchneider(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(NeriSchneider.dayOfMonthFromDays(days));
        }
    }

    @Benchmark
    public void monthlyBucketJoda(Data data, Blackhole bh)
    {
        // year(d) * 100 + month(d) -- the GROUP BY pattern from the proposal.
        for (int days : data.days) {
            long millis = DAYS.toMillis(days);
            bh.consume(UTC.year().get(millis) * 100 + UTC.monthOfYear().get(millis));
        }
    }

    @Benchmark
    public void monthlyBucketNeriSchneider(Data data, Blackhole bh)
    {
        for (int days : data.days) {
            bh.consume(NeriSchneider.yearFromDays(days) * 100 + NeriSchneider.monthFromDays(days));
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkDateTimeExtraction.class)
                .withOptions(builder ->
                        builder.resultFormat(ResultFormatType.JSON))
                .run();
    }
}
