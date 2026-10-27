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

import io.trino.operator.scalar.DateTimeFunctions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkDateExtraction
{
    @State(Thread)
    public static class BenchmarkData
    {
        // realistic    ≈ 1970-01-01 to 2100-12-31 (≈ daily-business range)
        // wide         ≈ across the algorithm's safe range; stresses branch predictor
        @Param({"realistic", "wide"})
        public String distribution;

        int[] dates;

        @Setup
        public void setup()
        {
            dates = new int[1024];
            if ("realistic".equals(distribution)) {
                Random r = new Random(42);
                int lo = 0;
                int hi = (int) LocalDate.of(2100, 12, 31).toEpochDay();
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = lo + r.nextInt(hi - lo);
                }
            }
            else {
                // Full int range — exercises both the fast path and the rare fallback
                // (~7000 days at the very bottom of the range).
                Random r = new Random(42);
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = r.nextInt();
                }
            }
        }
    }

    // ---- year ----

    @Benchmark
    public long javaTimeYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).getYear();
        }
        return sum;
    }

    @Benchmark
    public long fastYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += FastDate.yearOf(d);
        }
        return sum;
    }

    // ---- month ----

    @Benchmark
    public long javaTimeMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).getMonthValue();
        }
        return sum;
    }

    @Benchmark
    public long fastMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += FastDate.monthOf(d);
        }
        return sum;
    }

    // ---- day-of-month ----

    @Benchmark
    public long javaTimeDay(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).getDayOfMonth();
        }
        return sum;
    }

    @Benchmark
    public long fastDay(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += FastDate.dayOf(d);
        }
        return sum;
    }

    // ---- day-of-year ----

    @Benchmark
    public long javaTimeDayOfYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).getDayOfYear();
        }
        return sum;
    }

    @Benchmark
    public long fastDayOfYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += FastDate.dayOfYearOf(d);
        }
        return sum;
    }

    // ---- quarter ----

    @Benchmark
    public long javaTimeQuarter(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).get(IsoFields.QUARTER_OF_YEAR);
        }
        return sum;
    }

    @Benchmark
    public long fastQuarter(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += FastDate.quarterOf(d);
        }
        return sum;
    }

    // ---- last_day_of_month ----

    @Benchmark
    public long javaTimeLastDayOfMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).with(TemporalAdjusters.lastDayOfMonth()).toEpochDay();
        }
        return sum;
    }

    @Benchmark
    public long fastLastDayOfMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += (long) d - FastDate.dayOf(d) + FastDate.daysInMonthOf(d);
        }
        return sum;
    }

    // ---- year+month+day combined (defeats DCE of unused fields) ----

    @Benchmark
    public long javaTimeYmd(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            LocalDate date = LocalDate.ofEpochDay(d);
            sum += date.getYear() + date.getMonthValue() + date.getDayOfMonth();
        }
        return sum;
    }

    @Benchmark
    public long fastYmd(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long ymd = FastDate.ymdFromEpochDay(d);
            sum += (int) (ymd >> 32) + (int) ((ymd >> 8) & 0xFF) + (int) (ymd & 0xFF);
        }
        return sum;
    }

    @Benchmark
    public long javaTimeDayOfWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).getDayOfWeek().getValue();
        }
        return sum;
    }

    @Benchmark
    public long fastDayOfWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += DateTimeFunctions.dayOfWeekFromDate(d);
        }
        return sum;
    }

    @Benchmark
    public long javaTimeWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        }
        return sum;
    }

    @Benchmark
    public long fastWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += DateTimeFunctions.weekFromDate(d);
        }
        return sum;
    }

    @Benchmark
    public long javaTimeYearOfWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += LocalDate.ofEpochDay(d).get(IsoFields.WEEK_BASED_YEAR);
        }
        return sum;
    }

    @Benchmark
    public long fastYearOfWeek(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += DateTimeFunctions.yearOfWeekFromDate(d);
        }
        return sum;
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkDateExtraction.class).run();
    }
}
