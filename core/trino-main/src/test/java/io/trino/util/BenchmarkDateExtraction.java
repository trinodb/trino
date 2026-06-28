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

import io.trino.operator.scalar.QuarterOfYearDateTimeField;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
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
import java.time.temporal.TemporalAdjusters;
import java.util.Random;
import java.util.SplittableRandom;
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
    private static final ISOChronology UTC = ISOChronology.getInstanceUTC();
    private static final DateTimeField YEAR = UTC.year();
    private static final DateTimeField MONTH = UTC.monthOfYear();
    private static final DateTimeField DAY = UTC.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC.dayOfYear();
    private static final DateTimeField QUARTER = QuarterOfYearDateTimeField.QUARTER_OF_YEAR.getField(UTC);
    private static final long MILLIS_PER_DAY = 86_400_000L;

    @State(Thread)
    public static class BenchmarkData
    {
        // realistic    ≈ 1970-01-01 to 2100-12-31 (≈ daily-business range)
        // ns_reference ≈ 1900-01-01 to 2050-01-01, matches the distribution used by
        //                #29291's BenchmarkDateTimeExtraction for direct apples-to-apples
        //                comparison ("bulk of analytical workloads")
        // wide         ≈ across the algorithm's safe range; stresses branch predictor
        @Param({"realistic", "ns_reference", "wide"})
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
            else if ("ns_reference".equals(distribution)) {
                // Match #29291's BenchmarkDateTimeExtraction setup exactly.
                SplittableRandom random = new SplittableRandom(42);
                int min = -25567; // 1900-01-01
                int max = 29220;  // 2050-01-01
                for (int i = 0; i < dates.length; i++) {
                    dates[i] = min + random.nextInt(max - min);
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
    public long jodaYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += YEAR.get(d * MILLIS_PER_DAY);
        }
        return sum;
    }

    @Benchmark
    public long jdkYear(BenchmarkData data)
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
    public long jodaMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += MONTH.get(d * MILLIS_PER_DAY);
        }
        return sum;
    }

    @Benchmark
    public long jdkMonth(BenchmarkData data)
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
    public long jodaDay(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += DAY.get(d * MILLIS_PER_DAY);
        }
        return sum;
    }

    @Benchmark
    public long jdkDay(BenchmarkData data)
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
    public long jodaDayOfYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += DAY_OF_YEAR.get(d * MILLIS_PER_DAY);
        }
        return sum;
    }

    @Benchmark
    public long jdkDayOfYear(BenchmarkData data)
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
    public long jodaQuarter(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += QUARTER.get(d * MILLIS_PER_DAY);
        }
        return sum;
    }

    @Benchmark
    public long jdkQuarter(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += (LocalDate.ofEpochDay(d).getMonthValue() - 1) / 3 + 1;
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
    public long jodaLastDayOfMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long millis = UTC.monthOfYear().roundCeiling(d * MILLIS_PER_DAY + 1) - MILLIS_PER_DAY;
            sum += millis / MILLIS_PER_DAY;
        }
        return sum;
    }

    @Benchmark
    public long jdkLastDayOfMonth(BenchmarkData data)
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
    public long jodaYmd(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long ms = d * MILLIS_PER_DAY;
            sum += YEAR.get(ms) + MONTH.get(ms) + DAY.get(ms);
        }
        return sum;
    }

    @Benchmark
    public long jdkYmd(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            LocalDate ld = LocalDate.ofEpochDay(d);
            sum += ld.getYear() + ld.getMonthValue() + ld.getDayOfMonth();
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

    // ---- Neri-Schneider reference (impl in NeriSchneiderDate; cited in the PR algorithm-choice discussion) ----

    // Day-of-year prefix tables (algorithm-independent — same lookup as FastDate uses internally).
    private static final int[] DOY_PREFIX_NON_LEAP = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    private static final int[] DOY_PREFIX_LEAP = {0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335};

    @Benchmark
    public long nsYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += (int) (NeriSchneiderDate.ymdFromEpochDay(d) >> 32);
        }
        return sum;
    }

    @Benchmark
    public long nsMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += (int) ((NeriSchneiderDate.ymdFromEpochDay(d) >> 8) & 0xFF);
        }
        return sum;
    }

    @Benchmark
    public long nsDay(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            sum += (int) (NeriSchneiderDate.ymdFromEpochDay(d) & 0xFF);
        }
        return sum;
    }

    @Benchmark
    public long nsDayOfYear(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long ymd = NeriSchneiderDate.ymdFromEpochDay(d);
            int year = (int) (ymd >> 32);
            int month = (int) ((ymd >> 8) & 0xFF);
            int day = (int) (ymd & 0xFF);
            sum += (FastDate.isLeap(year) ? DOY_PREFIX_LEAP : DOY_PREFIX_NON_LEAP)[month] + day;
        }
        return sum;
    }

    @Benchmark
    public long nsQuarter(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            int month = (int) ((NeriSchneiderDate.ymdFromEpochDay(d) >> 8) & 0xFF);
            sum += (month - 1) / 3 + 1;
        }
        return sum;
    }

    @Benchmark
    public long nsLastDayOfMonth(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long ymd = NeriSchneiderDate.ymdFromEpochDay(d);
            int year = (int) (ymd >> 32);
            int month = (int) ((ymd >> 8) & 0xFF);
            int dayOfMonth = (int) (ymd & 0xFF);
            sum += (long) d - dayOfMonth + FastDate.daysInMonth(year, month);
        }
        return sum;
    }

    @Benchmark
    public long nsYmd(BenchmarkData data)
    {
        long sum = 0;
        for (int d : data.dates) {
            long ymd = NeriSchneiderDate.ymdFromEpochDay(d);
            sum += (int) (ymd >> 32) + (int) ((ymd >> 8) & 0xFF) + (int) (ymd & 0xFF);
        }
        return sum;
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkDateExtraction.class).run();
    }
}
