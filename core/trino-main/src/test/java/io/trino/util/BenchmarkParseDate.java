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

import io.trino.util.DateTimeUtils.FastDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.time.LocalDate;
import java.util.OptionalInt;
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
public class BenchmarkParseDate
{
    @Benchmark
    public void parseDate(BenchmarkData data, Blackhole blackhole)
    {
        for (String dt : data.dates) {
            blackhole.consume(DateTimeUtils.parseDate(dt));
        }
    }

    @Benchmark
    public void parseDateBaseline(BenchmarkData data, Blackhole blackhole)
    {
        // Replicates the pre-PR DateTimeUtils.parseIfIso8601DateFormat structure exactly
        // (length and separator checks, OptionalInt-style digit parsing) but with the original
        // LocalDate.of(y, m, d).toEpochDay() conversion. Apples-to-apples end-to-end baseline.
        for (String dt : data.dates) {
            if (dt.length() != 10 || dt.charAt(4) != '-' || dt.charAt(7) != '-') {
                continue;
            }
            OptionalInt year = parseIntSimple(dt, 0, 4);
            if (year.isEmpty()) {
                continue;
            }
            OptionalInt month = parseIntSimple(dt, 5, 2);
            if (month.isEmpty()) {
                continue;
            }
            OptionalInt day = parseIntSimple(dt, 8, 2);
            if (day.isEmpty()) {
                continue;
            }
            blackhole.consume((int) LocalDate.of(year.getAsInt(), month.getAsInt(), day.getAsInt()).toEpochDay());
        }
    }

    private static OptionalInt parseIntSimple(String s, int offset, int length)
    {
        int result = 0;
        for (int i = 0; i < length; i++) {
            int digit = s.charAt(offset + i) - '0';
            if (digit < 0 || digit > 9) {
                return OptionalInt.empty();
            }
            result = result * 10 + digit;
        }
        return OptionalInt.of(result);
    }

    // Pre-parsed (year, month, day) triples — isolates the y/m/d → days conversion cost.

    @Benchmark
    public void daysFromYmdJdk(BenchmarkData data, Blackhole blackhole)
    {
        int[] ymd = data.ymd;
        for (int i = 0; i < ymd.length; i += 3) {
            blackhole.consume(LocalDate.of(ymd[i], ymd[i + 1], ymd[i + 2]).toEpochDay());
        }
    }

    @Benchmark
    public void daysFromYmdFast(BenchmarkData data, Blackhole blackhole)
    {
        int[] ymd = data.ymd;
        for (int i = 0; i < ymd.length; i += 3) {
            blackhole.consume(FastDate.daysFromYmd(ymd[i], ymd[i + 1], ymd[i + 2]));
        }
    }

    // CAST(date AS varchar) — int days → "YYYY-MM-DD".

    private static final DateTimeFormatter JODA_DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    @Benchmark
    public void printDateJoda(BenchmarkData data, Blackhole blackhole)
    {
        for (int days : data.dayValues) {
            blackhole.consume(JODA_DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days)));
        }
    }

    @Benchmark
    public void printDateFast(BenchmarkData data, Blackhole blackhole)
    {
        for (int days : data.dayValues) {
            blackhole.consume(DateTimeUtils.printDate(days));
        }
    }

    @State(Thread)
    public static class BenchmarkData
    {
        String[] dates;
        int[] ymd; // packed year, month, day triples — same dates as above, pre-parsed
        int[] dayValues; // raw int days for printDate benchmarks

        @Setup
        public void setup()
        {
            dates = new String[100];
            ymd = new int[dates.length * 3];
            dayValues = new int[dates.length];
            // use the 100 consecutive dates start from 2023-01-01
            String startDate = "2023-01-01";
            int days = DateTimeUtils.parseDate(startDate);
            for (int i = 0; i < dates.length; i++) {
                dates[i] = DateTimeUtils.printDate(days + i);
                dayValues[i] = days + i;
                LocalDate ld = LocalDate.ofEpochDay(days + i);
                ymd[i * 3] = ld.getYear();
                ymd[i * 3 + 1] = ld.getMonthValue();
                ymd[i * 3 + 2] = ld.getDayOfMonth();
            }
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkParseDate.class).run();
    }
}
