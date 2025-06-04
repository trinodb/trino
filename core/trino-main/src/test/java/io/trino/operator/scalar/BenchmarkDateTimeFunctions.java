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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Test;
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

import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Locale.ENGLISH;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDateTimeFunctions
{
    private static final ISOChronology ISO_CHRONOLOGY = ISOChronology.getInstanceUTC();

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
                "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"})
        private String unitString = "year";
        private Slice unit;

        @Setup
        public void setup()
        {
            this.unit = Slices.utf8Slice(unitString);
        }

        public Slice getUnit()
        {
            return unit;
        }
    }

    // Old implementation
    public static DateTimeField getTimestampFieldOld(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        return switch (unitString) {
            case "millisecond" -> chronology.millisOfSecond();
            case "second" -> chronology.secondOfMinute();
            case "minute" -> chronology.minuteOfHour();
            case "hour" -> chronology.hourOfDay();
            case "day" -> chronology.dayOfMonth();
            case "week" -> chronology.weekOfWeekyear();
            case "month" -> chronology.monthOfYear();
            case "quarter" -> QUARTER_OF_YEAR.getField(chronology);
            case "year" -> chronology.year();
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid TIMESTAMP field");
        };
    }

    @Benchmark
    public DateTimeField getTimestampFieldOld(BenchmarkData data)
    {
        return getTimestampFieldOld(ISO_CHRONOLOGY, data.getUnit());
    }

    @Benchmark
    public DateTimeField getTimestampFieldNew(BenchmarkData data)
    {
        return DateTimeFunctions.getTimestampField(ISO_CHRONOLOGY, data.getUnit());
    }

    @Test
    public void test()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        getTimestampFieldOld(data);
        getTimestampFieldNew(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkDateTimeFunctions.class).run();
    }
}
