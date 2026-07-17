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
package io.trino.operator.scalar.timestamp;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.type.LongInterval;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.getMicrosOfMilli;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static io.trino.type.IntervalDayTimeOperators.dateTimeDifference;

public final class TimestampOperators
{
    private TimestampOperators() {}

    // fallible
    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalDayToSecond
    {
        private TimestampPlusIntervalDayToSecond() {}

        // The result keeps at least microsecond precision (the historical interval resolution) and the
        // greater of the timestamp's and the interval's fractional-seconds precisions.
        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return timestamp + interval;
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return new LongTimestamp(timestamp.getEpochMicros() + interval, timestamp.getPicosOfMicro());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return addLongInterval(timestamp, 0, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return addLongInterval(timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), interval);
        }
    }

    // fallible
    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTimestamp
    {
        private IntervalDayToSecondPlusTimestamp() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(
                @SqlType("interval day(q) to second(r)") long interval,
                @SqlType("timestamp(p)") long timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("interval day(q) to second(r)") long interval,
                @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("interval day(q) to second(r)") LongInterval interval,
                @SqlType("timestamp(p)") long timestamp)
        {
            return addLongInterval(timestamp, 0, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp add(
                @SqlType("interval day(q) to second(r)") LongInterval interval,
                @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return addLongInterval(timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), interval);
        }
    }

    /// Adds a long (picosecond) interval to a timestamp's microseconds and picoseconds, carrying the
    /// sub-microsecond fraction.
    private static LongTimestamp addLongInterval(long epochMicros, int picosOfMicro, LongInterval interval)
    {
        long micros = epochMicros + interval.getMicros();
        int picos = picosOfMicro + interval.getPicosOfMicro();
        if (picos >= PICOSECONDS_PER_MICROSECOND) {
            micros++;
            picos -= PICOSECONDS_PER_MICROSECOND;
        }
        return new LongTimestamp(micros, picos);
    }

    /// Subtracts a long (picosecond) interval from a timestamp's microseconds and picoseconds, borrowing
    /// across the microsecond boundary.
    private static LongTimestamp subtractLongInterval(long epochMicros, int picosOfMicro, LongInterval interval)
    {
        long micros = epochMicros - interval.getMicros();
        int picos = picosOfMicro - interval.getPicosOfMicro();
        if (picos < 0) {
            micros--;
            picos += PICOSECONDS_PER_MICROSECOND;
        }
        return new LongTimestamp(micros, picos);
    }

    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalYearToMonth
    {
        private TimestampPlusIntervalYearToMonth() {}

        private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static long add(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            long fractionMicros = getMicrosOfMilli(timestamp);
            long result = MONTH_OF_YEAR_UTC.add(scaleEpochMicrosToMillis(timestamp), interval);
            return scaleEpochMillisToMicros(result) + fractionMicros;
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static LongTimestamp add(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            return new LongTimestamp(
                    add(timestamp.getEpochMicros(), interval),
                    timestamp.getPicosOfMicro());
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalYearToMonthPlusTimestamp
    {
        private IntervalYearToMonthPlusTimestamp() {}

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static long add(
                @SqlType("interval year(q) to month") long interval,
                @SqlType("timestamp(p)") long timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static LongTimestamp add(
                @SqlType("interval year(q) to month") long interval,
                @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalYearToMonth
    {
        private TimestampMinusIntervalYearToMonth() {}

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static long subtract(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p)")
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }
    }

    // fallible
    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalDayToSecond
    {
        private TimestampMinusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long subtract(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return subtractLongInterval(timestamp, 0, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return subtractLongInterval(timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), interval);
        }
    }

    public static SqlScalarFunction timestampMinusTimestamp()
    {
        return dateTimeDifference(TimestampOperators.class, TIMESTAMP, "subtractTimestampsShort", "subtractTimestampsLong");
    }

    @UsedByGeneratedCode
    public static long subtractTimestampsShort(long left, long right)
    {
        return left - right;
    }

    @UsedByGeneratedCode
    public static LongInterval subtractTimestampsLong(LongTimestamp left, LongTimestamp right)
    {
        long micros = left.getEpochMicros() - right.getEpochMicros();
        int picos = left.getPicosOfMicro() - right.getPicosOfMicro();
        if (picos < 0) {
            micros--;
            picos += PICOSECONDS_PER_MICROSECOND;
        }
        return new LongInterval(micros, picos);
    }
}
