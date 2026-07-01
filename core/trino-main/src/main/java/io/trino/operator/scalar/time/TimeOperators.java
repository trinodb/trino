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
package io.trino.operator.scalar.time;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.type.LongInterval;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MINUTES_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.rescaleWithRounding;
import static io.trino.type.DateTimes.scaleFactor;
import static io.trino.type.IntervalDayTimeOperators.dateTimeDifference;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class TimeOperators
{
    private TimeOperators() {}

    public static SqlScalarFunction timeMinusTime()
    {
        return dateTimeDifference(TimeOperators.class, TIME, "subtractShort", "subtractLong");
    }

    @UsedByGeneratedCode
    public static long subtractShort(long left, long right)
    {
        // a time is a count of picoseconds; a difference whose precision is 6 or coarser is whole microseconds
        return rescaleWithRounding(left - right, MAX_PRECISION, 6);
    }

    @UsedByGeneratedCode
    public static LongInterval subtractLong(long left, long right)
    {
        long picos = left - right;
        return new LongInterval(floorDiv(picos, PICOSECONDS_PER_MICROSECOND), (int) floorMod(picos, PICOSECONDS_PER_MICROSECOND));
    }

    // fallible
    @ScalarOperator(CAST)
    @LiteralParameters({"x", "p"})
    @SqlType("time(p)")
    public static long castFromVarchar(@LiteralParameter("p") long precision, @SqlType("varchar(x)") Slice value)
    {
        try {
            long picos = parseTime(value.toStringUtf8());
            return round(picos, (int) (MAX_PRECISION - precision)) % PICOSECONDS_PER_DAY;
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to time: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long castToTime(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("time(sourcePrecision)") long time)
    {
        if (sourcePrecision <= targetPrecision) {
            return time;
        }

        // round can round up to a value equal to 24h, so we need to compute module 24h
        return round(time, (int) (MAX_PRECISION - targetPrecision)) % PICOSECONDS_PER_DAY;
    }

    // The result keeps at least microsecond precision and the greater of the time's and the interval's
    // fractional-seconds precisions, so a picosecond interval is preserved.
    @ScalarOperator(ADD)
    public static final class TimePlusIntervalDayToSecond
    {
        private TimePlusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("time(p)") long time, @SqlType("interval day(q) to second(r)") long interval)
        {
            return TimeOperators.add(time, intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("time(p)") long time, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return TimeOperators.add(time, intervalPicos(interval));
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTime
    {
        private IntervalDayToSecondPlusTime() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("interval day(q) to second(r)") long interval, @SqlType("time(p)") long time)
        {
            return TimeOperators.add(time, intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("interval day(q) to second(r)") LongInterval interval, @SqlType("time(p)") long time)
        {
            return TimeOperators.add(time, intervalPicos(interval));
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimeMinusIntervalDayToSecond
    {
        private TimeMinusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long subtract(@SqlType("time(p)") long time, @SqlType("interval day(q) to second(r)") long interval)
        {
            return TimeOperators.add(time, -intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u)")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long subtract(@SqlType("time(p)") long time, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return TimeOperators.add(time, -intervalPicos(interval));
        }
    }

    /// The interval's value in picoseconds, reduced modulo a day so the running time stays within 24 hours.
    private static long intervalPicos(long intervalMicros)
    {
        return (long) floorMod(intervalMicros, MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND;
    }

    private static long intervalPicos(LongInterval interval)
    {
        return (long) floorMod(interval.getMicros(), MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND + interval.getPicosOfMicro();
    }

    // fallible
    @ScalarOperator(CAST)
    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @LiteralParameter("p") long precision, @SqlType("time(p)") long value)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
        int precisionInt = (int) precision;
        int size = (8 + // hour:minute:second
                (precisionInt > 0 ? 1 : 0) + // period
                precisionInt); // fraction

        int hours = (int) (value / PICOSECONDS_PER_HOUR);
        int minutes = (int) ((value / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR);
        int seconds = (int) ((value / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE);

        byte[] bytes = new byte[size];
        appendTwoDecimalDigits(0, bytes, hours);
        bytes[2] = ':';
        appendTwoDecimalDigits(3, bytes, minutes);
        bytes[5] = ':';
        appendTwoDecimalDigits(6, bytes, seconds);

        if (precisionInt > 0) {
            long scaledFraction = (value % PICOSECONDS_PER_SECOND) / scaleFactor(precisionInt, MAX_PRECISION);
            bytes[8] = '.';

            for (int index = 8 + precisionInt; index > 8; index--) {
                long temp = scaledFraction / 10;
                int digit = (int) (scaledFraction - (temp * 10));
                scaledFraction = temp;
                bytes[index] = (byte) ('0' + digit);
            }
        }

        // bytes are all-ASCII, so length here returns actual code points count
        if (bytes.length <= x) {
            return Slices.wrappedBuffer(bytes);
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", new String(bytes, US_ASCII), x));
    }

    private static void appendTwoDecimalDigits(int index, byte[] bytes, int value)
    {
        int tens = value / 10;
        int ones = value - (tens * 10);
        bytes[index] = (byte) ('0' + tens);
        bytes[index + 1] = (byte) ('0' + ones);
    }

    public static long add(long picos, long delta)
    {
        long result = (picos + (delta % PICOSECONDS_PER_DAY)) % PICOSECONDS_PER_DAY;
        if (result < 0) {
            result += PICOSECONDS_PER_DAY;
        }

        return result;
    }
}
