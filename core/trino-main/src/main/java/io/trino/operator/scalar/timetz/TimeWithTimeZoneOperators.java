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
package io.trino.operator.scalar.timetz;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.time.TimeOperators;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.type.LongInterval;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.type.DateTimes.rescaleWithRounding;
import static io.trino.type.IntervalDayTimeOperators.dateTimeDifference;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

public final class TimeWithTimeZoneOperators
{
    private TimeWithTimeZoneOperators() {}

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     *
     * @return the time in nanoseconds
     */
    static long normalize(long packedTime)
    {
        long nanos = unpackTimeNanos(packedTime);
        int offsetMinutes = unpackOffsetMinutes(packedTime);
        return floorMod(nanos - offsetMinutes * NANOSECONDS_PER_MINUTE, NANOSECONDS_PER_DAY);
    }

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     *
     * @return the time in picoseconds
     */
    static long normalize(LongTimeWithTimeZone time)
    {
        return floorMod(time.getPicoseconds() - time.getOffsetMinutes() * PICOSECONDS_PER_MINUTE, PICOSECONDS_PER_DAY);
    }

    // A short time with time zone holds nanoseconds (precisions 0-9) and the long form holds picoseconds
    // (10-12). The result keeps at least microsecond precision and the greater of the time's and the
    // interval's fractional-seconds precisions, so a picosecond interval survives and may itself push the
    // result into the long form.
    @ScalarOperator(ADD)
    public static final class TimePlusIntervalDayToSecond
    {
        private TimePlusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") long interval)
        {
            return packedPlusPicos(packedTime, intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return packedPlusPicos(packedTime, intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone addToLong(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return longPlusPicos(unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packedTime), intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone add(@SqlType("time(p) with time zone") LongTimeWithTimeZone time, @SqlType("interval day(q) to second(r)") long interval)
        {
            return longPlusPicos(time.getPicoseconds(), time.getOffsetMinutes(), intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone add(@SqlType("time(p) with time zone") LongTimeWithTimeZone time, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return longPlusPicos(time.getPicoseconds(), time.getOffsetMinutes(), intervalPicos(interval));
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTime
    {
        private IntervalDayToSecondPlusTime() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("interval day(q) to second(r)") long interval, @SqlType("time(p) with time zone") long time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long add(@SqlType("interval day(q) to second(r)") LongInterval interval, @SqlType("time(p) with time zone") long time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone addToLong(@SqlType("interval day(q) to second(r)") LongInterval interval, @SqlType("time(p) with time zone") long time)
        {
            return TimePlusIntervalDayToSecond.addToLong(time, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone add(@SqlType("interval day(q) to second(r)") long interval, @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone add(@SqlType("interval day(q) to second(r)") LongInterval interval, @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimeMinusIntervalDayToSecond
    {
        private TimeMinusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long subtract(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") long interval)
        {
            return packedPlusPicos(packedTime, -intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static long subtract(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return packedPlusPicos(packedTime, -intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone subtractToLong(@SqlType("time(p) with time zone") long packedTime, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return longPlusPicos(unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packedTime), -intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone subtract(@SqlType("time(p) with time zone") LongTimeWithTimeZone time, @SqlType("interval day(q) to second(r)") long interval)
        {
            return longPlusPicos(time.getPicoseconds(), time.getOffsetMinutes(), -intervalPicos(interval));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(6, max(r, p))")
        public static LongTimeWithTimeZone subtract(@SqlType("time(p) with time zone") LongTimeWithTimeZone time, @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return longPlusPicos(time.getPicoseconds(), time.getOffsetMinutes(), -intervalPicos(interval));
        }
    }

    private static long intervalPicos(long intervalMicros)
    {
        return (long) floorMod(intervalMicros, MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND;
    }

    private static long intervalPicos(LongInterval interval)
    {
        return (long) floorMod(interval.getMicros(), MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND + interval.getPicosOfMicro();
    }

    /// Adds picoseconds (modulo a day) to a short time with time zone, producing a short time with time zone.
    private static long packedPlusPicos(long packedTime, long deltaPicos)
    {
        long picos = TimeOperators.add(unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND, deltaPicos);
        return packTimeWithTimeZone(picos / PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packedTime));
    }

    /// Adds picoseconds (modulo a day) to a time with time zone given as picoseconds and offset, producing
    /// the long form.
    private static LongTimeWithTimeZone longPlusPicos(long picoseconds, int offsetMinutes, long deltaPicos)
    {
        return new LongTimeWithTimeZone(TimeOperators.add(picoseconds, deltaPicos), offsetMinutes);
    }

    public static SqlScalarFunction timeMinusTime()
    {
        // a short time with time zone holds nanoseconds, so it spans precisions 0-9 and can itself produce a
        // long (picosecond) interval; the long form holds picoseconds for precisions 10-12
        return dateTimeDifference(
                TimeWithTimeZoneOperators.class,
                TIME_WITH_TIME_ZONE,
                "subtractShort",
                "subtractShortToLong",
                "subtractLong");
    }

    @UsedByGeneratedCode
    public static long subtractShort(long left, long right)
    {
        return rescaleWithRounding(normalize(left) - normalize(right), 9, 6);
    }

    @UsedByGeneratedCode
    public static LongInterval subtractShortToLong(long left, long right)
    {
        return fromPicos((normalize(left) - normalize(right)) * PICOSECONDS_PER_NANOSECOND);
    }

    @UsedByGeneratedCode
    public static LongInterval subtractLong(LongTimeWithTimeZone left, LongTimeWithTimeZone right)
    {
        return fromPicos(normalize(left) - normalize(right));
    }

    private static LongInterval fromPicos(long picos)
    {
        return new LongInterval(floorDiv(picos, PICOSECONDS_PER_MICROSECOND), (int) floorMod(picos, PICOSECONDS_PER_MICROSECOND));
    }
}
