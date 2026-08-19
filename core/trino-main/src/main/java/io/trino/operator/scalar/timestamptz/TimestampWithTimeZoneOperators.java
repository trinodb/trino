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
package io.trino.operator.scalar.timestamptz;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.type.LongInterval;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.IntervalCasts.negatePicos;
import static io.trino.type.IntervalDayTimeOperators.dateTimeDifference;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;

public final class TimestampWithTimeZoneOperators
{
    private TimestampWithTimeZoneOperators() {}

    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalDayToSecond
    {
        private TimestampPlusIntervalDayToSecond() {}

        // The result keeps the timestamp-with-time-zone's millisecond floor and the greater of the
        // timestamp's and the interval's fractional-seconds precisions, so a picosecond interval survives.
        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static long add(
                @SqlType("timestamp(p) with time zone") long packedEpochMillis,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return packDateTimeWithZone(unpackMillisUtc(packedEpochMillis) + interval / MICROSECONDS_PER_MILLISECOND, unpackZoneKey(packedEpochMillis));
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return addShortInterval(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), interval, timestamp.getTimeZoneKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone addToLong(
                @SqlType("timestamp(p) with time zone") long packedEpochMillis,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return addShortInterval(unpackMillisUtc(packedEpochMillis), 0, interval, unpackZoneKey(packedEpochMillis).getKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("timestamp(p) with time zone") long packedEpochMillis,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return addLongInterval(unpackMillisUtc(packedEpochMillis), 0, interval, unpackZoneKey(packedEpochMillis).getKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            return addLongInterval(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), interval, timestamp.getTimeZoneKey());
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTimestamp
    {
        private IntervalDayToSecondPlusTimestamp() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static long add(
                @SqlType("interval day(q) to second(r)") long interval,
                @SqlType("timestamp(p) with time zone") long timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("interval day(q) to second(r)") long interval,
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone addToLong(
                @SqlType("interval day(q) to second(r)") long interval,
                @SqlType("timestamp(p) with time zone") long timestamp)
        {
            return TimestampPlusIntervalDayToSecond.addToLong(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("interval day(q) to second(r)") LongInterval interval,
                @SqlType("timestamp(p) with time zone") long timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone add(
                @SqlType("interval day(q) to second(r)") LongInterval interval,
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }
    }

    private static LongTimestampWithTimeZone addShortInterval(long epochMillis, int picosOfMilli, long intervalMicros, short zoneKey)
    {
        return combineInterval(epochMillis, picosOfMilli, intervalMicros, 0, zoneKey);
    }

    private static LongTimestampWithTimeZone addLongInterval(long epochMillis, int picosOfMilli, LongInterval interval, short zoneKey)
    {
        return combineInterval(epochMillis, picosOfMilli, interval.getMicros(), interval.getPicosOfMicro(), zoneKey);
    }

    /// Adds an interval's `(micros, picosOfMicro)` to a timestamp-with-time-zone's `(epochMillis,
    /// picosOfMilli)`, decomposing the interval into milliseconds and the picoseconds within the last
    /// millisecond and carrying the fraction.
    private static LongTimestampWithTimeZone combineInterval(long epochMillis, int picosOfMilli, long intervalMicros, int intervalPicosOfMicro, short zoneKey)
    {
        long deltaMillis = Math.floorDiv(intervalMicros, MICROSECONDS_PER_MILLISECOND);
        long deltaPicosOfMilli = (long) Math.floorMod(intervalMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND + intervalPicosOfMicro;
        long resultMillis = epochMillis + deltaMillis;
        long resultPicos = picosOfMilli + deltaPicosOfMilli;
        if (resultPicos >= PICOSECONDS_PER_MILLISECOND) {
            resultMillis++;
            resultPicos -= PICOSECONDS_PER_MILLISECOND;
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(resultMillis, (int) resultPicos, zoneKey);
    }

    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalYearToMonth
    {
        private TimestampPlusIntervalYearToMonth() {}

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static long add(
                @SqlType("timestamp(p) with time zone") long packedEpochMillis,
                @SqlType("interval year(q) to month") long interval)
        {
            long epochMillis = unpackMillisUtc(packedEpochMillis);
            long result = unpackChronology(packedEpochMillis).monthOfYear().add(epochMillis, interval);

            return packDateTimeWithZone(result, unpackZoneKey(packedEpochMillis));
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone add(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            long epochMillis = timestamp.getEpochMillis();
            long result = unpackChronology(timestamp.getTimeZoneKey()).monthOfYear().add(epochMillis, interval);

            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(result, timestamp.getPicosOfMilli(), timestamp.getTimeZoneKey());
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalYearToMonthPlusTimestamp
    {
        private IntervalYearToMonthPlusTimestamp() {}

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static long add(
                @SqlType("interval year(q) to month") long interval,
                @SqlType("timestamp(p) with time zone") long timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone add(
                @SqlType("interval year(q) to month") long interval,
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalYearToMonth
    {
        private TimestampMinusIntervalYearToMonth() {}

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static long subtract(
                @SqlType("timestamp(p) with time zone") long timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "q"})
        @SqlType("timestamp(p) with time zone")
        public static LongTimestampWithTimeZone subtract(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval year(q) to month") long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalDayToSecond
    {
        private TimestampMinusIntervalDayToSecond() {}

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static long subtract(
                @SqlType("timestamp(p) with time zone") long timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone subtract(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return addShortInterval(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), -interval, timestamp.getTimeZoneKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone subtractToLong(
                @SqlType("timestamp(p) with time zone") long timestamp,
                @SqlType("interval day(q) to second(r)") long interval)
        {
            return addShortInterval(unpackMillisUtc(timestamp), 0, -interval, unpackZoneKey(timestamp).getKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone subtract(
                @SqlType("timestamp(p) with time zone") long timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            long[] negated = negatePicos(interval.getMicros(), interval.getPicosOfMicro());
            return combineInterval(unpackMillisUtc(timestamp), 0, negated[0], (int) negated[1], unpackZoneKey(timestamp).getKey());
        }

        @LiteralParameters({"p", "q", "r", "u"})
        @SqlType("timestamp(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, max(r, p))")
        public static LongTimestampWithTimeZone subtract(
                @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
                @SqlType("interval day(q) to second(r)") LongInterval interval)
        {
            long[] negated = negatePicos(interval.getMicros(), interval.getPicosOfMicro());
            return combineInterval(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), negated[0], (int) negated[1], timestamp.getTimeZoneKey());
        }
    }

    public static SqlScalarFunction timestampMinusTimestamp()
    {
        return dateTimeDifference(
                TimestampWithTimeZoneOperators.class,
                TIMESTAMP_WITH_TIME_ZONE,
                "subtractShort",
                "subtractLongToShort",
                "subtractLongToLong");
    }

    @UsedByGeneratedCode
    public static long subtractShort(long left, long right)
    {
        return (unpackMillisUtc(left) - unpackMillisUtc(right)) * MICROSECONDS_PER_MILLISECOND;
    }

    @UsedByGeneratedCode
    public static long subtractLongToShort(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        // a difference whose precision is 6 or coarser is whole microseconds, so the picoseconds are zero
        return difference(left, right).getMicros();
    }

    @UsedByGeneratedCode
    public static LongInterval subtractLongToLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return difference(left, right);
    }

    /// The difference of two picosecond-resolution timestamps with time zone, as microseconds plus the
    /// picoseconds within the last microsecond. The picosecond fraction lives within a millisecond, so the
    /// borrow is across the millisecond before the value is split into microseconds.
    private static LongInterval difference(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        long millis = left.getEpochMillis() - right.getEpochMillis();
        int picosOfMilli = left.getPicosOfMilli() - right.getPicosOfMilli();
        if (picosOfMilli < 0) {
            millis--;
            picosOfMilli += PICOSECONDS_PER_MILLISECOND;
        }
        long micros = millis * MICROSECONDS_PER_MILLISECOND + picosOfMilli / PICOSECONDS_PER_MICROSECOND;
        return new LongInterval(micros, picosOfMilli % PICOSECONDS_PER_MICROSECOND);
    }
}
