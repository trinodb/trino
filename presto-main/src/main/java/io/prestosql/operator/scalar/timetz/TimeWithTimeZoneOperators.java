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
package io.prestosql.operator.scalar.timetz;

import io.airlift.slice.XxHash64;
import io.prestosql.operator.scalar.time.TimeOperators;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.Constraint;

import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeWithTimezoneTypes.hashLongTimeWithTimeZone;
import static io.prestosql.spi.type.TimeWithTimezoneTypes.hashShortTimeWithTimeZone;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.rescaleWithRounding;
import static java.lang.Math.floorMod;

@SuppressWarnings("UtilityClassWithoutPrivateConstructor")
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
        return floorMod(time.getPicoSeconds() - time.getOffsetMinutes() * PICOSECONDS_PER_MINUTE, PICOSECONDS_PER_DAY);
    }

    @ScalarOperator(EQUAL)
    public static final class Equal
    {
        @LiteralParameters("p")
        @SqlNullable
        @SqlType(StandardTypes.BOOLEAN)
        public static Boolean equal(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return normalize(left) == normalize(right);
        }

        @LiteralParameters("p")
        @SqlNullable
        @SqlType(StandardTypes.BOOLEAN)
        public static Boolean equal(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return normalize(left) == normalize(right);
        }
    }

    @ScalarOperator(NOT_EQUAL)
    public static final class NotEqual
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean notEqual(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return !Equal.equal(left, right);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean notEqual(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return !Equal.equal(left, right);
        }
    }

    @ScalarOperator(LESS_THAN)
    public static final class LessThan
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean lessThan(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return normalize(left) < normalize(right);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean lessThan(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return normalize(left) < normalize(right);
        }
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static final class LessThanOrEqual
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean lessThanOrEqual(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return normalize(left) <= normalize(right);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean lessThanOrEqual(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return normalize(left) <= normalize(right);
        }
    }

    @ScalarOperator(GREATER_THAN)
    public static final class GreaterThan
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean greaterThan(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return !LessThanOrEqual.lessThanOrEqual(left, right);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean greaterThan(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return !LessThanOrEqual.lessThanOrEqual(left, right);
        }
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static final class GreaterThanOrEqual
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean greaterThanOrEqual(@SqlType("time(p) with time zone") long left, @SqlType("time(p) with time zone") long right)
        {
            return !LessThan.lessThan(left, right);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean greaterThanOrEqual(@SqlType("time(p) with time zone") LongTimeWithTimeZone left, @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            return !LessThan.lessThan(left, right);
        }
    }

    @ScalarOperator(HASH_CODE)
    public static final class HashCode
    {
        @SqlType(StandardTypes.BIGINT)
        @LiteralParameters("p")
        public static long hashCode(@SqlType("time(p) with time zone") long packedTime)
        {
            return hashShortTimeWithTimeZone(packedTime);
        }

        @SqlType(StandardTypes.BIGINT)
        @LiteralParameters("p")
        public static long hashCode(@SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return hashLongTimeWithTimeZone(time);
        }
    }

    @ScalarOperator(INDETERMINATE)
    public static final class Indeterminate
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean indeterminate(@SqlType("time(p) with time zone") long value, @IsNull boolean isNull)
        {
            return isNull;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean indeterminate(@SqlType("time(p) with time zone") LongTimeWithTimeZone value, @IsNull boolean isNull)
        {
            return isNull;
        }
    }

    @ScalarOperator(XX_HASH_64)
    public static final class XxHash64Operator
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BIGINT)
        public static long xxHash64(@SqlType("time(p) with time zone") long packedTime)
        {
            return XxHash64.hash(normalize(packedTime));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BIGINT)
        public static long xxHash64(@SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return XxHash64.hash(normalize(time));
        }
    }

    @ScalarOperator(ADD)
    public static final class TimePlusIntervalDayToSecond
    {
        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long add(
                @SqlType("time(p) with time zone") long packedTime,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            long picos = unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND;
            long nanos = TimeOperators.add(picos, interval * PICOSECONDS_PER_MILLISECOND) / PICOSECONDS_PER_NANOSECOND;

            return packTimeWithTimeZone(nanos, unpackOffsetMinutes(packedTime));
        }

        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimeWithTimeZone add(
                @SqlType("time(p) with time zone") LongTimeWithTimeZone time,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            long picos = TimeOperators.add(time.getPicoSeconds(), interval * PICOSECONDS_PER_MILLISECOND);
            return new LongTimeWithTimeZone(picos, time.getOffsetMinutes());
        }
    }

    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTime
    {
        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long add(
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval,
                @SqlType("time(p) with time zone") long time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }

        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimeWithTimeZone add(
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval,
                @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
        {
            return TimePlusIntervalDayToSecond.add(time, interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimeMinusIntervalDayToSecond
    {
        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long subtract(
                @SqlType("time(p) with time zone") long packedTime,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            return TimePlusIntervalDayToSecond.add(packedTime, -interval);
        }

        @LiteralParameters({"p", "u"})
        @SqlType("time(u) with time zone")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimeWithTimeZone subtract(
                @SqlType("time(p) with time zone") LongTimeWithTimeZone time,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            return TimePlusIntervalDayToSecond.add(time, -interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimeMinusTime
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
        public static long subtract(
                @SqlType("time(p) with time zone") long left,
                @SqlType("time(p) with time zone") long right)
        {
            long nanos = normalize(left) - normalize(right);
            return rescaleWithRounding(nanos, 9, 3);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
        public static long subtract(
                @SqlType("time(p) with time zone") LongTimeWithTimeZone left,
                @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
        {
            long picos = normalize(left) - normalize(right);
            return rescaleWithRounding(picos, 12, 3);
        }
    }
}
