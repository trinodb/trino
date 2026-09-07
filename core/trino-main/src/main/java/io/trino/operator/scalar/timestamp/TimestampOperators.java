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

import io.trino.spi.TrinoException;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.getMicrosOfMilli;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static java.lang.Math.addExact;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;

public final class TimestampOperators
{
    private TimestampOperators() {}

    // fallible
    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalDayToSecond
    {
        private TimestampPlusIntervalDayToSecond() {}

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long add(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            try {
                // scale to micros
                interval = multiplyExact(interval, MICROSECONDS_PER_MILLISECOND);

                return addExact(timestamp, interval);
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp out of range", e);
            }
        }

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimestamp add(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            return new LongTimestamp(add(timestamp.getEpochMicros(), interval), timestamp.getPicosOfMicro());
        }
    }

    // fallible
    @ScalarOperator(ADD)
    public static final class IntervalDayToSecondPlusTimestamp
    {
        private IntervalDayToSecondPlusTimestamp() {}

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long add(
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval,
                @SqlType("timestamp(p)") long timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimestamp add(
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval,
                @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, interval);
        }
    }

    // fallible
    @ScalarOperator(ADD)
    public static final class TimestampPlusIntervalYearToMonth
    {
        private TimestampPlusIntervalYearToMonth() {}

        private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static long add(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
        {
            try {
                long fractionMicros = getMicrosOfMilli(timestamp);
                long result = MONTH_OF_YEAR_UTC.add(scaleEpochMicrosToMillis(timestamp), interval);
                return addExact(scaleEpochMillisToMicros(result), fractionMicros);
            }
            catch (IllegalArgumentException | ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp out of range", e);
            }
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static LongTimestamp add(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
        {
            return new LongTimestamp(
                    add(timestamp.getEpochMicros(), interval),
                    timestamp.getPicosOfMicro());
        }
    }

    // fallible
    @ScalarOperator(ADD)
    public static final class IntervalYearToMonthPlusTimestamp
    {
        private IntervalYearToMonthPlusTimestamp() {}

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static long add(
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval,
                @SqlType("timestamp(p)") long timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static LongTimestamp add(
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval,
                @SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, interval);
        }
    }

    // fallible
    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalYearToMonth
    {
        private TimestampMinusIntervalYearToMonth() {}

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static long subtract(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }

        @LiteralParameters("p")
        @SqlType("timestamp(p)")
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
        {
            return TimestampPlusIntervalYearToMonth.add(timestamp, -interval);
        }
    }

    // fallible
    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusIntervalDayToSecond
    {
        private TimestampMinusIntervalDayToSecond() {}

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static long subtract(
                @SqlType("timestamp(p)") long timestamp,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, -interval);
        }

        @LiteralParameters({"p", "u"})
        @SqlType("timestamp(u)")
        @Constraint(variable = "u", expression = "max(3, p)") // Interval is currently p = 3, so the minimum result precision is 3.
        public static LongTimestamp subtract(
                @SqlType("timestamp(p)") LongTimestamp timestamp,
                @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
        {
            return TimestampPlusIntervalDayToSecond.add(timestamp, -interval);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class TimestampMinusTimestamp
    {
        private static final int HALF_MILLISECOND_IN_MICROS = MICROSECONDS_PER_MILLISECOND / 2;

        private TimestampMinusTimestamp() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
        public static long subtract(
                @SqlType("timestamp(p)") long left,
                @SqlType("timestamp(p)") long right)
        {
            return subtract(left, 0, right, 0);
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
        public static long subtract(
                @SqlType("timestamp(p)") LongTimestamp left,
                @SqlType("timestamp(p)") LongTimestamp right)
        {
            return subtract(left.getEpochMicros(), left.getPicosOfMicro(), right.getEpochMicros(), right.getPicosOfMicro());
        }

        private static long subtract(long leftEpochMicros, int leftPicosOfMicro, long rightEpochMicros, int rightPicosOfMicro)
        {
            // The difference in microseconds can overflow a long, while the difference in milliseconds cannot, so
            // subtract the milliseconds and round the sub-millisecond remainder separately.
            long millis = floorDiv(leftEpochMicros, MICROSECONDS_PER_MILLISECOND) - floorDiv(rightEpochMicros, MICROSECONDS_PER_MILLISECOND);
            int microsOfMilli = floorMod(leftEpochMicros, MICROSECONDS_PER_MILLISECOND) - floorMod(rightEpochMicros, MICROSECONDS_PER_MILLISECOND);
            int picosOfMicro = leftPicosOfMicro - rightPicosOfMicro;

            // round half up, as roundDiv does; the picoseconds decide the exact half-millisecond ties
            if (microsOfMilli > HALF_MILLISECOND_IN_MICROS || (microsOfMilli == HALF_MILLISECOND_IN_MICROS && picosOfMicro >= 0)) {
                return millis + 1;
            }
            if (microsOfMilli < -HALF_MILLISECOND_IN_MICROS || (microsOfMilli == -HALF_MILLISECOND_IN_MICROS && picosOfMicro < 0)) {
                return millis - 1;
            }
            return millis;
        }
    }
}
