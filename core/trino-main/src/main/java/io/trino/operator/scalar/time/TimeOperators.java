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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.type.Constraint;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.type.DateTimes.MINUTES_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.SECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.rescaleWithRounding;
import static io.trino.type.DateTimes.round;
import static io.trino.type.DateTimes.scaleFactor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TimeOperators
{
    private TimeOperators() {}

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("p")
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(@SqlType("time(p)") long left, @SqlType("time(p)") long right)
    {
        long interval = left - right;

        interval = rescaleWithRounding(interval, MAX_PRECISION, 3);

        return interval;
    }

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

    @ScalarOperator(ADD)
    @LiteralParameters({"p", "u"})
    @SqlType("time(u)")
    @Constraint(variable = "u", expression = "max(3, p)") // interval is currently p = 3
    public static long timePlusIntervalDayToSecond(@SqlType("time(p)") long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return add(time, interval * PICOSECONDS_PER_MILLISECOND);
    }

    @ScalarOperator(ADD)
    @LiteralParameters({"p", "u"})
    @SqlType("time(u)")
    @Constraint(variable = "u", expression = "max(3, p)") // interval is currently p = 3
    public static long intervalDayToSecondPlusTime(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType("time(p)") long time)
    {
        return timePlusIntervalDayToSecond(time, interval);
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters({"p", "u"})
    @SqlType("time(u)")
    @Constraint(variable = "u", expression = "max(3, p)") // interval is currently p = 3
    public static long timeMinusIntervalDayToSecond(@SqlType("time(p)") long time, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        return add(time, -interval * PICOSECONDS_PER_MILLISECOND);
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("p") long precision, @SqlType("time(p)") long value)
    {
        int size = (int) (8 + // hour:minute:second
                (precision > 0 ? 1 : 0) + // period
                precision); // fraction

        DynamicSliceOutput output = new DynamicSliceOutput(size);

        String formatted = format(
                "%02d:%02d:%02d",
                value / PICOSECONDS_PER_HOUR,
                (value / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR,
                (value / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE);
        output.appendBytes(formatted.getBytes(UTF_8));

        if (precision > 0) {
            long scaledFraction = (value % PICOSECONDS_PER_SECOND) / scaleFactor((int) precision, MAX_PRECISION);
            output.appendByte('.');
            output.appendBytes(format("%0" + precision + "d", scaledFraction).getBytes(UTF_8));
        }

        return output.slice();
    }

    public static long add(long picos, long delta)
    {
        long result = (picos + delta) % PICOSECONDS_PER_DAY;
        if (result < 0) {
            result += PICOSECONDS_PER_DAY;
        }

        return result;
    }
}
