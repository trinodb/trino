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
import io.trino.operator.scalar.DateTimeUnit;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.type.DateTimes.HOURS_PER_DAY;
import static io.trino.type.DateTimes.MILLISECONDS_PER_DAY;
import static io.trino.type.DateTimes.MILLISECONDS_PER_SECOND;
import static io.trino.type.DateTimes.MINUTES_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.SECONDS_PER_DAY;
import static io.trino.type.DateTimes.SECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.round;
import static org.joda.time.DateTimeConstants.MINUTES_PER_DAY;

public class TimeFunctions
{
    private TimeFunctions() {}

    @Description("Millisecond of the second of the given time")
    @ScalarFunction("millisecond")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecond(@SqlType("time(p)") long time)
    {
        return (time / PICOSECONDS_PER_MILLISECOND) % MILLISECONDS_PER_SECOND;
    }

    @Description("Second of the minute of the given time")
    @ScalarFunction("second")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long second(@SqlType("time(p)") long time)
    {
        return (time / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE;
    }

    @Description("Minute of the hour of the given time")
    @ScalarFunction("minute")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long minute(@SqlType("time(p)") long time)
    {
        return (time / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR;
    }

    @Description("Hour of the day of the given time")
    @ScalarFunction("hour")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long hour(@SqlType("time(p)") long time)
    {
        return time / PICOSECONDS_PER_HOUR;
    }

    @Description("Truncate to the specified precision")
    @ScalarFunction("date_trunc")
    @LiteralParameters({"x", "p"})
    @SqlType("time(p)")
    public static long truncate(@SqlType("varchar(x)") Slice unitString, @SqlType("time(p)") long time)
    {
        DateTimeUnit unit = DateTimeUnit.valueOf(unitString, false)
                .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString.toStringUtf8() + "' is not a valid TIME field"));

        return switch (unit) {
            case MILLISECOND -> time / PICOSECONDS_PER_MILLISECOND * PICOSECONDS_PER_MILLISECOND;
            case SECOND -> time / PICOSECONDS_PER_SECOND * PICOSECONDS_PER_SECOND;
            case MINUTE -> time / PICOSECONDS_PER_MINUTE * PICOSECONDS_PER_MINUTE;
            case HOUR -> time / PICOSECONDS_PER_HOUR * PICOSECONDS_PER_HOUR;
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit + "' is not a valid TIME field");
        };
    }

    @Description("Add the specified amount of time to the given time")
    @LiteralParameters({"x", "p"})
    @ScalarFunction("date_add")
    @SqlType("time(p)")
    public static long dateAdd(
            @LiteralParameter("p") long precision,
            @SqlType("varchar(x)") Slice unitString,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("time(p)") long time)
    {
        DateTimeUnit unit = DateTimeUnit.valueOf(unitString, true)
                .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString.toStringUtf8() + "' is not a valid TIME field"));

        long delta = switch (unit) {
            case MILLISECOND -> (value % MILLISECONDS_PER_DAY) * PICOSECONDS_PER_MILLISECOND;
            case SECOND -> (value % SECONDS_PER_DAY) * PICOSECONDS_PER_SECOND;
            case MINUTE -> (value % MINUTES_PER_DAY) * PICOSECONDS_PER_MINUTE;
            case HOUR -> (value % HOURS_PER_DAY) * PICOSECONDS_PER_HOUR;
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit + "' is not a valid TIME field");
        };

        long result = TimeOperators.add(time, delta);

        // smallest unit for "value" is millisecond, so we only need to round in the case below
        if (precision <= 3) {
            return round(result, (int) (MAX_PRECISION - precision)) % PICOSECONDS_PER_DAY;
        }

        return result;
    }

    @Description("Difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long dateDiff(@SqlType("varchar(x)") Slice unitString, @SqlType("time(p)") long time1, @SqlType("time(p)") long time2)
    {
        long delta = time2 - time1;
        DateTimeUnit unit = DateTimeUnit.valueOf(unitString, true)
                .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString.toStringUtf8() + "' is not a valid TIME field"));

        return switch (unit) {
            case MILLISECOND -> delta / PICOSECONDS_PER_MILLISECOND;
            case SECOND -> delta / PICOSECONDS_PER_SECOND;
            case MINUTE -> delta / PICOSECONDS_PER_MINUTE;
            case HOUR -> delta / PICOSECONDS_PER_HOUR;
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid TIME field");
        };
    }
}
