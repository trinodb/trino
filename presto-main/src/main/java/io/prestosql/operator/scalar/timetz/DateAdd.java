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

import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.time.TimeOperators;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeWithTimeZoneType.MAX_PRECISION;
import static io.prestosql.type.DateTimes.HOURS_PER_DAY;
import static io.prestosql.type.DateTimes.MILLISECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.SECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.round;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeConstants.MINUTES_PER_DAY;

@Description("Add the specified amount of time to the given time")
@ScalarFunction("date_add")
public class DateAdd
{
    private DateAdd() {}

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static long add(
            @LiteralParameter("p") long precision,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("time(p) with time zone") long packedTime)
    {
        long picos = add(unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND, unit, value);

        // smallest unit for "value" is millisecond, so we only need to round in the case below
        if (precision <= 3) {
            picos = round(picos, (int) (MAX_PRECISION - precision)) % PICOSECONDS_PER_DAY;
        }

        return packTimeWithTimeZone(picos / PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packedTime));
    }

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone add(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        long picos = add(time.getPicoSeconds(), unit, value);

        return new LongTimeWithTimeZone(picos, time.getOffsetMinutes());
    }

    private static long add(long picos, Slice unit, long value)
    {
        long delta = value;
        String unitString = unit.toStringAscii().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                delta = (delta % MILLISECONDS_PER_DAY) * PICOSECONDS_PER_MILLISECOND;
                break;
            case "second":
                delta = (delta % SECONDS_PER_DAY) * PICOSECONDS_PER_SECOND;
                break;
            case "minute":
                delta = (delta % MINUTES_PER_DAY) * PICOSECONDS_PER_MINUTE;
                break;
            case "hour":
                delta = (delta % HOURS_PER_DAY) * PICOSECONDS_PER_HOUR;
                break;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Time field");
        }

        return TimeOperators.add(picos, delta);
    }
}
