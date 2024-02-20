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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static java.util.Locale.ENGLISH;

@Description("Truncate to the specified precision")
@ScalarFunction("date_trunc")
public final class DateTrunc
{
    private DateTrunc() {}

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static long truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("time(p) with time zone") long packedTime)
    {
        long picos = unpackTimeNanos(packedTime) * PICOSECONDS_PER_NANOSECOND;
        return packTimeWithTimeZone(truncate(picos, unit) / PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(packedTime));
    }

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        return new LongTimeWithTimeZone(truncate(time.getPicoseconds(), unit), time.getOffsetMinutes());
    }

    private static long truncate(long picos, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);

        switch (unitString) {
            case "millisecond":
                return picos / PICOSECONDS_PER_MILLISECOND * PICOSECONDS_PER_MILLISECOND;
            case "second":
                return picos / PICOSECONDS_PER_SECOND * PICOSECONDS_PER_SECOND;
            case "minute":
                return picos / PICOSECONDS_PER_MINUTE * PICOSECONDS_PER_MINUTE;
            case "hour":
                return picos / PICOSECONDS_PER_HOUR * PICOSECONDS_PER_HOUR;
            default:
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid TIME field");
        }
    }
}
