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
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.scalar.timetz.TimeWithTimeZoneOperators.normalize;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.type.DateTimes.NANOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.NANOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static java.util.Locale.ENGLISH;

@Description("Difference of the given times in the given unit")
@ScalarFunction("date_diff")
public class DateDiff
{
    private DateDiff() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("time(p) with time zone") long left,
            @SqlType("time(p) with time zone") long right)
    {
        long nanos = normalize(right) - normalize(left);
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return nanos / NANOSECONDS_PER_MILLISECOND;
            case "second":
                return nanos / NANOSECONDS_PER_SECOND;
            case "minute":
                return nanos / NANOSECONDS_PER_MINUTE;
            case "hour":
                return nanos / NANOSECONDS_PER_HOUR;
            default:
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid TIME field");
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.BIGINT)
    public static long diff(
            @SqlType("varchar(x)") Slice unit,
            @SqlType("time(p) with time zone") LongTimeWithTimeZone left,
            @SqlType("time(p) with time zone") LongTimeWithTimeZone right)
    {
        long picos = normalize(right) - normalize(left);
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return picos / PICOSECONDS_PER_MILLISECOND;
            case "second":
                return picos / PICOSECONDS_PER_SECOND;
            case "minute":
                return picos / PICOSECONDS_PER_MINUTE;
            case "hour":
                return picos / PICOSECONDS_PER_HOUR;
            default:
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a TIME field");
        }
    }
}
