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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneNotSupportedException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.lang.String.format;

@ScalarFunction("at_timezone")
public class AtTimeZone
{
    private AtTimeZone() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long atTimeZone(@SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType("varchar(x)") Slice zoneId)
    {
        try {
            return packDateTimeWithZone(unpackMillisUtc(packedEpochMillis), zoneId.toStringUtf8());
        }
        catch (TimeZoneNotSupportedException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone atTimeZone(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        try {
            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), getTimeZoneKey(zoneId.toStringUtf8()));
        }
        catch (TimeZoneNotSupportedException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
        }
    }
}
