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
package io.prestosql.operator.scalar.timestamptz;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneNotSupportedException;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
        }
    }
}
