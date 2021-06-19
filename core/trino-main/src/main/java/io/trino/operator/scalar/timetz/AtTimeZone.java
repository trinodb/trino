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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimeZoneNotSupportedException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.getOffsetMinutes;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

@ScalarFunction(value = "$at_timezone", hidden = true)
public class AtTimeZone
{
    private AtTimeZone() {}

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static long atTimeZone(
            ConnectorSession session,
            @SqlType("time(p) with time zone") long packedTime,
            @SqlType("varchar(x)") Slice zoneId)
    {
        TimeZoneKey zoneKey;
        try {
            zoneKey = getTimeZoneKey(zoneId.toStringUtf8());
        }
        catch (TimeZoneNotSupportedException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId));
        }

        int offsetMinutes = getOffsetMinutes(session.getStart(), zoneKey);
        long nanos = unpackTimeNanos(packedTime) - (unpackOffsetMinutes(packedTime) - offsetMinutes) * NANOSECONDS_PER_MINUTE;
        return packTimeWithTimeZone(floorMod(nanos, NANOSECONDS_PER_DAY), offsetMinutes);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone atTimeZone(
            ConnectorSession session,
            @SqlType("time(p) with time zone") LongTimeWithTimeZone time,
            @SqlType("varchar(x)") Slice zoneId)
    {
        TimeZoneKey zoneKey;
        try {
            zoneKey = getTimeZoneKey(zoneId.toStringUtf8());
        }
        catch (TimeZoneNotSupportedException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId));
        }

        int offsetMinutes = getOffsetMinutes(session.getStart(), zoneKey);
        long picos = time.getPicoseconds() - (time.getOffsetMinutes() - offsetMinutes) * PICOSECONDS_PER_MINUTE;
        return new LongTimeWithTimeZone(floorMod(picos, PICOSECONDS_PER_DAY), offsetMinutes);
    }
}
