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
import io.trino.plugin.base.time.DateTimeFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;

import java.time.ZoneId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.roundToEpochMillis;

@Description("Formats the given time by the given format")
@ScalarFunction("format_datetime")
public final class FormatDateTime
{
    private FormatDateTime() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType("varchar(x)") Slice formatString)
    {
        ZoneId zoneId = unpackZoneKey(packedEpochMillis).getZoneId();
        long epochMillis = unpackMillisUtc(packedEpochMillis);

        return format(session, epochMillis, zoneId, formatString);
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return format(session, roundToEpochMillis(timestamp), getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId(), formatString);
    }

    private static Slice format(ConnectorSession session, long epochMillis, ZoneId zoneId, Slice formatString)
    {
        try {
            return utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8()).format(epochMillis, zoneId, session.getLocale()));
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
