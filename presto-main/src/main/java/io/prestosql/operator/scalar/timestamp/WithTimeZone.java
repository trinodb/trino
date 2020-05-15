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
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTimeZone;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;

@ScalarFunction("with_timezone")
public class WithTimeZone
{
    private WithTimeZone() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long withTimezone(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        if (precision > 3) {
            timestamp = scaleEpochMicrosToMillis(round(timestamp, 3));
        }

        TimeZoneKey toTimeZoneKey = getTimeZoneKey(zoneId.toStringUtf8());
        DateTimeZone fromDateTimeZone = session.isLegacyTimestamp() ? getDateTimeZone(session.getTimeZoneKey()) : DateTimeZone.UTC;
        DateTimeZone toDateTimeZone = getDateTimeZone(toTimeZoneKey);
        return packDateTimeWithZone(fromDateTimeZone.getMillisKeepLocal(toDateTimeZone, timestamp), toTimeZoneKey);
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long withTimezone(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        return withTimezone(6, session, timestamp.getEpochMicros(), zoneId);
    }
}
