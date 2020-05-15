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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.type.TimeWithTimeZoneOperators.REFERENCE_TIMESTAMP_UTC;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimeWithTimezoneToTimestampCast
{
    private TimeWithTimezoneToTimestampCast() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        long epochMillis;
        if (session.isLegacyTimestamp()) {
            epochMillis = unpackMillisUtc(time);
        }
        else {
            // This is hack that we need to use as the timezone interpretation depends on date (not only on time)
            // TODO remove REFERENCE_TIMESTAMP_UTC when removing support for political time zones in TIME WITH TIME ZONE
            long currentMillisOfDay = ChronoField.MILLI_OF_DAY.getFrom(Instant.ofEpochMilli(REFERENCE_TIMESTAMP_UTC).atZone(ZoneOffset.UTC));
            long timeMillisUtcInCurrentDay = REFERENCE_TIMESTAMP_UTC - currentMillisOfDay + unpackMillisUtc(time);

            ISOChronology chronology = getChronology(unpackZoneKey(time));
            epochMillis = unpackMillisUtc(time) + chronology.getZone().getOffset(timeMillisUtcInCurrentDay);
        }

        if (precision > 3) {
            return scaleEpochMillisToMicros(epochMillis);
        }

        if (precision < 3) {
            return round(epochMillis, (int) (3 - precision));
        }

        return epochMillis;
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp cast(ConnectorSession session, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        return new LongTimestamp(cast(6, session, time), 0);
    }
}
