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

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimestampWithTimezoneToTimestampCast
{
    private TimestampWithTimezoneToTimestampCast() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        long epochMillis;
        if (session.isLegacyTimestamp()) {
            epochMillis = unpackMillisUtc(value);
        }
        else {
            ISOChronology chronology = getChronology(unpackZoneKey(value));
            epochMillis = chronology.getZone().convertUTCToLocal(unpackMillisUtc(value));
        }

        if (precision > 3) {
            return scaleEpochMillisToMicros(epochMillis);
        }

        return round(epochMillis, (int) (3 - precision));
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp cast(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long time)
    {
        return new LongTimestamp(cast(6, session, time), 0);
    }
}
