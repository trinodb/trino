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
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
@ScalarFunction("date")
public final class TimestampToDateCast
{
    private TimestampToDateCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
    {
        if (precision > 3) {
            timestamp = scaleEpochMicrosToMillis(timestamp);
        }

        ISOChronology chronology;
        if (session.isLegacyTimestamp()) {
            // round down the current timestamp to days
            chronology = getChronology(session.getTimeZoneKey());
            long date = chronology.dayOfYear().roundFloor(timestamp);
            // date is currently midnight in timezone of the session
            // convert to UTC
            long millis = date + chronology.getZone().getOffset(date);
            return TimeUnit.MILLISECONDS.toDays(millis);
        }

        return TimeUnit.MILLISECONDS.toDays(timestamp);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return cast(6, session, timestamp.getEpochMicros());
    }
}
