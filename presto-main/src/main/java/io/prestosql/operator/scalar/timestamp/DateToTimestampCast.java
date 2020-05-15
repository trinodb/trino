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

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class DateToTimestampCast
{
    private DateToTimestampCast() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType(StandardTypes.DATE) long date)
    {
        long result;
        if (session.isLegacyTimestamp()) {
            long utcMillis = TimeUnit.DAYS.toMillis(date);

            // date is encoded as milliseconds at midnight in UTC
            // convert to midnight in the session timezone
            ISOChronology chronology = getChronology(session.getTimeZoneKey());
            result = utcMillis - chronology.getZone().getOffset(utcMillis);
        }
        else {
            result = TimeUnit.DAYS.toMillis(date);
        }

        if (precision > 3) {
            return scaleEpochMillisToMicros(result);
        }

        return result;
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp cast(ConnectorSession session, @SqlType(StandardTypes.DATE) long date)
    {
        return new LongTimestamp(cast(6, session, date), 0);
    }
}
