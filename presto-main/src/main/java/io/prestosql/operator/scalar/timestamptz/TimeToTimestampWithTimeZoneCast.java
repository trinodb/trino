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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimeToTimestampWithTimeZoneCast
{
    private TimeToTimestampWithTimeZoneCast() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p) with time zone")
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType(StandardTypes.TIME) long time)
    {
        // time is implicitly time(3), so truncate to expected precision
        long epochMillis = convert(session, round(time, (int) (3 - precision)));
        return packDateTimeWithZone(epochMillis, session.getTimeZoneKey());
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone cast(ConnectorSession session, @SqlType(StandardTypes.TIME) long time)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(convert(session, time), 0, session.getTimeZoneKey());
    }

    private static long convert(ConnectorSession session, long time)
    {
        if (session.isLegacyTimestamp()) {
            return time;
        }

        return getChronology(session.getTimeZoneKey())
                .getZone()
                .convertLocalToUTC(time, false);
    }
}
