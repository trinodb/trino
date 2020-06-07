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

import static io.prestosql.operator.scalar.DateTimeFunctions.valueToSessionTimeZoneOffsetDiff;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.DateTimeOperators.modulo24Hour;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;

@ScalarOperator(CAST)
public final class TimestampToTimeCast
{
    private TimestampToTimeCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME)
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long value)
    {
        if (precision > 3) {
            value = scaleEpochMicrosToMillis(round(value, 3));
        }

        if (session.isLegacyTimestamp()) {
            ISOChronology chronology = getChronology(session.getTimeZoneKey());
            long result = chronology.millisOfDay().get(value) - chronology.getZone().getOffset(value);
            result -= valueToSessionTimeZoneOffsetDiff(value, getDateTimeZone(session.getTimeZoneKey()));
            return result;
        }

        return modulo24Hour(value);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME)
    public static long cast(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return cast(6, session, timestamp.getEpochMicros());
    }
}
