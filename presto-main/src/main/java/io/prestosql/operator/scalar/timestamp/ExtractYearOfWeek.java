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
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@Description("Year of the ISO week of the given timestamp")
@ScalarFunction(value = "year_of_week", alias = "yow")
public class ExtractYearOfWeek
{
    private ExtractYearOfWeek() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
    {
        if (precision > 3) {
            timestamp = scaleEpochMicrosToMillis(timestamp);
        }

        ISOChronology chronology = ISOChronology.getInstanceUTC();
        if (session.isLegacyTimestamp()) {
            chronology = getChronology(session.getTimeZoneKey());
        }

        return chronology.weekyear().get(timestamp);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return extract(6, session, timestamp.getEpochMicros());
    }
}
