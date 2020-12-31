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

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.scalar.DateTimeFunctions.dateFormat;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.type.DateTimes.roundToEpochMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;

@ScalarFunction
@Description("Formats the given timestamp by the given format")
public class DateFormat
{
    private DateFormat() {}

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") long packedEpochMillis, @SqlType("varchar(x)") Slice formatString)
    {
        return dateFormat(unpackChronology(packedEpochMillis), session.getLocale(), unpackMillisUtc(packedEpochMillis), formatString);
    }

    @LiteralParameters({"x", "p"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice format(ConnectorSession session, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return dateFormat(getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), session.getLocale(), roundToEpochMillis(timestamp), formatString);
    }
}
