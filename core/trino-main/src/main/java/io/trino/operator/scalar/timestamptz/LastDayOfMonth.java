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

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Description("Last day of the month of the given timestamp")
@ScalarFunction("last_day_of_month")
public class LastDayOfMonth
{
    private static final int MILLISECONDS_IN_DAY = 24 * 3600 * 1000;

    private LastDayOfMonth() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        return lastDayOfMonth(unpackChronology(packedEpochMillis), unpackMillisUtc(packedEpochMillis));
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return lastDayOfMonth(getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), timestamp.getEpochMillis());
    }

    private static long lastDayOfMonth(ISOChronology chronology, long millis)
    {
        // Calculate point in time corresponding to midnight (00:00) of first day of next month in the given zone.
        millis = chronology.monthOfYear().roundCeiling(millis + 1);
        // Convert to UTC and take the previous day
        millis = chronology.getZone().convertUTCToLocal(millis) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }
}
