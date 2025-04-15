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
package io.trino.operator.scalar.timestamptz;

import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Description("Last occurrence of the given day of the week in the month of the given timestamp")
@ScalarFunction("last_day_of_week_of_month")
public class LastDayOfWeekOfMonth
{
    private static final int MILLISECONDS_IN_DAY = 24 * 3600 * 1000;

    private LastDayOfWeekOfMonth() {}


    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfWeekOfMonth(
            @SqlType("timestamp(p) with time zone") long packedEpochMillis,
            @SqlType(StandardTypes.INTEGER) long dayOfWeek)
    {
        return lastDayOfWeekOfMonth(unpackChronology(packedEpochMillis), unpackMillisUtc(packedEpochMillis), (int) dayOfWeek);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfWeekOfMonth(
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp,
            @SqlType(StandardTypes.INTEGER) long dayOfWeek)
    {
        return lastDayOfWeekOfMonth(getChronology(getTimeZoneKey(timestamp.getTimeZoneKey())), timestamp.getEpochMillis(), (int) dayOfWeek);
    }

    private static long lastDayOfWeekOfMonth(ISOChronology chronology, long millis, int dayOfWeek)
    {
        // Get the first day of the next month
        millis = chronology.monthOfYear().roundCeiling(millis + 1);
        // Move back to the last day of the current month
        millis = chronology.getZone().convertUTCToLocal(millis) - MILLISECONDS_IN_DAY;

        // Find the last occurrence of the given day of the week
        DateTimeField dayOfWeekField = chronology.dayOfWeek();
        while (dayOfWeekField.get(millis) != dayOfWeek) {
            millis -= MILLISECONDS_IN_DAY;
        }

        return MILLISECONDS.toDays(millis);
    }
}
