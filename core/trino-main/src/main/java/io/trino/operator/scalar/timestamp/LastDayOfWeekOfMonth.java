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
package io.trino.operator.scalar.timestamp;

import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;

@Description("Last occurrence of the given day of the week in the month of the given timestamp")
@ScalarFunction("last_day_of_week_of_month")
public class LastDayOfWeekOfMonth
{
    private LastDayOfWeekOfMonth() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long lastDayOfWeekOfMonth(@SqlType("timestamp(p)") long timestamp, @SqlType(StandardTypes.INTEGER) int dayOfWeek)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp);
        DateTime dateTime = new DateTime(epochMillis, ISOChronology.getInstanceUTC());
        DateTime lastDayOfMonth = dateTime.dayOfMonth().withMaximumValue();

        while (lastDayOfMonth.getDayOfWeek() != dayOfWeek) {
            lastDayOfMonth = lastDayOfMonth.minusDays(1);
        }

        return lastDayOfMonth.getMillis();
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long lastDayOfWeekOfMonth(@SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType(StandardTypes.INTEGER) int dayOfWeek)
    {
        return lastDayOfWeekOfMonth(timestamp.getEpochMicros(), dayOfWeek);
    }
}
