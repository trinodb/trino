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

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.type.DateTimes.scaleEpochMicrosToMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Description("Last day of the month of the given timestamp")
@ScalarFunction("last_day_of_month")
public class LastDayOfMonth
{
    private static final int MILLISECONDS_IN_DAY = 24 * 3600 * 1000;

    private LastDayOfMonth() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p)") long timestamp)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp);
        long millis = ISOChronology.getInstanceUTC().monthOfYear().roundCeiling(epochMillis + 1) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return lastDayOfMonth(timestamp.getEpochMicros());
    }
}
