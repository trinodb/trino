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

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;

@Description("Last day of the month of the given timestamp")
@ScalarFunction("last_day_of_month")
public final class LastDayOfMonth
{
    private LastDayOfMonth() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        return lastDayOfMonth(unpackZoneKey(packedEpochMillis).getZoneId(), unpackMillisUtc(packedEpochMillis));
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonth(@SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return lastDayOfMonth(getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId(), timestamp.getEpochMillis());
    }

    private static long lastDayOfMonth(ZoneId zoneId, long millis)
    {
        return Instant.ofEpochMilli(millis).atZone(zoneId).toLocalDate()
                .with(TemporalAdjusters.lastDayOfMonth())
                .toEpochDay();
    }
}
