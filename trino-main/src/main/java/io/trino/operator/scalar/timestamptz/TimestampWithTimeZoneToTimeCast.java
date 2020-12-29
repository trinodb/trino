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

import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;

import java.time.Instant;
import java.time.ZoneId;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.round;

@ScalarOperator(CAST)
public final class TimestampWithTimeZoneToTimeCast
{
    private TimestampWithTimeZoneToTimeCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") long packedEpochMillis)
    {
        long epochMillis = unpackMillisUtc(packedEpochMillis);
        ZoneId zoneId = unpackZoneKey(packedEpochMillis).getZoneId();
        return convert(targetPrecision, epochMillis, 0, zoneId);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision)")
    public static long cast(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return convert(targetPrecision, timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId());
    }

    private static long convert(long targetPrecision, long epochMillis, long picosOfMilli, ZoneId zoneId)
    {
        long nanoOfDay = Instant.ofEpochMilli(epochMillis)
                .atZone(zoneId)
                .toLocalDateTime()
                .toLocalTime()
                .toNanoOfDay();

        long picoOfDay = rescale(nanoOfDay, 9, 12) + picosOfMilli;
        return round(picoOfDay, (int) (MAX_PRECISION - targetPrecision)) % PICOSECONDS_PER_DAY;
    }
}
