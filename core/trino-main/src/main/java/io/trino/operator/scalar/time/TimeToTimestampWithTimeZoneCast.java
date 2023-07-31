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
package io.trino.operator.scalar.time;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;

import java.time.LocalDate;
import java.time.ZoneId;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.type.DateTimes.MILLISECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.SECONDS_PER_DAY;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.round;
import static java.lang.Math.multiplyExact;

@ScalarOperator(CAST)
public final class TimeToTimestampWithTimeZoneCast
{
    private TimeToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long castToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision)") long time)
    {
        ZoneId zoneId = session.getTimeZoneKey().getZoneId();
        long epochSeconds = getEpochSeconds(session, time, zoneId);
        long picoFraction = getPicoFraction(sourcePrecision, targetPrecision, time);
        long epochMillis = computeEpochMillis(session, zoneId, epochSeconds, picoFraction);

        return packDateTimeWithZone(epochMillis, session.getTimeZoneKey());
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone castToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision)") long time)
    {
        ZoneId zoneId = session.getTimeZoneKey().getZoneId();
        long epochSeconds = getEpochSeconds(session, time, zoneId);
        long picoFraction = getPicoFraction(sourcePrecision, targetPrecision, time);
        long epochMillis = computeEpochMillis(session, zoneId, epochSeconds, picoFraction);

        int picosOfMilli = (int) (picoFraction % PICOSECONDS_PER_MILLISECOND);
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, session.getTimeZoneKey().getKey());
    }

    private static long getEpochSeconds(ConnectorSession session, @SqlType("time(sourcePrecision)") long time, ZoneId zoneId)
    {
        long epochDay = LocalDate.ofInstant(session.getStart(), zoneId)
                .toEpochDay();

        return multiplyExact(epochDay, SECONDS_PER_DAY) + time / PICOSECONDS_PER_SECOND;
    }

    private static long getPicoFraction(@LiteralParameter("sourcePrecision") long sourcePrecision, @LiteralParameter("targetPrecision") long targetPrecision, @SqlType("time(sourcePrecision)") long time)
    {
        long picoFraction = time % PICOSECONDS_PER_SECOND;
        if (sourcePrecision > targetPrecision) {
            picoFraction = round(picoFraction, (int) (TimeType.MAX_PRECISION - targetPrecision));
        }
        return picoFraction;
    }

    private static long computeEpochMillis(ConnectorSession session, ZoneId zoneId, long epochSeconds, long picoFraction)
    {
        long milliFraction = rescale(picoFraction, TimeType.MAX_PRECISION, 3);
        long epochMillis = multiplyExact(epochSeconds, MILLISECONDS_PER_SECOND) + milliFraction;
        epochMillis -= zoneId.getRules().getOffset(session.getStart()).getTotalSeconds() * (long) MILLISECONDS_PER_SECOND;
        return epochMillis;
    }
}
