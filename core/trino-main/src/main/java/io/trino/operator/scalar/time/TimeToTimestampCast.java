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
import io.trino.spi.type.LongTimestamp;

import java.time.LocalDate;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.type.DateTimes.MICROSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.SECONDS_PER_DAY;
import static io.trino.type.DateTimes.round;
import static java.lang.Math.multiplyExact;

@ScalarOperator(CAST)
public final class TimeToTimestampCast
{
    private TimeToTimestampCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static long castToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision)") long time)
    {
        long epochSeconds = getEpochSeconds(session, time);
        long picoFraction = getPicoFraction(sourcePrecision, targetPrecision, time);
        return computeEpochMicros(epochSeconds, picoFraction);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp castToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision)") long time)
    {
        long epochSeconds = getEpochSeconds(session, time);
        long picoFraction = getPicoFraction(sourcePrecision, targetPrecision, time);
        long epochMicros = computeEpochMicros(epochSeconds, picoFraction);

        int picosOfMicro = (int) (picoFraction % PICOSECONDS_PER_MICROSECOND);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private static long getEpochSeconds(ConnectorSession session, long time)
    {
        // TODO: consider using something more efficient than LocalDate.ofInstant() to compute epochDay
        long epochDay = LocalDate.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toEpochDay();

        return multiplyExact(epochDay, SECONDS_PER_DAY) + time / PICOSECONDS_PER_SECOND;
    }

    private static long getPicoFraction(long sourcePrecision, long targetPrecision, long time)
    {
        long picoFraction = time % PICOSECONDS_PER_SECOND;
        if (sourcePrecision > targetPrecision) {
            picoFraction = round(picoFraction, (int) (12 - targetPrecision));
        }

        return picoFraction;
    }

    private static long computeEpochMicros(long epochSeconds, long picoFraction)
    {
        // picoFraction is already rounded to whole micros
        long microFraction = picoFraction / PICOSECONDS_PER_MICROSECOND;
        return multiplyExact(epochSeconds, MICROSECONDS_PER_SECOND) + microFraction;
    }
}
