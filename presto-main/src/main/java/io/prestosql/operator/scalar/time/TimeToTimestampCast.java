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
package io.prestosql.operator.scalar.time;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.TimeType;

import java.time.LocalDate;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.DateTimes.MICROSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.SECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.rescale;
import static io.prestosql.type.DateTimes.round;
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
        return cast(sourcePrecision, targetPrecision, session, time);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp castToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision)") long time)
    {
        long epochMicros = cast(sourcePrecision, targetPrecision, session, time);
        return new LongTimestamp(epochMicros, (int) (time % PICOSECONDS_PER_MICROSECOND));
    }

    private static long cast(long sourcePrecision, long targetPrecision, ConnectorSession session, long time)
    {
        // TODO: consider using something more efficient than LocalDate.ofInstant() to compute epochDay
        long epochDay = LocalDate.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toEpochDay();

        long epochSecond = multiplyExact(epochDay, SECONDS_PER_DAY) + time / PICOSECONDS_PER_SECOND;
        long picoFraction = time % PICOSECONDS_PER_SECOND;
        if (sourcePrecision > targetPrecision) {
            picoFraction = round(picoFraction, (int) (TimeType.MAX_PRECISION - targetPrecision));
        }

        long microFraction = rescale(picoFraction, TimeType.MAX_PRECISION, 6);
        return multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + microFraction;
    }
}
