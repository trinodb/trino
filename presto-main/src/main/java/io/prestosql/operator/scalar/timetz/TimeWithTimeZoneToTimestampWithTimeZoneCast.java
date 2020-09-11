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
package io.prestosql.operator.scalar.timetz;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.spi.type.LongTimestampWithTimeZone;

import java.time.LocalDate;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.prestosql.type.DateTimes.MILLISECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.rescale;
import static io.prestosql.type.DateTimes.round;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;

@ScalarOperator(CAST)
public final class TimeWithTimeZoneToTimestampWithTimeZoneCast
{
    private TimeWithTimeZoneToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long shortToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision) with time zone") long packedTime)
    {
        // source precision <= 9
        // target precision <= 3
        long picos = normalizeAndRound(targetPrecision, rescale(unpackTimeNanos(packedTime), 9, 12), unpackOffsetMinutes(packedTime));

        return packDateTimeWithZone(calculateEpochMillis(session, picos), getTimeZoneKeyForOffset(unpackOffsetMinutes(packedTime)));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision) with time zone") LongTimeWithTimeZone time)
    {
        // source precision > 9
        // target precision <= 3
        long picos = normalizeAndRound(targetPrecision, time.getPicoSeconds(), time.getOffsetMinutes());

        return packDateTimeWithZone(calculateEpochMillis(session, picos), getTimeZoneKeyForOffset(time.getOffsetMinutes()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone shortToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision) with time zone") long packedTime)
    {
        // source precision <= 9
        // target precision > 3
        long picos = normalizeAndRound(targetPrecision, rescale(unpackTimeNanos(packedTime), 9, 12), unpackOffsetMinutes(packedTime));

        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                calculateEpochMillis(session, picos),
                (int) (picos % PICOSECONDS_PER_MILLISECOND),
                getTimeZoneKeyForOffset(unpackOffsetMinutes(packedTime)));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone longToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("time(sourcePrecision) with time zone") LongTimeWithTimeZone time)
    {
        // source precision > 9
        // target precision > 3
        long picos = normalizeAndRound(targetPrecision, time.getPicoSeconds(), time.getOffsetMinutes());

        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                calculateEpochMillis(session, picos),
                (int) (picos % PICOSECONDS_PER_MILLISECOND),
                getTimeZoneKeyForOffset(time.getOffsetMinutes()));
    }

    private static long normalizeAndRound(long targetPrecision, long picos, int offsetMinutes)
    {
        picos = picos - offsetMinutes * PICOSECONDS_PER_MINUTE;
        picos = floorMod(picos, PICOSECONDS_PER_DAY);
        picos = round(picos, (int) (12 - targetPrecision));
        return picos;
    }

    private static long calculateEpochMillis(ConnectorSession session, long picos)
    {
        // TODO: consider using something more efficient than LocalDate.ofInstant() to compute epochDay
        long epochDay = LocalDate.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toEpochDay();

        long picosOfDay = picos % PICOSECONDS_PER_DAY;
        return multiplyExact(epochDay, MILLISECONDS_PER_DAY) + rescale(picosOfDay, 12, 3);
    }
}
