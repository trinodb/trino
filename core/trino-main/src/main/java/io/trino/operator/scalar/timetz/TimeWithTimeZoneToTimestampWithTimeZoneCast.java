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
package io.trino.operator.scalar.timetz;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestampWithTimeZone;

import java.time.LocalDate;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.type.DateTimes.MILLISECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.round;
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
        int offsetMinutes = unpackOffsetMinutes(packedTime);
        long picos = normalizeAndRound(targetPrecision, rescale(unpackTimeNanos(packedTime), 9, 12), offsetMinutes);

        return packDateTimeWithZone(calculateEpochMillis(session, picos), getTimeZoneKeyForOffset(offsetMinutes));
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
        long picos = normalizeAndRound(targetPrecision, time.getPicoseconds(), time.getOffsetMinutes());

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
        int offsetMinutes = unpackOffsetMinutes(packedTime);
        long picos = normalizeAndRound(targetPrecision, rescale(unpackTimeNanos(packedTime), 9, 12), offsetMinutes);

        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                calculateEpochMillis(session, picos),
                (int) (picos % PICOSECONDS_PER_MILLISECOND),
                getTimeZoneKeyForOffset(offsetMinutes));
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
        long picos = normalizeAndRound(targetPrecision, time.getPicoseconds(), time.getOffsetMinutes());

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
