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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.spi.type.LongTimestamp;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.type.DateTimes.MICROSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.getOffsetMinutes;
import static io.prestosql.type.DateTimes.rescale;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.scaleEpochMillisToMicros;
import static java.lang.Math.floorMod;

@ScalarOperator(CAST)
public final class TimestampToTimeWithTimezoneCast
{
    private TimestampToTimeWithTimezoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static long shortToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        // source precision <= 6
        // target precision <= 9
        long nanos = getMicros(sourcePrecision, timestamp) * NANOSECONDS_PER_MICROSECOND;

        nanos = round(nanos, (int) (9 - targetPrecision)) % NANOSECONDS_PER_DAY;

        return packTimeWithTimeZone(nanos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static long longToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        // source precision > 6
        // target precision <= 9
        long picos = getMicros(sourcePrecision, timestamp.getEpochMicros()) * PICOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro();
        picos = round(picos, (int) (12 - targetPrecision));

        long nanos = rescale(picos, 12, 9) % NANOSECONDS_PER_DAY;
        return packTimeWithTimeZone(nanos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static LongTimeWithTimeZone shortToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        // source precision <= 6
        // target precision > 9
        long picos = getMicros(sourcePrecision, timestamp) * PICOSECONDS_PER_MICROSECOND;
        return new LongTimeWithTimeZone(picos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static LongTimeWithTimeZone longToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        // source precision > 6
        // target precision > 9
        long picos = getMicros(sourcePrecision, timestamp.getEpochMicros()) * PICOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro();

        picos = round(picos, (int) (12 - targetPrecision)) % PICOSECONDS_PER_DAY;

        return new LongTimeWithTimeZone(picos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    private static long getMicros(long sourcePrecision, long timestamp)
    {
        long epochMicros = timestamp;
        if (sourcePrecision <= 3) {
            epochMicros = scaleEpochMillisToMicros(timestamp);
        }

        return floorMod(epochMicros, MICROSECONDS_PER_DAY);
    }
}
