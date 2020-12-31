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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.type.DateTimes.MICROSECONDS_PER_DAY;
import static io.trino.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.getOffsetMinutes;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.round;
import static java.lang.Math.floorMod;

@ScalarOperator(CAST)
public final class TimestampToTimeWithTimeZoneCast
{
    private TimestampToTimeWithTimeZoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static long shortToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        // source precision <= 6
        // target precision <= 9
        long nanos = floorMod(timestamp, MICROSECONDS_PER_DAY) * NANOSECONDS_PER_MICROSECOND;

        nanos = round(nanos, (int) (9 - targetPrecision)) % NANOSECONDS_PER_DAY;

        return packTimeWithTimeZone(nanos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        // source precision > 6
        // target precision <= 9
        long picos = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro();
        picos = round(picos, (int) (12 - targetPrecision));

        long nanos = rescale(picos, 12, 9) % NANOSECONDS_PER_DAY;
        return packTimeWithTimeZone(nanos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static LongTimeWithTimeZone shortToLong(
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        // source precision <= 6
        // target precision > 9
        long picos = floorMod(timestamp, MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND;
        return new LongTimeWithTimeZone(picos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("time(targetPrecision) with time zone")
    public static LongTimeWithTimeZone longToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        // source precision > 6
        // target precision > 9
        long picos = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_DAY) * PICOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro();

        picos = round(picos, (int) (12 - targetPrecision)) % PICOSECONDS_PER_DAY;

        return new LongTimeWithTimeZone(picos, getOffsetMinutes(session.getStart(), session.getTimeZoneKey()));
    }
}
