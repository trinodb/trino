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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.round;
import static io.trino.type.DateTimes.roundToNearest;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static io.trino.type.DateTimes.toEpochMicros;
import static io.trino.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimestampWithTimeZoneToTimestampCast
{
    private TimestampWithTimeZoneToTimestampCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static long shortToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") long timestamp)
    {
        long epochMillis = getChronology(unpackZoneKey(timestamp))
                .getZone()
                .convertUTCToLocal(unpackMillisUtc(timestamp));

        return round(scaleEpochMillisToMicros(epochMillis), (int) (6 - targetPrecision));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") LongTimestampWithTimeZone timestamp)
    {
        // Extract
        long epochMillis = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()))
                .getZone()
                .convertUTCToLocal(timestamp.getEpochMillis());
        int picosOfMilli = timestamp.getPicosOfMilli();

        // Convert to micros
        long epochMicros = toEpochMicros(epochMillis, picosOfMilli);
        int picosOfMicro = picosOfMilli % PICOSECONDS_PER_MICROSECOND;

        // Round
        if (targetPrecision < 6) {
            epochMicros = round(epochMicros, (int) (6 - targetPrecision));
        }
        else if (roundToNearest(picosOfMicro, PICOSECONDS_PER_MICROSECOND) == PICOSECONDS_PER_MICROSECOND) {
            epochMicros++;
        }

        return epochMicros;
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp shortToLong(@SqlType("timestamp(sourcePrecision) with time zone") long timestamp)
    {
        long epochMillis = getChronology(unpackZoneKey(timestamp))
                .getZone()
                .convertUTCToLocal(unpackMillisUtc(timestamp));

        return new LongTimestamp(scaleEpochMillisToMicros(epochMillis), 0);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp longToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") LongTimestampWithTimeZone timestamp)
    {
        // Extract
        long epochMillis = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()))
                .getZone()
                .convertUTCToLocal(timestamp.getEpochMillis());
        int picosOfMilli = timestamp.getPicosOfMilli();

        // Convert to micros
        long epochMicros = toEpochMicros(epochMillis, picosOfMilli);
        int picosOfMicro = picosOfMilli % PICOSECONDS_PER_MICROSECOND;

        // Round
        picosOfMicro = (int) round(picosOfMicro, (int) (12 - targetPrecision));
        if (picosOfMicro == PICOSECONDS_PER_MICROSECOND) {
            epochMicros++;
            picosOfMicro = 0;
        }

        return new LongTimestamp(epochMicros, picosOfMicro);
    }
}
