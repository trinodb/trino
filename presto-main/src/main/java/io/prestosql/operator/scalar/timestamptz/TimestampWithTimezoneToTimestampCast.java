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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.LongTimestampWithTimeZone;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.roundToNearest;
import static io.prestosql.type.DateTimes.scaleEpochMillisToMicros;
import static io.prestosql.type.DateTimes.toEpochMicros;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimestampWithTimezoneToTimestampCast
{
    private TimestampWithTimezoneToTimestampCast() {}

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
        long epochMillis = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()))
                .getZone()
                .convertUTCToLocal(timestamp.getEpochMillis());
        int picosOfMilli = timestamp.getPicosOfMilli();

        long epochMicros = toEpochMicros(epochMillis, picosOfMilli);
        if (targetPrecision < 6) {
            return round(epochMicros, (int) (6 - targetPrecision));
        }

        if (roundToNearest(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MILLISECOND) == PICOSECONDS_PER_MILLISECOND) {
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
        long epochMillis = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()))
                .getZone()
                .convertUTCToLocal(timestamp.getEpochMillis());

        long epochMicros;
        int picosOfMicro;
        if (targetPrecision <= 6) {
            epochMicros = toEpochMicros(epochMillis, timestamp.getPicosOfMilli());
            epochMicros = round(epochMicros, (int) (6 - targetPrecision));
            picosOfMicro = 0;
        }
        else {
            int picosOfMilli = timestamp.getPicosOfMilli();
            epochMicros = toEpochMicros(epochMillis, picosOfMilli);
            picosOfMicro = (int) round(picosOfMilli % PICOSECONDS_PER_MICROSECOND, (int) (12 - targetPrecision));
        }

        return new LongTimestamp(epochMicros, picosOfMicro);
    }
}
