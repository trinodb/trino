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

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_PRECISION;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.round;
import static io.trino.type.DateTimes.roundToNearest;

@ScalarOperator(CAST)
public final class TimestampWithTimeZoneToTimestampWithTimeZoneCast
{
    private TimestampWithTimeZoneToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long shortToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") long packedEpochMillis)
    {
        if (sourcePrecision <= targetPrecision) {
            return packedEpochMillis;
        }

        long epochMillis = unpackMillisUtc(packedEpochMillis);
        epochMillis = round(epochMillis, (int) (MAX_SHORT_PRECISION - targetPrecision));
        return packDateTimeWithZone(epochMillis, unpackZoneKey(packedEpochMillis));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone shortToLong(@SqlType("timestamp(sourcePrecision) with time zone") long packedEpochMillis)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(unpackMillisUtc(packedEpochMillis), 0, unpackZoneKey(packedEpochMillis));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") LongTimestampWithTimeZone timestamp)
    {
        long epochMillis = timestamp.getEpochMillis();

        if (targetPrecision < MAX_SHORT_PRECISION) {
            epochMillis = round(epochMillis, (int) (MAX_SHORT_PRECISION - targetPrecision));
        }
        else if (roundToNearest(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MILLISECOND) == PICOSECONDS_PER_MILLISECOND) {
            epochMillis++;
        }

        return packDateTimeWithZone(epochMillis, timestamp.getTimeZoneKey());
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone longToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision) with time zone") LongTimestampWithTimeZone timestamp)
    {
        long epochMillis = timestamp.getEpochMillis();
        int picosOfMilli = (int) round(timestamp.getPicosOfMilli(), (int) (MAX_PRECISION - targetPrecision));
        if (picosOfMilli == PICOSECONDS_PER_MILLISECOND) {
            epochMillis++;
            picosOfMilli = 0;
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                epochMillis,
                picosOfMilli,
                timestamp.getTimeZoneKey());
    }
}
