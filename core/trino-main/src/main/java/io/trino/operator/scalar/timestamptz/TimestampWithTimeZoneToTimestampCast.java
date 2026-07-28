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

import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundExact;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static io.trino.type.DateTimes.toEpochMicros;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static java.lang.Math.incrementExact;

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

        // Round in the (millis, picos of milli) domain before converting to micros, so that a value
        // whose rounded result is representable does not overflow the conversion
        if (targetPrecision < 3) {
            // The positive sub-millisecond fraction cannot affect rounding at a grain of 10ms or more
            epochMillis = roundEpochMillisExact(epochMillis, (int) (3 - targetPrecision));
            picosOfMilli = 0;
        }
        else {
            picosOfMilli = (int) round(picosOfMilli, (int) (12 - targetPrecision));
            if (picosOfMilli == PICOSECONDS_PER_MILLISECOND) {
                epochMillis = incrementEpochMillisExact(epochMillis);
                picosOfMilli = 0;
            }
        }

        // Convert to micros
        return toEpochMicrosExact(epochMillis, picosOfMilli);
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

        // Round in the (millis, picos of milli) domain before converting to micros, so that a value
        // whose rounded result is representable does not overflow the conversion
        picosOfMilli = (int) round(picosOfMilli, (int) (12 - targetPrecision));
        if (picosOfMilli == PICOSECONDS_PER_MILLISECOND) {
            epochMillis = incrementEpochMillisExact(epochMillis);
            picosOfMilli = 0;
        }

        // Convert to micros
        long epochMicros = toEpochMicrosExact(epochMillis, picosOfMilli);
        return new LongTimestamp(epochMicros, picosOfMilli % PICOSECONDS_PER_MICROSECOND);
    }

    private static long roundEpochMillisExact(long epochMillis, int magnitude)
    {
        try {
            return roundExact(epochMillis, magnitude);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Out of range for timestamp: " + epochMillis + " milliseconds", e);
        }
    }

    private static long incrementEpochMillisExact(long epochMillis)
    {
        try {
            return incrementExact(epochMillis);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Out of range for timestamp: " + epochMillis + " milliseconds", e);
        }
    }

    private static long toEpochMicrosExact(long epochMillis, int picosOfMilli)
    {
        try {
            return toEpochMicros(epochMillis, picosOfMilli);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Out of range for timestamp: " + epochMillis + " milliseconds", e);
        }
    }
}
