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
package io.trino.plugin.iceberg.util;

import com.google.common.math.LongMath;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;

public final class Timestamps
{
    // Nano timestamp range: Long.MIN_VALUE/MAX_VALUE nanos converted to micros/millis
    private static final long MIN_NANO_EPOCH_MICROS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MIN_NANO_OF_MICRO = toIntExact(floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND));
    private static final long MAX_NANO_EPOCH_MICROS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MAX_NANO_OF_MICRO = toIntExact(floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND));
    private static final long MIN_NANO_EPOCH_MILLIS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MIN_NANO_OF_MILLI = toIntExact(floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND));
    private static final long MAX_NANO_EPOCH_MILLIS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MAX_NANO_OF_MILLI = toIntExact(floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND));

    private Timestamps() {}

    public static long timestampTzToMicros(LongTimestampWithTimeZone timestamp)
    {
        return (timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND) +
                LongMath.divide(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MICROSECOND, UNNECESSARY);
    }

    public static LongTimestampWithTimeZone timestampTzFromMicros(long epochMicros)
    {
        long epochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
        int picosOfMillis = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMillis, UTC_KEY);
    }

    public static LongTimestampWithTimeZone getTimestampTzMicros(Block block, int position)
    {
        return (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
    }

    public static LongTimestampWithTimeZone getTimestampTzNanos(Block block, int position)
    {
        return (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, position);
    }

    public static int getNanosOfMicro(LongTimestamp timestamp)
    {
        return toIntExact(LongMath.divide(timestamp.getPicosOfMicro(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY));
    }

    public static int getNanosOfMilli(LongTimestampWithTimeZone timestamp)
    {
        return toIntExact(LongMath.divide(timestamp.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY));
    }

    public static int compareTimestampNanosToRange(LongTimestamp timestamp)
    {
        return compareToRange(timestamp.getEpochMicros(), getNanosOfMicro(timestamp), MIN_NANO_EPOCH_MICROS, MIN_NANO_OF_MICRO, MAX_NANO_EPOCH_MICROS, MAX_NANO_OF_MICRO);
    }

    public static int compareTimestampTzNanosToRange(LongTimestampWithTimeZone timestamp)
    {
        return compareToRange(timestamp.getEpochMillis(), getNanosOfMilli(timestamp), MIN_NANO_EPOCH_MILLIS, MIN_NANO_OF_MILLI, MAX_NANO_EPOCH_MILLIS, MAX_NANO_OF_MILLI);
    }

    // Nano timestamp conversions (local timestamp without timezone)
    public static LongTimestamp timestampFromNanos(long epochNanos)
    {
        long epochMicros = floorDiv(epochNanos, NANOSECONDS_PER_MICROSECOND);
        int picosOfMicro = toIntExact(floorMod(epochNanos, NANOSECONDS_PER_MICROSECOND)) * PICOSECONDS_PER_NANOSECOND;
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    public static long timestampToNanos(LongTimestamp timestamp)
    {
        long epochMicros = timestamp.getEpochMicros();
        int nanosOfMicro = getNanosOfMicro(timestamp);
        if (isOutOfRange(epochMicros, nanosOfMicro, MIN_NANO_EPOCH_MICROS, MIN_NANO_OF_MICRO, MAX_NANO_EPOCH_MICROS, MAX_NANO_OF_MICRO)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp value is outside the range supported by Iceberg nano timestamps");
        }
        if (epochMicros == MIN_NANO_EPOCH_MICROS) {
            return Long.MIN_VALUE + (nanosOfMicro - MIN_NANO_OF_MICRO);
        }
        return (epochMicros * NANOSECONDS_PER_MICROSECOND) + nanosOfMicro;
    }

    // Nano timestamp with timezone conversions (instant, stored as UTC)
    public static LongTimestampWithTimeZone timestampTzFromNanos(long epochNanos)
    {
        long epochMillis = floorDiv(epochNanos, NANOSECONDS_PER_MILLISECOND);
        int picosOfMilli = toIntExact(floorMod(epochNanos, NANOSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_NANOSECOND;
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY);
    }

    public static long timestampTzToNanos(LongTimestampWithTimeZone timestamp)
    {
        long epochMillis = timestamp.getEpochMillis();
        int nanosOfMilli = getNanosOfMilli(timestamp);
        if (isOutOfRange(epochMillis, nanosOfMilli, MIN_NANO_EPOCH_MILLIS, MIN_NANO_OF_MILLI, MAX_NANO_EPOCH_MILLIS, MAX_NANO_OF_MILLI)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp value is outside the range supported by Iceberg nano timestamps");
        }
        if (epochMillis == MIN_NANO_EPOCH_MILLIS) {
            return Long.MIN_VALUE + (nanosOfMilli - MIN_NANO_OF_MILLI);
        }
        return (epochMillis * NANOSECONDS_PER_MILLISECOND) + nanosOfMilli;
    }

    private static boolean isOutOfRange(long epoch, int nanosFraction, long minEpoch, int minNanosFraction, long maxEpoch, int maxNanosFraction)
    {
        return compareToRange(epoch, nanosFraction, minEpoch, minNanosFraction, maxEpoch, maxNanosFraction) != 0;
    }

    private static int compareToRange(long epoch, int nanosFraction, long minEpoch, int minNanosFraction, long maxEpoch, int maxNanosFraction)
    {
        return epoch < minEpoch ||
                (epoch == minEpoch && nanosFraction < minNanosFraction) ? -1 :
                epoch > maxEpoch ||
                        (epoch == maxEpoch && nanosFraction > maxNanosFraction) ? 1 :
                0;
    }
}
