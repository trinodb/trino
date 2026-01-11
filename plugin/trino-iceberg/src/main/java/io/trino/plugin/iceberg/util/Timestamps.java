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
    private static final long MIN_NANO_EPOCH_MICROS = Long.MIN_VALUE / NANOSECONDS_PER_MICROSECOND;
    private static final long MAX_NANO_EPOCH_MICROS = Long.MAX_VALUE / NANOSECONDS_PER_MICROSECOND;
    private static final long MIN_NANO_EPOCH_MILLIS = Long.MIN_VALUE / NANOSECONDS_PER_MILLISECOND;
    private static final long MAX_NANO_EPOCH_MILLIS = Long.MAX_VALUE / NANOSECONDS_PER_MILLISECOND;

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

    public static LongTimestampWithTimeZone getTimestampTz(Block block, int position)
    {
        return (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
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
        if (epochMicros < MIN_NANO_EPOCH_MICROS || epochMicros > MAX_NANO_EPOCH_MICROS) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp value is outside the range supported by Iceberg nano timestamps");
        }
        return (epochMicros * NANOSECONDS_PER_MICROSECOND) +
                (timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);
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
        if (epochMillis < MIN_NANO_EPOCH_MILLIS || epochMillis > MAX_NANO_EPOCH_MILLIS) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp value is outside the range supported by Iceberg nano timestamps");
        }
        return (epochMillis * NANOSECONDS_PER_MILLISECOND) +
                (timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
    }
}
