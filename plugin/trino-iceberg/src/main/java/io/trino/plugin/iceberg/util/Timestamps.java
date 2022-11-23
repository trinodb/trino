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
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.math.RoundingMode.UNNECESSARY;

public final class Timestamps
{
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
}
