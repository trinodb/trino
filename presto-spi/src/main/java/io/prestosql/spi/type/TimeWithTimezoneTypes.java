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
package io.prestosql.spi.type;

import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MINUTE;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static java.lang.Math.floorMod;

public final class TimeWithTimezoneTypes
{
    private TimeWithTimezoneTypes() {}

    public static long hashLongTimeWithTimeZone(LongTimeWithTimeZone value)
    {
        return hashLongTimeWithTimeZone(value.getPicoSeconds(), value.getOffsetMinutes());
    }

    public static long hashShortTimeWithTimeZone(long packedTime)
    {
        return AbstractLongType.hash(normalize(packedTime));
    }

    static long hashLongTimeWithTimeZone(long picos, int offsetMinutes)
    {
        return AbstractLongType.hash(normalizePicos(picos, offsetMinutes));
    }

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     */
    static long normalizePicos(long picos, int offsetMinutes)
    {
        return floorMod(picos - offsetMinutes * PICOSECONDS_PER_MINUTE, PICOSECONDS_PER_DAY);
    }

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     */
    static long normalizeNanos(long nanos, int offsetMinutes)
    {
        return floorMod(nanos - offsetMinutes * NANOSECONDS_PER_MINUTE, NANOSECONDS_PER_DAY);
    }

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     *
     * @return the time in nanoseconds
     */
    private static long normalize(long packedTime)
    {
        return normalizeNanos(unpackTimeNanos(packedTime), unpackOffsetMinutes(packedTime));
    }

    /**
     * Normalize to offset +00:00. The calculation is done modulo 24h
     *
     * @return the time in picoseconds
     */
    static long normalize(LongTimeWithTimeZone time)
    {
        return floorMod(time.getPicoSeconds() - time.getOffsetMinutes() * PICOSECONDS_PER_MINUTE, PICOSECONDS_PER_DAY);
    }
}
