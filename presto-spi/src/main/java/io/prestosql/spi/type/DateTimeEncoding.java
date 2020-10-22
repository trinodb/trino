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

import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static java.util.Objects.requireNonNull;

public final class DateTimeEncoding
{
    private DateTimeEncoding() {}

    private static final int TIME_ZONE_MASK = 0xFFF;
    private static final int MILLIS_SHIFT = 12;

    private static long pack(long millisUtc, short timeZoneKey)
    {
        if (millisUtc << MILLIS_SHIFT >> MILLIS_SHIFT != millisUtc) {
            throw new IllegalArgumentException("Millis overflow: " + millisUtc);
        }

        return (millisUtc << MILLIS_SHIFT) | (timeZoneKey & TIME_ZONE_MASK);
    }

    /**
     * @throws TimeZoneNotSupportedException when {@code zoneId} does not identity a time zone
     */
    public static long packDateTimeWithZone(long millisUtc, String zoneId)
    {
        return packDateTimeWithZone(millisUtc, getTimeZoneKey(zoneId));
    }

    public static long packDateTimeWithZone(long millisUtc, int offsetMinutes)
    {
        return packDateTimeWithZone(millisUtc, getTimeZoneKeyForOffset(offsetMinutes));
    }

    public static long packDateTimeWithZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        return pack(millisUtc, timeZoneKey.getKey());
    }

    public static long packDateTimeWithZone(long millisUtc, short timeZoneKey)
    {
        return pack(millisUtc, timeZoneKey);
    }

    public static long unpackMillisUtc(long dateTimeWithTimeZone)
    {
        return dateTimeWithTimeZone >> MILLIS_SHIFT;
    }

    public static TimeZoneKey unpackZoneKey(long dateTimeWithTimeZone)
    {
        return getTimeZoneKey((short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }

    public static long updateMillisUtc(long newMillsUtc, long dateTimeWithTimeZone)
    {
        return pack(newMillsUtc, (short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }

    public static long packTimeWithTimeZone(long nanos, int offsetMinutes)
    {
        // offset is encoded as a 2s complement 11-bit number
        return (nanos << 11) | (offsetMinutes & 0b111_1111_1111);
    }

    public static long unpackTimeNanos(long packedTimeWithTimeZone)
    {
        return packedTimeWithTimeZone >>> 11;
    }

    public static int unpackOffsetMinutes(long packedTimeWithTimeZone)
    {
        int unpacked = (int) (packedTimeWithTimeZone & 0b11_1111_1111);
        if ((packedTimeWithTimeZone & 0b100_0000_0000) != 0) {
            // extend sign up to int
            unpacked |= 0b1111_1111_1111_1111_1111_1100_0000_0000;
        }
        return unpacked;
    }
}
