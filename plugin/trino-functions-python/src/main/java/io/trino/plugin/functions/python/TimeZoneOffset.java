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
package io.trino.plugin.functions.python;

import io.trino.spi.type.TimeZoneKey;
import org.joda.time.DateTimeZone;

import static io.trino.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_MINUTE;

final class TimeZoneOffset
{
    private TimeZoneOffset() {}

    private static final DateTimeZone[] DATE_TIME_ZONES;
    private static final short[] FIXED_ZONE_OFFSET;
    private static final short VARIABLE_ZONE = Short.MAX_VALUE;

    static {
        DATE_TIME_ZONES = new DateTimeZone[MAX_TIME_ZONE_KEY + 1];
        FIXED_ZONE_OFFSET = new short[MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            short zoneKey = timeZoneKey.getKey();
            DateTimeZone zone = DateTimeZone.forID(timeZoneKey.getId());
            DATE_TIME_ZONES[zoneKey] = zone;
            if (zone.isFixed() && ((zone.getOffset(0) % MILLISECONDS_PER_MINUTE) == 0)) {
                FIXED_ZONE_OFFSET[zoneKey] = zoneOffsetMinutes(0, zone);
            }
            else {
                FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
            }
        }
    }

    public static short zoneOffsetMinutes(long millis, short zoneKey)
    {
        if (FIXED_ZONE_OFFSET[zoneKey] == VARIABLE_ZONE) {
            return zoneOffsetMinutes(millis, DATE_TIME_ZONES[zoneKey]);
        }
        return FIXED_ZONE_OFFSET[zoneKey];
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static short zoneOffsetMinutes(long millis, DateTimeZone zone)
    {
        int value = zone.getOffset(millis) / MILLISECONDS_PER_MINUTE;
        if ((short) value != value) {
            throw new ArithmeticException("integer overflow");
        }
        return (short) value;
    }
}
