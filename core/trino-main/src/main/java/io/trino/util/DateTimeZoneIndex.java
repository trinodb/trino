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
package io.trino.util;

import io.airlift.log.Logger;
import io.trino.plugin.base.type.TimeZoneConversions;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static java.lang.Math.addExact;
import static java.lang.Math.subtractExact;

public final class DateTimeZoneIndex
{
    private static final Logger log = Logger.get(DateTimeZoneIndex.class);
    private static final int VARIABLE_ZONE = Integer.MAX_VALUE;
    private static final ZoneId[] ZONE_IDS = new ZoneId[MAX_TIME_ZONE_KEY + 1];
    private static final int[] FIXED_ZONE_OFFSET = new int[MAX_TIME_ZONE_KEY + 1];

    private DateTimeZoneIndex() {}

    static {
        try {
            for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
                short zoneKey = timeZoneKey.getKey();
                ZoneId zoneId = timeZoneKey.getZoneId();
                ZONE_IDS[zoneKey] = zoneId;
                ZoneRules rules = zoneId.getRules();
                int offsetSeconds = rules.getOffset(Instant.EPOCH).getTotalSeconds();
                if (rules.isFixedOffset() && offsetSeconds % 60 == 0) {
                    FIXED_ZONE_OFFSET[zoneKey] = offsetSeconds / 60;
                }
                else {
                    FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
                }
            }
        }
        catch (Exception e) {
            // Log static initializer failure to ensure it is visible.
            log.error(e, "DateTimeZoneIndex initialization failed");
            throw e;
        }
    }

    public static ZoneId getZoneId(TimeZoneKey zoneKey)
    {
        return ZONE_IDS[zoneKey.getKey()];
    }

    public static ZoneId unpackZoneId(long timestampWithTimeZone)
    {
        return getZoneId(unpackZoneKey(timestampWithTimeZone));
    }

    public static int getOffsetMillis(long epochMillis, ZoneId zoneId)
    {
        return zoneId.getRules().getOffset(Instant.ofEpochMilli(epochMillis)).getTotalSeconds() * 1000;
    }

    public static long convertUtcToLocal(long epochMillis, ZoneId zoneId)
    {
        return addExact(epochMillis, getOffsetMillis(epochMillis, zoneId));
    }

    public static long convertLocalToUtc(long localMillis, ZoneId zoneId)
    {
        // Preserve the existing offset-estimation policy, including the choice of
        // offset in historical overlaps where java.time's default can differ.
        int initialOffset = getOffsetMillis(localMillis, zoneId);
        long initialInstant = subtractExact(localMillis, initialOffset);
        int offset = getOffsetMillis(initialInstant, zoneId);
        if (initialOffset != offset && initialOffset < 0) {
            if (!nextTransition(initialInstant, zoneId).equals(nextTransition(subtractExact(localMillis, offset), zoneId))) {
                offset = initialOffset;
            }
        }
        return subtractExact(localMillis, offset);
    }

    public static int getOffsetFromLocalMillis(long localMillis, ZoneId zoneId)
    {
        return TimeZoneConversions.getOffsetFromLocalMillis(zoneId, localMillis);
    }

    public static long convertLocalToUtc(long localMillis, ZoneId zoneId, long originalInstant)
    {
        int originalOffset = getOffsetMillis(originalInstant, zoneId);
        long candidate = subtractExact(localMillis, originalOffset);
        if (getOffsetMillis(candidate, zoneId) == originalOffset) {
            return candidate;
        }
        return convertLocalToUtc(localMillis, zoneId);
    }

    private static Instant nextTransition(long epochMillis, ZoneId zoneId)
    {
        ZoneOffsetTransition transition = zoneId.getRules().nextTransition(Instant.ofEpochMilli(epochMillis));
        if (transition == null || transition.getInstant().isAfter(Instant.ofEpochMilli(Long.MAX_VALUE))) {
            return Instant.MAX;
        }
        return transition.getInstant();
    }

    public static int extractZoneOffsetMinutes(long dateTimeWithTimeZone)
    {
        return extractZoneOffsetMinutes(unpackMillisUtc(dateTimeWithTimeZone), unpackZoneKey(dateTimeWithTimeZone).getKey());
    }

    public static int extractZoneOffsetMinutes(long epochMillis, short zoneKey)
    {
        if (FIXED_ZONE_OFFSET[zoneKey] == VARIABLE_ZONE) {
            return getOffsetMillis(epochMillis, ZONE_IDS[zoneKey]) / 60_000;
        }
        return FIXED_ZONE_OFFSET[zoneKey];
    }
}
