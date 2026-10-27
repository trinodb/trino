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
package io.trino.plugin.base.type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;

import static java.lang.Math.addExact;
import static java.lang.Math.subtractExact;
import static java.time.ZoneOffset.UTC;

public final class TimeZoneConversions
{
    private TimeZoneConversions() {}

    public static long convertUtcToLocal(ZoneId timeZone, long epochMillis)
    {
        return addExact(epochMillis, offsetMillis(timeZone, epochMillis));
    }

    public static long convertLocalToUtc(ZoneId timeZone, long localEpochMillis)
    {
        return convertLocalToUtc(timeZone, localEpochMillis, false);
    }

    public static long convertLocalToUtc(ZoneId timeZone, long localEpochMillis, boolean strict)
    {
        Instant localInstant = Instant.ofEpochMilli(localEpochMillis);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(localInstant, UTC);
        if (strict && timeZone.getRules().getValidOffsets(localDateTime).isEmpty()) {
            throw new IllegalArgumentException("Illegal instant due to time zone offset transition: " + localDateTime + " (" + timeZone + ")");
        }
        // Preserve the offset estimated from the local instant when the clock repeats.
        // This selects the later offset in eastern zones and the earlier offset in western zones.
        return ZonedDateTime.ofLocal(localDateTime, timeZone, timeZone.getRules().getOffset(localInstant))
                .toInstant()
                .toEpochMilli();
    }

    public static int getOffsetFromLocalMillis(ZoneId timeZone, long localEpochMillis)
    {
        int initialOffset = offsetMillis(timeZone, localEpochMillis);
        long candidate = subtractExact(localEpochMillis, initialOffset);
        int adjustedOffset = offsetMillis(timeZone, candidate);
        if (initialOffset != adjustedOffset) {
            if (initialOffset < adjustedOffset && !nextTransition(timeZone, candidate).equals(nextTransition(timeZone, subtractExact(localEpochMillis, adjustedOffset)))) {
                return initialOffset;
            }
        }
        else if (initialOffset >= 0) {
            ZoneOffsetTransition previous = timeZone.getRules().previousTransition(Instant.ofEpochMilli(candidate).plusNanos(1));
            if (previous != null && previous.getInstant().isAfter(Instant.ofEpochMilli(Long.MIN_VALUE))) {
                long beforeTransition = previous.getInstant().toEpochMilli() - 1;
                int previousOffset = offsetMillis(timeZone, beforeTransition);
                if (candidate - beforeTransition <= previousOffset - initialOffset) {
                    return previousOffset;
                }
            }
        }
        return adjustedOffset;
    }

    private static int offsetMillis(ZoneId timeZone, long epochMillis)
    {
        return timeZone.getRules().getOffset(Instant.ofEpochMilli(epochMillis)).getTotalSeconds() * 1_000;
    }

    private static Instant nextTransition(ZoneId timeZone, long epochMillis)
    {
        ZoneOffsetTransition transition = timeZone.getRules().nextTransition(Instant.ofEpochMilli(epochMillis));
        if (transition == null || transition.getInstant().isAfter(Instant.ofEpochMilli(Long.MAX_VALUE))) {
            return Instant.MAX;
        }
        return transition.getInstant();
    }
}
