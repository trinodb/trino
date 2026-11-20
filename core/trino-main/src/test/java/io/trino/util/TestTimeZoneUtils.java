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

import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimeZoneNotSupportedException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.TreeSet;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;
import static io.trino.util.DateTimeZoneIndex.packDateTimeWithZone;
import static io.trino.util.DateTimeZoneIndex.unpackDateTimeZone;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Fail.fail;

public class TestTimeZoneUtils
{
    @Test
    public void testNamedZones()
    {
        TreeSet<String> jdkZones = new TreeSet<>(ZoneId.getAvailableZoneIds());
        for (String zoneId : jdkZones) {
            if (zoneId.startsWith("SystemV/")) {
                // SystemV zones are not supported by TimeZoneKey
                assertThat(catchThrowable(() -> TimeZoneKey.getTimeZoneKey(zoneId)))
                        .as("getTimeZoneKey('%s') error", zoneId)
                        .isInstanceOf(TimeZoneNotSupportedException.class);
                continue;
            }

            DateTimeZone dateTimeZone = DateTimeZone.forID(zoneId);
            if (equivalent(dateTimeZone, DateTimeZone.UTC)) {
                assertThat(TimeZoneKey.getTimeZoneKey(zoneId))
                        .as("TimeZoneKey should recognize zone '%s' as UTC-equivalent", zoneId)
                        .isSameAs(TimeZoneKey.UTC_KEY);
                continue;
            }

            DateTimeZone indexedZone = getDateTimeZone(TimeZoneKey.getTimeZoneKey(zoneId));
            assertThat(equivalent(indexedZone, dateTimeZone))
                    .as("getDateTimeZone('%s') returned '%s', expected '%s'", zoneId, indexedZone, dateTimeZone)
                    .isTrue();
            assertTimeZone(zoneId, dateTimeZone);
        }
    }

    @Test
    public void testOffsets()
    {
        for (int offsetHours = -13; offsetHours < 14; offsetHours++) {
            for (int offsetMinutes = 0; offsetMinutes < 60; offsetMinutes++) {
                DateTimeZone dateTimeZone = DateTimeZone.forOffsetHoursMinutes(offsetHours, offsetMinutes);
                assertTimeZone(dateTimeZone.getID(), dateTimeZone);
            }
        }
    }

    private static void assertTimeZone(String zoneId, DateTimeZone dateTimeZone)
    {
        long packWithDateTime = packDateTimeWithZone(new DateTime(42, dateTimeZone));
        long packWithZoneId = packDateTimeWithZone(42L, ZoneId.of(dateTimeZone.getID()).getId());
        if (packWithDateTime != packWithZoneId) {
            fail(format(
                    "packWithDateTime and packWithZoneId differ for zone [%s] / [%s]: %s [%s %s] and %s [%s %s]",
                    zoneId,
                    dateTimeZone,
                    packWithDateTime,
                    unpackMillisUtc(packWithDateTime),
                    unpackZoneKey(packWithDateTime),
                    packWithZoneId,
                    unpackMillisUtc(packWithZoneId),
                    unpackZoneKey(packWithZoneId)));
        }
        DateTimeZone unpackedZone = unpackDateTimeZone(packWithDateTime);
        assertThat(equivalent(unpackedZone, dateTimeZone))
                .as("unpackDateTimeZone(packDateTimeWithZone('%s')) returned '%s', expected '%s'", zoneId, unpackedZone, dateTimeZone)
                .isTrue();
    }

    private static boolean equivalent(DateTimeZone actual, DateTimeZone expected)
    {
        return actual.equals(expected)
                || (actual.isFixed() && expected.isFixed() && actual.getOffset(0) == expected.getOffset(0));
    }
}
