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
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdkDateTimeZone
{
    @Test
    public void testRules()
    {
        List<Instant> instants = List.of(
                Instant.ofEpochMilli(Long.MIN_VALUE),
                Instant.parse("1800-01-01T00:00:00Z"),
                Instant.parse("1900-01-01T00:00:00Z"),
                Instant.EPOCH,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-07-01T00:00:00Z"),
                Instant.parse("2100-07-01T00:00:00Z"),
                Instant.ofEpochMilli(Long.MAX_VALUE));
        for (TimeZoneKey key : getTimeZoneKeys()) {
            DateTimeZone zone = getDateTimeZone(key);
            ZoneRules rules = key.getZoneId().getRules();
            assertThat(zone.getID()).isEqualTo(key.getId());
            assertThat(zone.isFixed()).isEqualTo(rules.isFixedOffset());
            for (Instant instant : instants) {
                long millis = instant.toEpochMilli();
                assertThat(zone.getOffset(millis))
                        .as("offset for %s at %s", key.getId(), instant)
                        .isEqualTo(rules.getOffset(instant).getTotalSeconds() * 1000);
                assertThat(zone.getStandardOffset(millis))
                        .as("standard offset for %s at %s", key.getId(), instant)
                        .isEqualTo(rules.getStandardOffset(instant).getTotalSeconds() * 1000);
            }
        }
    }

    @Test
    public void testTransitions()
    {
        for (String id : List.of("Europe/Warsaw", "America/New_York", "Asia/Kathmandu", "Australia/Lord_Howe", "Pacific/Apia")) {
            ZoneRules rules = ZoneId.of(id).getRules();
            DateTimeZone zone = getDateTimeZone(TimeZoneKey.getTimeZoneKey(id));
            for (ZoneOffsetTransition transition : rules.getTransitions()) {
                assertTransition(zone, transition);
            }
            ZoneOffsetTransition futureTransition = rules.nextTransition(Instant.parse("2100-01-01T00:00:00Z"));
            if (futureTransition != null) {
                assertTransition(zone, futureTransition);
            }
        }
    }

    private static void assertTransition(DateTimeZone zone, ZoneOffsetTransition transition)
    {
        long millis = transition.getInstant().toEpochMilli();
        assertThat(zone.getOffset(millis - 1)).isEqualTo(transition.getOffsetBefore().getTotalSeconds() * 1000);
        assertThat(zone.getOffset(millis)).isEqualTo(transition.getOffsetAfter().getTotalSeconds() * 1000);
        assertThat(zone.nextTransition(millis - 1)).isEqualTo(millis);
        assertThat(zone.nextTransition(millis)).isGreaterThanOrEqualTo(millis);
        assertThat(zone.previousTransition(millis)).isEqualTo(millis - 1);
        assertThat(zone.previousTransition(millis + 1)).isEqualTo(millis - 1);
    }

    @Test
    public void testNoTransition()
    {
        DateTimeZone fixedZone = getDateTimeZone(TimeZoneKey.getTimeZoneKey("+05:45"));
        DateTimeZone variableZone = getDateTimeZone(TimeZoneKey.getTimeZoneKey("Europe/Warsaw"));
        for (long millis : List.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE)) {
            assertThat(fixedZone.nextTransition(millis)).isEqualTo(millis);
            assertThat(fixedZone.previousTransition(millis)).isEqualTo(millis);
        }
        assertThat(variableZone.previousTransition(Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
        assertThat(variableZone.nextTransition(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testLocalTimeConversions()
    {
        for (String id : List.of("Europe/Warsaw", "America/New_York", "Australia/Lord_Howe", "Pacific/Apia")) {
            ZoneId zoneId = ZoneId.of(id);
            DateTimeZone zone = getDateTimeZone(TimeZoneKey.getTimeZoneKey(id));
            for (ZoneOffsetTransition transition : zoneId.getRules().getTransitions()) {
                LocalDateTime local = transition.getDateTimeBefore().minusMinutes(1);
                if (transition.isGap()) {
                    local = transition.getDateTimeBefore().plusMinutes(1);
                }
                long localMillis = local.toInstant(UTC).toEpochMilli();
                if (transition.isGap()) {
                    assertThat(zone.getOffsetFromLocal(localMillis))
                            .as("offset in gap %s in %s", local, id)
                            .isEqualTo(transition.getOffsetBefore().getTotalSeconds() * 1000);
                    assertThat(zone.convertLocalToUTC(localMillis, false))
                            .isEqualTo(local.atZone(zoneId).toInstant().toEpochMilli());
                    assertThatThrownBy(() -> zone.convertLocalToUTC(localMillis, true))
                            .isInstanceOf(IllegalArgumentException.class);
                }
                else {
                    // Preserve Joda-Time's overlap resolution, which can choose either valid offset.
                    assertThat(zone.getOffsetFromLocal(localMillis))
                            .as("offset in overlap %s in %s", local, id)
                            .isIn(transition.getOffsetBefore().getTotalSeconds() * 1000, transition.getOffsetAfter().getTotalSeconds() * 1000);
                    long utcMillis = zone.convertLocalToUTC(localMillis, false);
                    assertThat(zone.convertUTCToLocal(utcMillis)).isEqualTo(localMillis);
                }
            }
        }
    }

    @Test
    public void testNames()
    {
        DateTimeZone zone = getDateTimeZone(TimeZoneKey.getTimeZoneKey("America/New_York"));
        long winter = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
        long summer = Instant.parse("2026-07-01T00:00:00Z").toEpochMilli();
        assertThat(zone.getShortName(winter, Locale.US)).isEqualTo("EST");
        assertThat(zone.getShortName(summer, Locale.US)).isEqualTo("EDT");
        assertThat(zone.getName(winter, Locale.US)).isEqualTo("Eastern Standard Time");
        assertThat(zone.getName(summer, Locale.US)).isEqualTo("Eastern Daylight Time");
    }
}
