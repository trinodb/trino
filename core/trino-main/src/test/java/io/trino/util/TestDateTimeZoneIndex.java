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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.util.List;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.trino.util.DateTimeZoneIndex.convertLocalToUtc;
import static io.trino.util.DateTimeZoneIndex.convertUtcToLocal;
import static io.trino.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static io.trino.util.DateTimeZoneIndex.getZoneId;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeZoneIndex
{
    @Test
    public void testOffsets()
    {
        for (TimeZoneKey key : getTimeZoneKeys()) {
            ZoneId zoneId = getZoneId(key);
            for (Instant instant : List.of(Instant.parse("1900-01-01T00:00:00Z"), Instant.EPOCH, Instant.parse("2026-07-01T00:00:00Z"))) {
                assertThat(extractZoneOffsetMinutes(instant.toEpochMilli(), key.getKey()))
                        .isEqualTo(zoneId.getRules().getOffset(instant).getTotalSeconds() / 60);
                assertThat(convertUtcToLocal(instant.toEpochMilli(), zoneId))
                        .isEqualTo(instant.atZone(zoneId).toLocalDateTime().toInstant(UTC).toEpochMilli());
            }
        }
    }

    @Test
    public void testTransitions()
    {
        for (String id : List.of("Europe/Warsaw", "America/New_York", "Asia/Kathmandu", "Australia/Lord_Howe", "Pacific/Apia")) {
            ZoneId zoneId = ZoneId.of(id);
            for (ZoneOffsetTransition transition : zoneId.getRules().getTransitions()) {
                LocalDateTime local = transition.getDateTimeBefore().minusMinutes(1);
                if (transition.isGap()) {
                    local = transition.getDateTimeBefore().plusMinutes(1);
                }
                long localMillis = local.toInstant(UTC).toEpochMilli();
                long utcMillis = convertLocalToUtc(localMillis, zoneId);
                if (transition.isGap()) {
                    assertThat(utcMillis).isEqualTo(local.atZone(zoneId).toInstant().toEpochMilli());
                }
                else {
                    // Historical overlaps can resolve to either valid offset.
                    assertThat(convertUtcToLocal(utcMillis, zoneId)).isEqualTo(localMillis);
                }
            }
        }
    }

    @Test
    public void testOverlapPreference()
    {
        ZoneId zoneId = ZoneId.of("America/New_York");
        long localMillis = LocalDateTime.of(2026, 11, 1, 1, 30).toInstant(UTC).toEpochMilli();
        long earlier = Instant.parse("2026-11-01T05:30:00Z").toEpochMilli();
        long later = Instant.parse("2026-11-01T06:30:00Z").toEpochMilli();
        assertThat(convertLocalToUtc(localMillis, zoneId, earlier)).isEqualTo(earlier);
        assertThat(convertLocalToUtc(localMillis, zoneId, later)).isEqualTo(later);
    }

    @Test
    public void testOverflow()
    {
        ZoneId zoneId = ZoneId.of("+14:00");
        assertThatThrownBy(() -> convertUtcToLocal(Long.MAX_VALUE, zoneId)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> convertLocalToUtc(Long.MIN_VALUE, zoneId)).isInstanceOf(ArithmeticException.class);
    }
}
