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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static io.trino.plugin.base.type.TimeZoneConversions.convertLocalToUtc;
import static io.trino.plugin.base.type.TimeZoneConversions.convertUtcToLocal;
import static io.trino.plugin.base.type.TimeZoneConversions.getOffsetFromLocalMillis;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTimeZoneConversions
{
    @Test
    void testUtc()
    {
        for (long millis : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            assertThat(convertUtcToLocal(UTC, millis)).isEqualTo(millis);
            assertThat(convertLocalToUtc(UTC, millis)).isEqualTo(millis);
        }
    }

    @Test
    void testOverlaps()
    {
        assertThat(convertLocalToUtc(ZoneId.of("Europe/Warsaw"), millis("2024-10-27T02:30:00.123Z")))
                .isEqualTo(millis("2024-10-27T01:30:00.123Z"));
        assertThat(convertLocalToUtc(ZoneId.of("America/New_York"), millis("2024-11-03T01:30:00.123Z")))
                .isEqualTo(millis("2024-11-03T05:30:00.123Z"));
    }

    @Test
    void testGaps()
    {
        assertThat(convertLocalToUtc(ZoneId.of("Europe/Warsaw"), millis("2024-03-31T02:30:00.123Z")))
                .isEqualTo(millis("2024-03-31T01:30:00.123Z"));
        assertThat(convertLocalToUtc(ZoneId.of("America/New_York"), millis("2024-03-10T02:30:00.123Z")))
                .isEqualTo(millis("2024-03-10T07:30:00.123Z"));
        assertThat(convertLocalToUtc(ZoneId.of("Pacific/Apia"), millis("2011-12-30T12:00:00Z")))
                .isEqualTo(millis("2011-12-30T22:00:00Z"));
        assertThatThrownBy(() -> convertLocalToUtc(ZoneId.of("Europe/Warsaw"), millis("2024-03-31T02:30:00Z"), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Europe/Warsaw");
    }

    @Test
    void testHistoricalOffsets()
    {
        assertThat(convertUtcToLocal(ZoneId.of("Europe/Paris"), millis("1900-01-01T00:00:00.123Z")))
                .isEqualTo(millis("1900-01-01T00:09:21.123Z"));
        assertThat(convertLocalToUtc(ZoneId.of("Europe/Paris"), millis("1900-01-01T00:09:21.123Z")))
                .isEqualTo(millis("1900-01-01T00:00:00.123Z"));
    }

    @Test
    void testOffsetFromLocal()
    {
        assertThat(getOffsetFromLocalMillis(ZoneId.of("Europe/Warsaw"), millis("2024-10-27T02:30:00Z"))).isEqualTo(7_200_000);
        assertThat(getOffsetFromLocalMillis(ZoneId.of("America/New_York"), millis("2024-11-03T01:30:00Z"))).isEqualTo(-14_400_000);
        assertThat(getOffsetFromLocalMillis(ZoneId.of("Pacific/Apia"), millis("1892-07-04T23:59:00Z"))).isEqualTo(-41_216_000);
    }

    @Test
    void testOverflow()
    {
        assertThatThrownBy(() -> convertUtcToLocal(ZoneOffset.ofHours(1), Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> convertLocalToUtc(ZoneOffset.ofHours(1), Long.MIN_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    private static long millis(String instant)
    {
        return Instant.parse(instant).toEpochMilli();
    }
}
