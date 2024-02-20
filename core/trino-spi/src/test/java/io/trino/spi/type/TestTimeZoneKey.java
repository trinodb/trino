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
package io.trino.spi.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.SortedSet;

import static io.trino.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Comparator.comparingInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimeZoneKey
{
    private static final TimeZoneKey PLUS_7_KEY = TimeZoneKey.getTimeZoneKeyForOffset(7 * 60);
    private static final TimeZoneKey MINUS_7_KEY = TimeZoneKey.getTimeZoneKeyForOffset(-7 * 60);

    @Test
    public void testUTC()
    {
        assertThat(UTC_KEY.getKey()).isEqualTo((short) 0);
        assertThat(UTC_KEY.getId()).isEqualTo("UTC");

        assertThat(TimeZoneKey.getTimeZoneKey((short) 0)).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC")).isSameAs(UTC_KEY);

        // verify UTC equivalent zones map to UTC
        assertThat(TimeZoneKey.getTimeZoneKey("Z")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Zulu")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UCT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Universal")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("+00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("-00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("+0000")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("-0000")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT-00:00")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UCT")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/Universal")).isSameAs(UTC_KEY);
    }

    @Test
    public void testHourOffsetZone()
    {
        assertThat(TimeZoneKey.getTimeZoneKey("GMT0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("GMT7");
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("GMT-7")).isSameAs(MINUS_7_KEY);

        assertTimeZoneNotSupported("UT0");
        assertThat(TimeZoneKey.getTimeZoneKey("UT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("UT7");
        assertThat(TimeZoneKey.getTimeZoneKey("UT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT-7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UT-7")).isSameAs(MINUS_7_KEY);

        assertTimeZoneNotSupported("UTC0");
        assertThat(TimeZoneKey.getTimeZoneKey("UTC+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("UTC7");
        assertThat(TimeZoneKey.getTimeZoneKey("UTC+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC-7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("UTC-7")).isSameAs(MINUS_7_KEY);

        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("Etc/GMT7");
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT-7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT+7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/GMT-7")).isSameAs(PLUS_7_KEY);

        assertTimeZoneNotSupported("Etc/UT0");
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("Etc/UT7");
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT-7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UT-7")).isSameAs(MINUS_7_KEY);

        assertTimeZoneNotSupported("Etc/UTC0");
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+0")).isSameAs(UTC_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-0")).isSameAs(UTC_KEY);
        assertTimeZoneNotSupported("Etc/UTC7");
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-7")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+7")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-7")).isSameAs(MINUS_7_KEY);

        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-7:00")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-07:00")).isSameAs(MINUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+7:00")).isSameAs(PLUS_7_KEY);
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+07:00")).isSameAs(PLUS_7_KEY);

        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-7:35")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(-(7 * 60 + 35)));
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC-07:35")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(-(7 * 60 + 35)));
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+7:35")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(7 * 60 + 35));
        assertThat(TimeZoneKey.getTimeZoneKey("Etc/UTC+07:35")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(7 * 60 + 35));

        assertThat(TimeZoneKey.getTimeZoneKey("+0735")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(7 * 60 + 35));
        assertThat(TimeZoneKey.getTimeZoneKey("-0735")).isSameAs(TimeZoneKey.getTimeZoneKeyForOffset(-(7 * 60 + 35)));
    }

    @Test
    public void testZoneKeyLookup()
    {
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            assertThat(TimeZoneKey.getTimeZoneKey(timeZoneKey.getKey())).isSameAs(timeZoneKey);
            assertThat(TimeZoneKey.getTimeZoneKey(timeZoneKey.getId())).isSameAs(timeZoneKey);
        }
    }

    @Test
    public void testMaxTimeZoneKey()
    {
        boolean foundMax = false;
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            assertThat(timeZoneKey.getKey() <= MAX_TIME_ZONE_KEY)
                    .describedAs(timeZoneKey + " key is larger than max key " + MAX_TIME_ZONE_KEY)
                    .isTrue();
            foundMax = foundMax || (timeZoneKey.getKey() == MAX_TIME_ZONE_KEY);
        }
        assertThat(foundMax)
                .describedAs("Did not find a time zone with the MAX_TIME_ZONE_KEY")
                .isTrue();
    }

    @Test
    public void testZoneKeyIdRange()
    {
        boolean[] hasValue = new boolean[MAX_TIME_ZONE_KEY + 1];

        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            short key = timeZoneKey.getKey();
            assertThat(key >= 0)
                    .describedAs(timeZoneKey + " has a negative time zone key")
                    .isTrue();
            assertThat(hasValue[key])
                    .describedAs("Another time zone has the same zone key as " + timeZoneKey)
                    .isFalse();
            hasValue[key] = true;
        }

        // previous spot for Canada/East-Saskatchewan
        assertThat(hasValue[2040]).isFalse();
        hasValue[2040] = true;
        // previous spot for EST
        assertThat(hasValue[2180]).isFalse();
        hasValue[2180] = true;
        // previous spot for HST
        assertThat(hasValue[2186]).isFalse();
        hasValue[2186] = true;
        // previous spot for MST
        assertThat(hasValue[2196]).isFalse();
        hasValue[2196] = true;
        // previous spot for US/Pacific-New
        assertThat(hasValue[2174]).isFalse();
        hasValue[2174] = true;

        for (int i = 0; i < hasValue.length; i++) {
            assertThat(hasValue[i])
                    .describedAs("There is no time zone with key " + i)
                    .isTrue();
        }
    }

    @Test
    public void testZoneKeyData()
    {
        Hasher hasher = Hashing.murmur3_128().newHasher();

        SortedSet<TimeZoneKey> timeZoneKeysSortedByKey = ImmutableSortedSet.copyOf(comparingInt(TimeZoneKey::getKey), TimeZoneKey.getTimeZoneKeys());

        for (TimeZoneKey timeZoneKey : timeZoneKeysSortedByKey) {
            hasher.putShort(timeZoneKey.getKey());
            hasher.putString(timeZoneKey.getId(), StandardCharsets.UTF_8);
        }
        // Zone file should not (normally) be changed, so let's make this more difficult
        assertThat(hasher.hash().asLong())
                .describedAs("zone-index.properties file contents changed!")
                .isEqualTo(4825838578917475630L);
    }

    @Test
    public void testRoundTripSerialization()
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        for (TimeZoneKey zoneKey : TimeZoneKey.getTimeZoneKeys()) {
            String json = mapper.writeValueAsString(zoneKey);
            Object value = mapper.readValue(json, zoneKey.getClass());
            assertThat(value).isEqualTo(zoneKey);
        }
    }

    private void assertTimeZoneNotSupported(String zoneId)
    {
        assertThatThrownBy(() -> TimeZoneKey.getTimeZoneKey(zoneId))
                .isInstanceOf(TimeZoneNotSupportedException.class)
                .hasMessageStartingWith("Time zone not supported: ");
    }
}
