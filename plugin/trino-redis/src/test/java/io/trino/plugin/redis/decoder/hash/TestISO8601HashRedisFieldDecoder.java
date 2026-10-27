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
package io.trino.plugin.redis.decoder.hash;

import io.trino.plugin.redis.RedisColumnHandle;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestISO8601HashRedisFieldDecoder
{
    @Test
    void testIsoDateVariantsAndOffsets()
    {
        assertThat(decode("2018-040T15:15:19.123+02:00", TIMESTAMP_MILLIS)).isEqualTo(1_518_182_119_123_000L);
        assertThat(decode("2018-040T15:15:19.123+02:00", TIMESTAMP_TZ_MILLIS))
                .isEqualTo(packDateTimeWithZone(1_518_182_119_123L, getTimeZoneKeyForOffset(120)));
        assertThat(decode("2018-W06-5T13:15:19Z", TIMESTAMP_MILLIS)).isEqualTo(1_518_182_119_000_000L);
        assertThat(decode("2018-02-09T13:15.5Z", TIMESTAMP_MILLIS)).isEqualTo(1_518_182_130_000_000L);
        assertThat(decode("2018", TIMESTAMP_TZ_MILLIS)).isEqualTo(packDateTimeWithZone(1_514_764_800_000L, UTC_KEY));
    }

    @Test
    void testInvalidDate()
    {
        assertThatThrownBy(() -> decode("2019-02-29T13:15:19Z", TIMESTAMP_MILLIS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static long decode(String value, Type type)
    {
        return new ISO8601HashRedisFieldDecoder()
                .decode(value, new RedisColumnHandle(0, "value", type, null, "iso8601", null, false, false, false))
                .getLong();
    }
}
