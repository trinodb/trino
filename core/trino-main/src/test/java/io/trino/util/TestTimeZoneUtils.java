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

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.trino.util.DateTimeZoneIndex.getZoneId;
import static io.trino.util.DateTimeZoneIndex.unpackZoneId;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeZoneUtils
{
    @Test
    public void testNamedZones()
    {
        for (TimeZoneKey key : getTimeZoneKeys()) {
            long packed = packDateTimeWithZone(42, key);
            assertThat(unpackMillisUtc(packed)).isEqualTo(42);
            assertThat(unpackZoneKey(packed)).isEqualTo(key);
            assertThat(unpackZoneId(packed)).isEqualTo(key.getZoneId());
        }
    }

    @Test
    public void testOffsets()
    {
        for (int offsetMinutes = -14 * 60; offsetMinutes <= 14 * 60; offsetMinutes++) {
            TimeZoneKey key = getTimeZoneKeyForOffset(offsetMinutes);
            assertThat(getZoneId(key).getRules().isFixedOffset()).isTrue();
            assertThat(getZoneId(key).getRules().getOffset(Instant.EPOCH).getTotalSeconds()).isEqualTo(offsetMinutes * 60);
        }
    }
}
