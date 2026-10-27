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

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static org.assertj.core.api.Assertions.assertThat;

class TestTimeZoneOffset
{
    @Test
    void testOffsets()
    {
        assertOffset("UTC", "2024-01-01T00:00:00Z", 0);
        assertOffset("+05:45", "1900-01-01T00:00:00Z", 345);
        assertOffset("America/New_York", "2024-03-10T06:59:59Z", -300);
        assertOffset("America/New_York", "2024-03-10T07:00:00Z", -240);
        assertOffset("Europe/Warsaw", "2024-10-27T00:59:59Z", 120);
        assertOffset("Europe/Warsaw", "2024-10-27T01:00:00Z", 60);
        assertOffset("Africa/Monrovia", "1900-01-01T00:00:00Z", -43);
    }

    private static void assertOffset(String zone, String instant, int minutes)
    {
        assertThat(TimeZoneOffset.zoneOffsetMinutes(Instant.parse(instant).toEpochMilli(), getTimeZoneKey(zone).getKey()))
                .isEqualTo((short) minutes);
    }
}
