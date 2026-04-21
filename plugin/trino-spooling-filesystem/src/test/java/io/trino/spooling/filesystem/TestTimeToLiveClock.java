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
package io.trino.spooling.filesystem;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static java.time.ZoneOffset.MAX;
import static java.time.ZoneOffset.MIN;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

class TestTimeToLiveClock
{
    @Test
    void testTimeToLiveClock()
    {
        testTimeToLiveClock(MIN);
        testTimeToLiveClock(MAX);
        testTimeToLiveClock(UTC);
        testTimeToLiveClock(ZoneId.of("Europe/Warsaw"));
        testTimeToLiveClock(ZoneId.of("America/New_York"));
        testTimeToLiveClock(ZoneId.of("Asia/Tokyo"));
    }

    void testTimeToLiveClock(ZoneId zoneId)
    {
        Duration ttl = Duration.of(1, HOURS);
        Instant now = Instant.now();

        Clock fixedClock = Clock.fixed(now, zoneId);
        TimeToLiveClock ttlClock = new TimeToLiveClock(ttl.toMillis(), fixedClock);

        assertThat(ttlClock.getZone())
                .isEqualTo(zoneId);

        assertThat(ttlClock.instant())
                .isEqualTo(fixedClock.instant().plus(ttl))
                .isEqualTo(now.plus(ttl));

        assertThat(ttlClock.withZone(zoneId).instant())
                .isEqualTo(fixedClock.withZone(zoneId).instant().plus(ttl))
                .isEqualTo(now.plus(ttl));
    }
}
