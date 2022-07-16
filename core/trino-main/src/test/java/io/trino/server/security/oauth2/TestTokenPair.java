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
package io.trino.server.security.oauth2;

import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;

import static io.trino.server.security.oauth2.TokenPairSerializer.TokenPair.accessAndRefreshTokens;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTokenPair
{
    @Test
    public void testHasExpiredWhenExpirationIsAfterCurrentTime()
    {
        TestingClock clock = new TestingClock();

        TokenPair tokens = expiredIn(clock, Duration.ofMinutes(10));
        clock.advanceBy(Duration.ofMinutes(11));

        assertThat(tokens.isBeforeExpirationForAtLeast(Duration.ZERO, clock)).isFalse();
    }

    @Test
    public void testHasNotExpiredWhenExpirationIsBeforeCurrentTime()
    {
        TestingClock clock = new TestingClock();

        TokenPair tokens = expiredIn(clock, Duration.ofMinutes(10));
        clock.advanceBy(Duration.ofMinutes(5));

        assertThat(tokens.isBeforeExpirationForAtLeast(Duration.ZERO, clock)).isTrue();
    }

    @Test
    public void testIsBeforeExpirationForAtLeastReturningFalseWhenAddedTimeExceedsCurrentTime()
    {
        TestingClock clock = new TestingClock();

        TokenPair tokens = expiredIn(clock, Duration.ofMinutes(10));
        clock.advanceBy(Duration.ofMinutes(5));

        assertThat(tokens.isBeforeExpirationForAtLeast(Duration.ofMinutes(6), clock)).isFalse();
    }

    @Test
    public void testIsBeforeExpirationForAtLeastReturningFalseWhenAddedTimeMatchesExactlyCurrentTime()
    {
        TestingClock clock = new TestingClock();

        TokenPair tokens = expiredIn(clock, Duration.ofMinutes(10));
        clock.advanceBy(Duration.ofMinutes(5));

        assertThat(tokens.isBeforeExpirationForAtLeast(Duration.ofMinutes(5), clock)).isFalse();
    }

    @Test
    public void testIsBeforeExpirationForAtLeastReturningTrueWhenAddedTimeIsBeforeCurrentTime()
    {
        TestingClock clock = new TestingClock();

        TokenPair tokens = expiredIn(clock, Duration.ofMinutes(10));
        clock.advanceBy(Duration.ofMinutes(5));

        assertThat(tokens.isBeforeExpirationForAtLeast(Duration.ofMinutes(4), clock)).isTrue();
    }

    private static TokenPair expiredIn(Clock clock, Duration duration)
    {
        return accessAndRefreshTokens("access_token", Date.from(clock.instant().plus(duration)), null);
    }
}
