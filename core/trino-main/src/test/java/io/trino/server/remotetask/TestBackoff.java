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
package io.trino.server.remotetask;

import com.google.common.collect.ImmutableList;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBackoff
{
    @Test
    public void testFailureInterval()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS), ticker, ImmutableList.of(new Duration(10, MILLISECONDS)));
        ticker.increment(10, MICROSECONDS);

        // verify initial state
        assertThat(backoff.getFailureCount()).isEqualTo(0);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);

        // first failure, should never fail
        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(1);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);

        ticker.increment(14, SECONDS);

        // second failure within the limit, should not fail
        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(2);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(14);

        ticker.increment(1, SECONDS);

        // final failure after the limit causes failure
        assertThat(backoff.failure()).isTrue();
        assertThat(backoff.getFailureCount()).isEqualTo(3);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(15);
    }

    @Test
    public void testMinTries()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(3, new Duration(1, NANOSECONDS), ticker, ImmutableList.of(new Duration(10, MILLISECONDS)));
        ticker.increment(10, MICROSECONDS);

        // verify initial state
        assertThat(backoff.getFailureCount()).isEqualTo(0);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);

        // first failure, should never fail
        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(1);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);

        ticker.increment(14, SECONDS);

        // second failure under min failures, should not fail
        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(2);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(14);

        ticker.increment(1, SECONDS);

        // last try failed
        assertThat(backoff.failure()).isTrue();
        assertThat(backoff.getFailureCount()).isEqualTo(3);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(15);
    }

    @Test
    public void testStartRequest()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS), ticker, ImmutableList.of(new Duration(10, MILLISECONDS)));
        ticker.increment(10, MICROSECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(1);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);
        assertThat(backoff.getFailureRequestTimeTotal().roundTo(SECONDS)).isEqualTo(0);

        ticker.increment(7, SECONDS);
        backoff.startRequest();
        ticker.increment(7, SECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(2);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(14);
        // failed request took 7 seconds.
        assertThat(backoff.getFailureRequestTimeTotal().roundTo(SECONDS)).isEqualTo(7);

        ticker.increment(1, SECONDS);
        backoff.startRequest();
        ticker.increment(1, SECONDS);

        assertThat(backoff.failure()).isTrue();
        assertThat(backoff.getFailureCount()).isEqualTo(3);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(16);
        // failed requests took 7+1 seconds.
        assertThat(backoff.getFailureRequestTimeTotal().roundTo(SECONDS)).isEqualTo(8);
    }

    @Test
    public void testDelay()
    {
        // 1, 2, 4, 8
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS), ticker, ImmutableList.of(
                new Duration(0, SECONDS),
                new Duration(1, SECONDS),
                new Duration(2, SECONDS),
                new Duration(4, SECONDS),
                new Duration(8, SECONDS)));

        assertThat(backoff.getFailureCount()).isEqualTo(0);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(1);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);
        long backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(0);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(2);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(0);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(1);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(3);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(1);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(2);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(4);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(3);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(4);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertThat(backoff.failure()).isFalse();
        assertThat(backoff.getFailureCount()).isEqualTo(5);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(7);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(8);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertThat(backoff.failure()).isTrue();
        assertThat(backoff.getFailureCount()).isEqualTo(6);
        assertThat(backoff.getFailureDuration().roundTo(SECONDS)).isEqualTo(15);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertThat(NANOSECONDS.toSeconds(backoffDelay)).isEqualTo(8);
    }
}
