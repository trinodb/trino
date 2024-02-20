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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCoalescingCounter
{
    @Test
    public void test()
    {
        TestingClock clock = new TestingClock();
        CoalescingCounter counter = new CoalescingCounter(clock, new Duration(1, SECONDS));

        assertThat(counter.get()).isEqualTo(0);
        assertThat(counter.incrementAndGet()).isEqualTo(1);
        assertThat(counter.incrementAndGet()).isEqualTo(1);
        assertThat(counter.get()).isEqualTo(1);

        clock.increment(100, MILLISECONDS);
        assertThat(counter.incrementAndGet()).isEqualTo(1);
        assertThat(counter.incrementAndGet()).isEqualTo(1);
        assertThat(counter.get()).isEqualTo(1);

        clock.increment(1000, MILLISECONDS);
        assertThat(counter.incrementAndGet()).isEqualTo(2);
        assertThat(counter.incrementAndGet()).isEqualTo(2);
        assertThat(counter.get()).isEqualTo(2);
    }

    // TODO replace with https://github.com/airlift/airlift/pull/780
    private static final class TestingClock
            extends Clock
    {
        private final ZoneId zone;
        private Instant instant;

        public TestingClock()
        {
            this(ZoneOffset.UTC);
        }

        public TestingClock(ZoneId zone)
        {
            this(zone, Instant.ofEpochMilli(1575000618963L));
        }

        private TestingClock(ZoneId zone, Instant instant)
        {
            this.zone = requireNonNull(zone, "zone is null");
            this.instant = requireNonNull(instant, "instant is null");
        }

        @Override
        public ZoneId getZone()
        {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return new TestingClock(zone, instant);
        }

        @Override
        public Instant instant()
        {
            return instant;
        }

        public void increment(long delta, TimeUnit unit)
        {
            checkArgument(delta >= 0, "delta is negative");
            instant = instant.plusNanos(unit.toNanos(delta));
        }
    }
}
