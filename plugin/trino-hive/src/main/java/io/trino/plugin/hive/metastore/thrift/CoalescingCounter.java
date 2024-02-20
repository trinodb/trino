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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.Duration;

import java.time.Clock;

import static java.util.Objects.requireNonNull;

/**
 * An event counter that coalesces events within provided time window.
 */
@ThreadSafe
final class CoalescingCounter
{
    private final Clock clock;
    private final long coalescingDurationMillis;

    @GuardedBy("this")
    private long count;

    @GuardedBy("this")
    private long lastUpdateTime;

    public CoalescingCounter(Duration coalescingDuration)
    {
        this(Clock.systemUTC(), coalescingDuration);
    }

    @VisibleForTesting
    CoalescingCounter(Clock clock, Duration coalescingDuration)
    {
        this.clock = requireNonNull(clock, "clock is null");
        coalescingDurationMillis = coalescingDuration.toMillis();
    }

    private synchronized void increment()
    {
        long now = clock.instant().toEpochMilli();
        if (lastUpdateTime + coalescingDurationMillis >= now) {
            return;
        }

        count++;
        lastUpdateTime = now;
    }

    public synchronized long get()
    {
        return count;
    }

    public synchronized long incrementAndGet()
    {
        increment();
        return get();
    }
}
