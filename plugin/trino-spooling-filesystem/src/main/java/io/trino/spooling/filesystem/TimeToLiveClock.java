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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.Duration;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static com.google.common.base.Verify.verify;
import static java.time.ZoneId.systemDefault;
import static java.util.Objects.requireNonNull;

public class TimeToLiveClock
        extends Clock
{
    private final long ttlMillis;
    private final Clock delegate;

    public TimeToLiveClock(Duration ttl)
    {
        this(ttl.toMillis(), tickMillis(systemDefault()));
    }

    @VisibleForTesting
    TimeToLiveClock(long ttlMillis, Clock delegate)
    {
        verify(ttlMillis > 0, "ttlMillis must be positive");
        this.ttlMillis = ttlMillis;
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ZoneId getZone()
    {
        return delegate.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone)
    {
        return new TimeToLiveClock(ttlMillis, delegate.withZone(zone));
    }

    @Override
    public Instant instant()
    {
        return delegate.instant().plusMillis(ttlMillis);
    }
}
