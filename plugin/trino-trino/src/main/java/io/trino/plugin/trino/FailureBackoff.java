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
package io.trino.plugin.trino;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Suppresses retry attempts for a fixed window after the most recent failure.
 * Concurrent attempts are intentionally allowed, matching the surrounding
 * capabilities loading behavior.
 */
final class FailureBackoff
{
    private final long backoffNanos;
    private final LongSupplier nanoTicker;
    // Boxed so that "no failure yet" is distinguishable from any ticker value
    private volatile Long lastFailureNanos;

    FailureBackoff(Duration backoff)
    {
        this(backoff, System::nanoTime);
    }

    FailureBackoff(Duration backoff, LongSupplier nanoTicker)
    {
        this.backoffNanos = backoff.toNanos();
        this.nanoTicker = nanoTicker;
    }

    boolean shouldAttempt()
    {
        Long lastFailure = lastFailureNanos;
        return lastFailure == null || nanoTicker.getAsLong() - lastFailure >= backoffNanos;
    }

    void recordFailure()
    {
        lastFailureNanos = nanoTicker.getAsLong();
    }
}
