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
package io.trino.execution.recovery;

import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public record BackoffPolicy(int maxRetries, Duration initialDelay, Duration maxDelay, double scaleFactor)
{
    private static final Duration ZERO = new Duration(0, MILLISECONDS);

    public BackoffPolicy
    {
        // 0 is a valid configuration: query-retry-attempts=0 means the failure is passed on without any retry
        checkArgument(maxRetries >= 0, "maxRetries must be >= 0, got %s", maxRetries);
        checkArgument(scaleFactor >= 1.0, "scaleFactor must be >= 1.0, got %s", scaleFactor);
        requireNonNull(initialDelay, "initialDelay is null");
        requireNonNull(maxDelay, "maxDelay is null");
        checkArgument(maxDelay.compareTo(initialDelay) >= 0, "maxDelay (%s) must be >= initialDelay (%s)", maxDelay, initialDelay);
    }

    public static final BackoffPolicy RETRY_ONCE_IMMEDIATE = new BackoffPolicy(1, ZERO, ZERO, 1.0);

    public Duration delayFor(int retry)
    {
        long millis = min(initialDelay.toMillis() * (long) pow(scaleFactor, retry), maxDelay.toMillis());
        return new Duration(millis, MILLISECONDS);
    }
}
