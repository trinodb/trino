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
package io.prestosql.client.auth.external;

import net.jodah.failsafe.RetryPolicy;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

class RetryPolicies
{
    private RetryPolicies()
    {
    }

    static <T> RetryPolicy<T> retryWithBackoffUntil(Duration timeout)
    {
        return RetryPolicies.<T>retryUntil(timeout)
                .withBackoff(100, 500, MILLIS, 1.1d);
    }

    static <T> RetryPolicy<T> retryUntil(Duration timeout)
    {
        requireNonNull(timeout, "timeout is nul");

        return new RetryPolicy<T>()
                .withMaxAttempts(-1)
                .withMaxDuration(timeout);
    }

    static <T> RetryPolicy<T> noRetries()
    {
        return new RetryPolicy<T>()
                .withMaxAttempts(1);
    }

    static <T> RetryPolicy<T> retries(int noOfRetries)
    {
        return new RetryPolicy<T>()
                .withMaxAttempts(1 + noOfRetries);
    }
}
