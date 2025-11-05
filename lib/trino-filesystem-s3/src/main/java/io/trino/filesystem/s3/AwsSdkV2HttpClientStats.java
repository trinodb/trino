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
package io.trino.filesystem.s3;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.metrics.SdkMetric;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.http.HttpMetric.AVAILABLE_CONCURRENCY;
import static software.amazon.awssdk.http.HttpMetric.LEASED_CONCURRENCY;
import static software.amazon.awssdk.http.HttpMetric.PENDING_CONCURRENCY_ACQUIRES;

@ThreadSafe
public class AwsSdkV2HttpClientStats
{
    private final TimeStat connectionAcquireLatency = new TimeStat(MILLISECONDS);
    private final AtomicLong availableConcurrency = new AtomicLong();
    private final AtomicLong leasedConcurrency = new AtomicLong();
    private final AtomicLong pendingConcurrencyAcquires = new AtomicLong();

    @Managed
    @Nested
    public TimeStat getConnectionAcquireLatency()
    {
        return connectionAcquireLatency;
    }

    @Managed
    public long getAvailableConcurrency()
    {
        return availableConcurrency.get();
    }

    @Managed
    public long getLeasedConcurrency()
    {
        return leasedConcurrency.get();
    }

    @Managed
    public long getPendingConcurrencyAcquires()
    {
        return pendingConcurrencyAcquires.get();
    }

    public void updateConcurrencyStats(SdkMetric<?> metric, int value)
    {
        if (metric.equals(AVAILABLE_CONCURRENCY)) {
            availableConcurrency.set(value);
        }
        else if (metric.equals(PENDING_CONCURRENCY_ACQUIRES)) {
            pendingConcurrencyAcquires.set(value);
        }
        else if (metric.equals(LEASED_CONCURRENCY)) {
            leasedConcurrency.set(value);
        }
    }

    public void updateConcurrencyAcquireDuration(Duration duration)
    {
        connectionAcquireLatency.addNanos(duration.toNanos());
    }
}
