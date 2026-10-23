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
package io.trino.plugin.base.metrics;

import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.TDigest;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Records the time and failure count of named API calls, both per call name and across all calls.
 */
@ThreadSafe
public class ApiCallStats
{
    private final CallStats allCallsStats = new CallStats();
    private final Map<String, CallStats> callStats = new ConcurrentHashMap<>();
    private final Ticker ticker;

    public ApiCallStats()
    {
        this(Ticker.systemTicker());
    }

    public ApiCallStats(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public void measure(String callName, Runnable call)
    {
        measure(callName, () -> {
            call.run();
            return null;
        });
    }

    public <T> T measure(String callName, Callable<T> call)
    {
        return measureCall(callName, call, _ -> false);
    }

    /**
     * Exceptions of type {@code expectedException} are rethrown but not counted as failures.
     */
    public <T> T measure(String callName, Callable<T> call, Class<? extends Exception> expectedException)
    {
        return measureCall(callName, call, expectedException::isInstance);
    }

    private <T> T measureCall(String callName, Callable<T> call, Predicate<Exception> isExpectedException)
    {
        CallStats stats = callStats.computeIfAbsent(callName, _ -> new CallStats());
        long start = ticker.read();
        try {
            return call.call();
        }
        catch (Exception e) {
            if (!isExpectedException.test(e)) {
                stats.recordFailure();
                allCallsStats.recordFailure();
            }
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            long timeNanos = ticker.read() - start;
            stats.record(timeNanos);
            allCallsStats.record(timeNanos);
        }
    }

    /**
     * Emits {@code <prefix>.all.*} for the aggregate and {@code <prefix>.<callName>.*} for each call name.
     * Returns {@link Metrics#EMPTY} when no call was recorded.
     */
    public Metrics toMetrics(String prefix)
    {
        if (callStats.isEmpty()) {
            return Metrics.EMPTY;
        }
        ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.builder();
        allCallsStats.storeTo(metrics, prefix + ".all");
        for (Entry<String, CallStats> entry : callStats.entrySet()) {
            entry.getValue().storeTo(metrics, prefix + "." + entry.getKey());
        }
        return new Metrics(metrics.buildOrThrow());
    }

    @ThreadSafe
    private static class CallStats
    {
        private final TDigest timeNanosDistribution = new TDigest();
        private long totalTimeNanos;
        private long totalFailures;

        synchronized void record(long nanos)
        {
            timeNanosDistribution.add((double) nanos);
            totalTimeNanos += nanos;
        }

        synchronized void recordFailure()
        {
            totalFailures++;
        }

        synchronized void storeTo(ImmutableMap.Builder<String, Metric<?>> metrics, String prefix)
        {
            // DistributionSnapshot does not retain reference to the histogram
            metrics.put(prefix + ".time.distribution", DistributionSnapshot.fromDistribution(new TDigestHistogram(timeNanosDistribution)));
            metrics.put(prefix + ".time.total", new LongCount(totalTimeNanos));

            // do not add redundant 0 failures to make the metrics more concise
            if (totalFailures > 0) {
                metrics.put(prefix + ".failures", new LongCount(totalFailures));
            }
        }
    }
}
