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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SpillMetrics
{
    private static final String SPILL_TIME_METRIC_NAME = "Spill wall time (s)";
    @VisibleForTesting
    public static final String SPILL_COUNT_METRIC_NAME = "Spill count";
    @VisibleForTesting
    public static final String SPILL_DATA_SIZE = "Spill data size (MB)";
    private static final String UNSPILL_TIME_METRIC_NAME = "Unspill wall time (s)";
    private static final String UNSPILL_COUNT_METRIC_NAME = "Unspill count";
    private static final String UNSPILL_DATA_SIZE = "Unspill data size (MB)";

    private final String prefix;

    private final AtomicLong spillTimeNanos = new AtomicLong();
    private final AtomicLong spillCount = new AtomicLong();
    private final AtomicLong spillBytes = new AtomicLong();
    private final AtomicLong unspillTimeNanos = new AtomicLong();
    private final AtomicLong unspillCount = new AtomicLong();
    private final AtomicLong unspillBytes = new AtomicLong();

    public SpillMetrics()
    {
        this.prefix = "";
    }

    public SpillMetrics(String prefix)
    {
        this.prefix = requireNonNull(prefix, "prefix is null") + ": ";
    }

    public void recordSpillSince(long startNanos, long spillBytes)
    {
        spillTimeNanos.addAndGet(System.nanoTime() - startNanos);
        spillCount.incrementAndGet();
        this.spillBytes.addAndGet(spillBytes);
    }

    public void recordUnspillSince(long startNanos, long unspillBytes)
    {
        unspillTimeNanos.addAndGet(System.nanoTime() - startNanos);
        unspillCount.incrementAndGet();
        this.unspillBytes.addAndGet(unspillBytes);
    }

    public Metrics getMetrics()
    {
        ImmutableMap.Builder<String, Metric<?>> metricsBuilder = ImmutableMap.builder();
        if (spillTimeNanos.get() > 0 || spillCount.get() > 0 || spillBytes.get() > 0) {
            metricsBuilder.put(prefix + SPILL_TIME_METRIC_NAME, TDigestHistogram.fromValue(new Duration(spillTimeNanos.longValue(), NANOSECONDS).getValue(SECONDS)));
            metricsBuilder.put(prefix + SPILL_COUNT_METRIC_NAME, TDigestHistogram.fromValue(spillCount.doubleValue()));
            metricsBuilder.put(prefix + SPILL_DATA_SIZE, TDigestHistogram.fromValue(spillBytes.longValue() * (1.0d / MEGABYTE.inBytes())));
        }
        if (unspillTimeNanos.get() > 0 || unspillCount.get() > 0 || unspillBytes.get() > 0) {
            metricsBuilder.put(prefix + UNSPILL_TIME_METRIC_NAME, TDigestHistogram.fromValue(new Duration(unspillTimeNanos.longValue(), NANOSECONDS).getValue(SECONDS)));
            metricsBuilder.put(prefix + UNSPILL_COUNT_METRIC_NAME, TDigestHistogram.fromValue(unspillCount.doubleValue()));
            metricsBuilder.put(prefix + UNSPILL_DATA_SIZE, TDigestHistogram.fromValue(unspillBytes.longValue() * (1.0d / MEGABYTE.inBytes())));
        }
        return new Metrics(metricsBuilder.buildOrThrow());
    }
}
