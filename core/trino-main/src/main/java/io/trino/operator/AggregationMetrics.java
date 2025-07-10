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
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class AggregationMetrics
{
    @VisibleForTesting
    static final String INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME = "Input rows processed without partial aggregation enabled";
    private static final String ACCUMULATOR_TIME_METRIC_NAME = "Accumulator update CPU time";
    private static final String GROUP_BY_HASH_TIME_METRIC_NAME = "Group by hash update CPU time";

    private final SpillMetrics spillMetrics = new SpillMetrics();

    private long accumulatorTimeNanos;
    private long groupByHashTimeNanos;
    private long inputRowsProcessedWithPartialAggregationDisabled;

    public void recordAccumulatorUpdateTimeSince(long startNanos)
    {
        accumulatorTimeNanos += System.nanoTime() - startNanos;
    }

    public void recordGroupByHashUpdateTimeSince(long startNanos)
    {
        groupByHashTimeNanos += System.nanoTime() - startNanos;
    }

    public void recordInputRowsProcessedWithPartialAggregationDisabled(long rows)
    {
        inputRowsProcessedWithPartialAggregationDisabled += rows;
    }

    public void recordSpillSince(long startNanos, long spillBytes)
    {
        spillMetrics.recordSpillSince(startNanos, spillBytes);
    }

    public void recordUnspillSince(long startNanos, long unspillBytes)
    {
        spillMetrics.recordUnspillSince(startNanos, unspillBytes);
    }

    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.<String, Metric<?>>builder()
                .put(INPUT_ROWS_WITH_PARTIAL_AGGREGATION_DISABLED_METRIC_NAME, new LongCount(inputRowsProcessedWithPartialAggregationDisabled))
                .put(ACCUMULATOR_TIME_METRIC_NAME, new DurationTiming(new Duration(accumulatorTimeNanos, NANOSECONDS)))
                .put(GROUP_BY_HASH_TIME_METRIC_NAME, new DurationTiming(new Duration(groupByHashTimeNanos, NANOSECONDS)))
                .putAll(spillMetrics.getMetrics().getMetrics())
                .buildOrThrow());
    }
}
