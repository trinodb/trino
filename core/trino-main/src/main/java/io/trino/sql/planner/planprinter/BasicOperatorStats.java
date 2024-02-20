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
package io.trino.sql.planner.planprinter;

import io.trino.spi.metrics.Metrics;

import java.util.List;

import static java.util.Objects.requireNonNull;

class BasicOperatorStats
{
    private final long totalDrivers;
    private final long inputPositions;
    private final double sumSquaredInputPositions;
    private final Metrics metrics;
    private final Metrics connectorMetrics;

    public BasicOperatorStats(
            long totalDrivers,
            long inputPositions,
            double sumSquaredInputPositions,
            Metrics metrics,
            Metrics connectorMetrics)
    {
        this.totalDrivers = totalDrivers;
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.connectorMetrics = requireNonNull(connectorMetrics, "connectorMetrics is null");
    }

    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    public long getInputPositions()
    {
        return inputPositions;
    }

    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    public Metrics getMetrics()
    {
        return metrics;
    }

    public Metrics getConnectorMetrics()
    {
        return connectorMetrics;
    }

    public static BasicOperatorStats merge(BasicOperatorStats first, BasicOperatorStats second)
    {
        return new BasicOperatorStats(
                first.totalDrivers + second.totalDrivers,
                first.inputPositions + second.inputPositions,
                first.sumSquaredInputPositions + second.sumSquaredInputPositions,
                first.metrics.mergeWith(second.metrics),
                first.connectorMetrics.mergeWith(second.connectorMetrics));
    }

    public static BasicOperatorStats merge(List<BasicOperatorStats> operatorStats)
    {
        long totalDrivers = 0;
        long inputPositions = 0;
        double sumSquaredInputPositions = 0;
        Metrics.Accumulator metricsAccumulator = Metrics.accumulator();
        Metrics.Accumulator connectorMetricsAccumulator = Metrics.accumulator();
        for (BasicOperatorStats stats : operatorStats) {
            totalDrivers += stats.totalDrivers;
            inputPositions += stats.inputPositions;
            sumSquaredInputPositions += stats.sumSquaredInputPositions;
            metricsAccumulator.add(stats.metrics);
            connectorMetricsAccumulator.add(stats.connectorMetrics);
        }
        return new BasicOperatorStats(totalDrivers, inputPositions, sumSquaredInputPositions, metricsAccumulator.get(), connectorMetricsAccumulator.get());
    }
}
