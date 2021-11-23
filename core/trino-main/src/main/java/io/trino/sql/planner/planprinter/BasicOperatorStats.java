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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.metrics.Metrics;

import static java.util.Objects.requireNonNull;

public class BasicOperatorStats
{
    private final long totalDrivers;
    private final long inputPositions;
    private final double sumSquaredInputPositions;
    private final Metrics metrics;
    private final Metrics connectorMetrics;

    @JsonCreator
    public BasicOperatorStats(
            @JsonProperty("totalDrivers") long totalDrivers,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,
            @JsonProperty("metrics") Metrics metrics,
            @JsonProperty("connectorMetrics") Metrics connectorMetrics)
    {
        this.totalDrivers = totalDrivers;
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.connectorMetrics = requireNonNull(connectorMetrics, "connectorMetrics is null");
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

    @JsonProperty
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    @JsonProperty
    public Metrics getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public Metrics getConnectorMetrics()
    {
        return connectorMetrics;
    }
}
