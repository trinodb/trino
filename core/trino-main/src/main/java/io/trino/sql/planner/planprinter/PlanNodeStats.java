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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.Mergeable;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.util.MoreMaps.mergeMaps;
import static java.lang.Double.max;
import static java.lang.Math.sqrt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PlanNodeStats
        implements Mergeable<PlanNodeStats>
{
    protected final Map<String, BasicOperatorStats> operatorStats;
    private final PlanNodeId planNodeId;
    private final Duration planNodeScheduledTime;
    private final Duration planNodeCpuTime;
    private final long planNodeInputPositions;
    private final DataSize planNodeInputDataSize;
    private final long planNodeOutputPositions;
    private final DataSize planNodeOutputDataSize;
    private final DataSize planNodeSpilledDataSize;

    @JsonCreator
    PlanNodeStats(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("planNodeScheduledTime") Duration planNodeScheduledTime,
            @JsonProperty("planNodeCpuTime") Duration planNodeCpuTime,
            @JsonProperty("planNodeInputPositions") long planNodeInputPositions,
            @JsonProperty("planNodeInputDataSize") DataSize planNodeInputDataSize,
            @JsonProperty("planNodeOutputPositions") long planNodeOutputPositions,
            @JsonProperty("planNodeOutputDataSize") DataSize planNodeOutputDataSize,
            @JsonProperty("planNodeSpilledDataSize") DataSize planNodeSpilledDataSize,
            @JsonProperty("operatorStats") Map<String, BasicOperatorStats> operatorStats)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

        this.planNodeScheduledTime = requireNonNull(planNodeScheduledTime, "planNodeScheduledTime is null");
        this.planNodeCpuTime = requireNonNull(planNodeCpuTime, "planNodeCpuTime is null");
        this.planNodeInputPositions = planNodeInputPositions;
        this.planNodeInputDataSize = planNodeInputDataSize;
        this.planNodeOutputPositions = planNodeOutputPositions;
        this.planNodeOutputDataSize = planNodeOutputDataSize;
        this.planNodeSpilledDataSize = requireNonNull(planNodeSpilledDataSize, "planNodeSpilledDataSize is null");
        this.operatorStats = ImmutableMap.copyOf(requireNonNull(operatorStats, "operatorStats is null"));
    }

    private static double computedStdDev(double sumSquared, double sum, long n)
    {
        double average = sum / n;
        double variance = (sumSquared - 2 * sum * average + average * average * n) / n;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Duration getPlanNodeScheduledTime()
    {
        return planNodeScheduledTime;
    }

    @JsonProperty
    public Duration getPlanNodeCpuTime()
    {
        return planNodeCpuTime;
    }

    @JsonProperty
    public Set<String> getOperatorTypes()
    {
        return operatorStats.keySet();
    }

    @JsonProperty
    public long getPlanNodeInputPositions()
    {
        return planNodeInputPositions;
    }

    @JsonProperty
    public DataSize getPlanNodeInputDataSize()
    {
        return planNodeInputDataSize;
    }

    @JsonProperty
    public long getPlanNodeOutputPositions()
    {
        return planNodeOutputPositions;
    }

    @JsonProperty
    public DataSize getPlanNodeOutputDataSize()
    {
        return planNodeOutputDataSize;
    }

    @JsonProperty
    public DataSize getPlanNodeSpilledDataSize()
    {
        return planNodeSpilledDataSize;
    }

    public Map<String, Double> getOperatorInputPositionsAverages()
    {
        return operatorStats.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> (double) entry.getValue().getInputPositions() / operatorStats.get(entry.getKey()).getTotalDrivers()));
    }

    public Map<String, Double> getOperatorInputPositionsStdDevs()
    {
        return operatorStats.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> computedStdDev(
                                entry.getValue().getSumSquaredInputPositions(),
                                entry.getValue().getInputPositions(),
                                entry.getValue().getTotalDrivers())));
    }

    @JsonProperty
    public Map<String, BasicOperatorStats> getOperatorStats()
    {
        return operatorStats;
    }

    @Override
    public PlanNodeStats mergeWith(PlanNodeStats other)
    {
        checkArgument(planNodeId.equals(other.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeId, other.getPlanNodeId());

        long planNodeInputPositions = this.planNodeInputPositions + other.planNodeInputPositions;
        DataSize planNodeInputDataSize = succinctBytes(this.planNodeInputDataSize.toBytes() + other.planNodeInputDataSize.toBytes());
        long planNodeOutputPositions = this.planNodeOutputPositions + other.planNodeOutputPositions;
        DataSize planNodeOutputDataSize = succinctBytes(this.planNodeOutputDataSize.toBytes() + other.planNodeOutputDataSize.toBytes());

        Map<String, BasicOperatorStats> operatorStats = mergeMaps(this.operatorStats, other.operatorStats, BasicOperatorStats::merge);

        return new PlanNodeStats(
                planNodeId,
                new Duration(planNodeScheduledTime.toMillis() + other.getPlanNodeScheduledTime().toMillis(), MILLISECONDS),
                new Duration(planNodeCpuTime.toMillis() + other.getPlanNodeCpuTime().toMillis(), MILLISECONDS),
                planNodeInputPositions, planNodeInputDataSize,
                planNodeOutputPositions, planNodeOutputDataSize,
                succinctBytes(this.planNodeSpilledDataSize.toBytes() + other.planNodeSpilledDataSize.toBytes()),
                operatorStats);
    }
}
