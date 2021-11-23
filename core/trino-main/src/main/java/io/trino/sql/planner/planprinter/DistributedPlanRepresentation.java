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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DistributedPlanRepresentation
{
    private final String name;
    private final String identifier;
    private final List<TypedSymbol> outputs;
    private final String details;
    private final DistributedPlanStats distributedPlanStats;
    private final Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary;
    private final List<EstimateStats> estimateStats;
    private final List<DistributedPlanRepresentation> children;
    private final Optional<JsonWindowOperatorStats> windowOperatorStats;

    @JsonCreator
    public DistributedPlanRepresentation(
            @JsonProperty("name") String name,
            @JsonProperty("identifier") String identifier,
            @JsonProperty("layout") List<TypedSymbol> outputs,
            @JsonProperty("details") String details,
            @JsonProperty("reOrderJoinStatsAndCosts") Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary,
            @JsonProperty("estimates") List<EstimateStats> estimateStats,
            @JsonProperty("planStats") DistributedPlanStats distributedPlanStats,
            @JsonProperty("children") List<DistributedPlanRepresentation> children,
            @JsonProperty("windowOperatorStats") Optional<JsonWindowOperatorStats> windowOperatorStats)
    {
        this.name = requireNonNull(name, "name is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.details = requireNonNull(details, "details is null");
        this.planNodeStatsAndCostSummary = planNodeStatsAndCostSummary;
        this.estimateStats = estimateStats;
        this.distributedPlanStats = distributedPlanStats;
        this.children = children;
        this.windowOperatorStats = windowOperatorStats;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getIdentifier()
    {
        return identifier;
    }

    @JsonProperty
    public List<TypedSymbol> getOutputs()
    {
        return outputs;
    }

    @JsonProperty
    public String getDetails()
    {
        return details;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getPlanNodeStatsAndCostSummary()
    {
        return planNodeStatsAndCostSummary;
    }

    @JsonProperty
    public List<EstimateStats> getEstimateStats()
    {
        return estimateStats;
    }

    @JsonProperty
    public DistributedPlanStats getDistributedPlanStats()
    {
        return distributedPlanStats;
    }

    @JsonProperty
    public List<DistributedPlanRepresentation> getChildren()
    {
        return children;
    }

    @JsonProperty
    public Optional<JsonWindowOperatorStats> getWindowOperatorStats()
    {
        return windowOperatorStats;
    }

    @Immutable
    public static class JsonWindowOperatorStats
    {
        private final int activeDrivers;
        private final int totalDrivers;
        private final double indexSizeStdDev;
        private final double indexPositionsStdDev;
        private final double indexCountPerDriverStdDev;
        private final double rowsPerDriverStdDev;
        private final double partitionRowsStdDev;

        @JsonCreator
        public JsonWindowOperatorStats(
                @JsonProperty("activeDrivers") int activeDrivers,
                @JsonProperty("totalDrivers") int totalDrivers,
                @JsonProperty("indexSizeStdDev") double indexSizeStdDev,
                @JsonProperty("indexPositionsStdDev") double indexPositionsStdDev,
                @JsonProperty("indexCountPerDriverStdDev") double indexCountPerDriverStdDev,
                @JsonProperty("rowsPerDriverStdDev") double rowsPerDriverStdDev,
                @JsonProperty("partitionRowsStdDev") double partitionRowsStdDev)
        {
            this.activeDrivers = activeDrivers;
            this.totalDrivers = totalDrivers;
            this.indexSizeStdDev = indexSizeStdDev;
            this.indexPositionsStdDev = indexPositionsStdDev;
            this.indexCountPerDriverStdDev = indexCountPerDriverStdDev;
            this.rowsPerDriverStdDev = rowsPerDriverStdDev;
            this.partitionRowsStdDev = partitionRowsStdDev;
        }

        @JsonProperty
        public int getActiveDrivers()
        {
            return activeDrivers;
        }

        @JsonProperty
        public int getTotalDrivers()
        {
            return totalDrivers;
        }

        @JsonProperty
        public double getIndexSizeStdDev()
        {
            return indexSizeStdDev;
        }

        @JsonProperty
        public double getIndexPositionsStdDev()
        {
            return indexPositionsStdDev;
        }

        @JsonProperty
        public double getIndexCountPerDriverStdDev()
        {
            return indexCountPerDriverStdDev;
        }

        @JsonProperty
        public double getRowsPerDriverStdDev()
        {
            return rowsPerDriverStdDev;
        }

        @JsonProperty
        public double getPartitionRowsStdDev()
        {
            return partitionRowsStdDev;
        }
    }

    @Immutable
    public static class EstimateStats
    {
        private final String outputRowCount;
        private final String outputSize;
        private final String cpuCost;
        private final String maxMemory;
        private final String networkCost;

        @JsonCreator
        public EstimateStats(
                @JsonProperty("outputRowCount") String outputRowCount,
                @JsonProperty("outputSize") String outputSize,
                @JsonProperty("cpuCost") String cpuCost,
                @JsonProperty("maxMemory") String maxMemory,
                @JsonProperty("networkCost") String networkCost)
        {
            this.outputRowCount = outputRowCount;
            this.outputSize = outputSize;
            this.cpuCost = cpuCost;
            this.maxMemory = maxMemory;
            this.networkCost = networkCost;
        }

        @JsonProperty
        public String getOutputRowCount()
        {
            return outputRowCount;
        }

        @JsonProperty
        public String getOutputSize()
        {
            return outputSize;
        }

        @JsonProperty
        public String getCpuCost()
        {
            return cpuCost;
        }

        @JsonProperty
        public String getMaxMemory()
        {
            return maxMemory;
        }

        @JsonProperty
        public String getNetworkCost()
        {
            return networkCost;
        }
    }

    @Immutable
    public static class DistributedPlanStats
    {
        private final Duration nodeCpuTime;
        private final Double nodeCpuFraction;
        private final Duration nodeScheduledTime;
        private final Double nodeScheduledFraction;
        private final long nodeOutputRows;
        private final DataSize nodeOutputDataSize;
        private final String translatedOperatorType;
        private final Double nodeInputRows;
        private final Double nodeInputStdDev;
        private final Double averageHashCollisions;
        private final Double hashCollisionsRatio;
        private final Double stdDevHashCollisions;
        private final DataSize planNodeSpilledDataSize;

        @JsonCreator
        public DistributedPlanStats(
                @JsonProperty("nodeCpuTime") Duration nodeCpuTime,
                @JsonProperty("nodeCpuFraction") Double nodeCpuFraction,
                @JsonProperty("nodeScheduledTime") Duration nodeScheduledTime,
                @JsonProperty("nodeScheduledFraction") Double nodeScheduledFraction,
                @JsonProperty("nodeOutputRows") Long nodeOutputRows,
                @JsonProperty("nodeOutputDataSize") DataSize nodeOutputDataSize,
                @JsonProperty("translatedOperatorType") String translatedOperatorType,
                @JsonProperty("nodeInputRows") Double nodeInputRows,
                @JsonProperty("nodeInputStdDev") Double nodeInputStdDev,
                @JsonProperty("averageHashCollisions") Double averageHashCollisions,
                @JsonProperty("hashCollisionsRatio") Double hashCollisionsRatio,
                @JsonProperty("stdDevHashCollisions") Double stdDevHashCollisions,
                @JsonProperty("planNodeSpilledDataSize") DataSize planNodeSpilledDataSize)
        {
            this.nodeCpuTime = nodeCpuTime;
            this.nodeCpuFraction = nodeCpuFraction;
            this.nodeScheduledTime = nodeScheduledTime;
            this.nodeScheduledFraction = nodeScheduledFraction;
            this.nodeOutputRows = nodeOutputRows;
            this.nodeOutputDataSize = nodeOutputDataSize;
            this.translatedOperatorType = translatedOperatorType;
            this.nodeInputRows = nodeInputRows;
            this.nodeInputStdDev = nodeInputStdDev;
            this.averageHashCollisions = averageHashCollisions;
            this.hashCollisionsRatio = hashCollisionsRatio;
            this.stdDevHashCollisions = stdDevHashCollisions;
            this.planNodeSpilledDataSize = planNodeSpilledDataSize;
        }

        @JsonCreator
        public DistributedPlanStats(
                @JsonProperty("nodeCpuTime") Duration nodeCpuTime,
                @JsonProperty("nodeCpuFraction") Double nodeCpuFraction,
                @JsonProperty("nodeScheduledTime") Duration nodeScheduledTime,
                @JsonProperty("nodeScheduledFraction") Double nodeScheduledFraction,
                @JsonProperty("nodeOutputRows") Long nodeOutputRows,
                @JsonProperty("nodeOutputDataSize") DataSize nodeOutputDataSize,
                @JsonProperty("translatedOperatorType") String translatedOperatorType,
                @JsonProperty("nodeInputRows") Double nodeInputRows,
                @JsonProperty("nodeInputStdDev") Double nodeInputStdDev,
                @JsonProperty("planNodeSpilledDataSize") DataSize planNodeSpilledDataSize)
        {
            this(nodeCpuTime, nodeCpuFraction, nodeScheduledTime, nodeScheduledFraction, nodeOutputRows, nodeOutputDataSize, translatedOperatorType,
                    nodeInputRows, nodeInputStdDev, 0.0, 0.0, 0.0, planNodeSpilledDataSize);
        }

        @JsonProperty
        public Duration getNodeCpuTime()
        {
            return nodeCpuTime;
        }

        @JsonProperty
        public Double getNodeCpuFraction()
        {
            return nodeCpuFraction;
        }

        @JsonProperty
        public Duration getNodeScheduledTime()
        {
            return nodeScheduledTime;
        }

        @JsonProperty
        public Double getNodeScheduledFraction()
        {
            return nodeScheduledFraction;
        }

        @JsonProperty
        public Long getNodeOutputRows()
        {
            return nodeOutputRows;
        }

        @JsonProperty
        public DataSize getNodeOutputDataSize()
        {
            return nodeOutputDataSize;
        }

        @JsonProperty
        public String getTranslatedOperatorType()
        {
            return translatedOperatorType;
        }

        @JsonProperty
        public Double getNodeInputRows()
        {
            return nodeInputRows;
        }

        @JsonProperty
        public Double getNodeInputStdDev()
        {
            return nodeInputStdDev;
        }

        @JsonProperty
        public Double getAverageHashCollisions()
        {
            return averageHashCollisions;
        }

        @JsonProperty
        public Double getHashCollisionsRatio()
        {
            return hashCollisionsRatio;
        }

        @JsonProperty
        public Double getStdDevHashCollisions()
        {
            return stdDevHashCollisions;
        }

        @JsonProperty
        public DataSize getPlanNodeSpilledDataSize()
        {
            return planNodeSpilledDataSize;
        }
    }
}
