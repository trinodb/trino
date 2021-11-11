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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.planprinter.RendererUtils.formatAsCpuCost;
import static io.trino.sql.planner.planprinter.RendererUtils.formatAsDataSize;
import static io.trino.sql.planner.planprinter.RendererUtils.formatAsLong;
import static io.trino.sql.planner.planprinter.RendererUtils.translateOperatorTypes;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DistributedFragmentRepresentation
{
    private final String name;
    private final String identifier;
    private final List<TypedSymbol> outputs;
    private final String details;
    private final Optional<DistributedFragmentStats> stats;
    private final Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary;
    private final List<EstimateStats> estimates;
    private final List<DistributedFragmentRepresentation> children;
    private final Optional<WindowOperatorStatsRepresentation> windowOperatorStats;
    private final NodeRepresentation node;

    @JsonCreator
    public DistributedFragmentRepresentation(
            @JsonProperty("name") String name,
            @JsonProperty("identifier") String identifier,
            @JsonProperty("layout") List<TypedSymbol> outputs,
            @JsonProperty("details") String details,
            @JsonProperty("cost") Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary,
            @JsonProperty("estimates") List<EstimateStats> estimates,
            @JsonProperty("stats") Optional<DistributedFragmentStats> stats,
            @JsonProperty("children") List<DistributedFragmentRepresentation> children,
            @JsonProperty("windowOperatorStats") Optional<WindowOperatorStatsRepresentation> windowOperatorStats,
            @JsonProperty("node") NodeRepresentation node)
    {
        this.name = requireNonNull(name, "name is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.details = requireNonNull(details, "details is null");
        this.planNodeStatsAndCostSummary = requireNonNull(planNodeStatsAndCostSummary, "planNodeStatsAndCostSummary is null");
        this.estimates = requireNonNull(estimates, "estimates is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.children = requireNonNull(children, "children is null");
        this.windowOperatorStats = requireNonNull(windowOperatorStats, "windowOperatorStats is null");
        this.node = requireNonNull(node, "node is null");
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
    public List<EstimateStats> getEstimates()
    {
        return estimates;
    }

    @JsonProperty
    public Optional<DistributedFragmentStats> getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<DistributedFragmentRepresentation> getChildren()
    {
        return children;
    }

    @JsonProperty
    public Optional<WindowOperatorStatsRepresentation> getWindowOperatorStats()
    {
        return windowOperatorStats;
    }

    @JsonIgnore
    public NodeRepresentation getNode()
    {
        return node;
    }

    public static DistributedFragmentRepresentation getFragmentRepresentation(PlanRepresentation plan, NodeRepresentation node, boolean verbose, int level)
    {
        Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary = getReorderJoinStatsAndCost(node, verbose);
        List<EstimateStats> estimateStats = getEstimates(plan, node);
        Optional<DistributedFragmentStats> distributedFragmentStats = getStats(plan, node);
        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
        List<DistributedFragmentRepresentation> childrenRepresentation = new ArrayList<>();
        for (NodeRepresentation child : children) {
            childrenRepresentation.add(getFragmentRepresentation(plan, child, verbose, level + 1));
        }

        Optional<PlanNodeStats> nodeStats = node.getStats();
        Optional<WindowOperatorStatsRepresentation> windowOperatorStatsRepresentation = Optional.empty();
        if (nodeStats.isPresent() && (nodeStats.get() instanceof WindowPlanNodeStats) && verbose) {
            WindowOperatorStats windowOperatorStats = ((WindowPlanNodeStats) nodeStats.get()).getWindowOperatorStats();
            windowOperatorStatsRepresentation = Optional.of(new WindowOperatorStatsRepresentation(windowOperatorStats));
        }

        return new DistributedFragmentRepresentation(
                node.getName(),
                node.getIdentifier(),
                node.getOutputs(),
                node.getDetails(),
                planNodeStatsAndCostSummary,
                estimateStats,
                distributedFragmentStats,
                childrenRepresentation,
                windowOperatorStatsRepresentation,
                node);
    }

    private static List<EstimateStats> getEstimates(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                node.getEstimatedCost().stream().allMatch(c -> c.equals(PlanCostEstimate.unknown()))) {
            return new ArrayList<>();
        }

        List<EstimateStats> estimateStats = new ArrayList<>();
        int estimateCount = node.getEstimatedStats().size();

        for (int i = 0; i < estimateCount; i++) {
            PlanNodeStatsEstimate stats = node.getEstimatedStats().get(i);
            PlanCostEstimate cost = node.getEstimatedCost().get(i);

            List<Symbol> outputSymbols = node.getOutputs().stream()
                    .map(NodeRepresentation.TypedSymbol::getSymbol)
                    .collect(toList());

            estimateStats.add(new EstimateStats(
                    formatAsLong(stats.getOutputRowCount()),
                    formatAsDataSize(stats.getOutputSizeInBytes(outputSymbols, plan.getTypes())),
                    formatAsCpuCost(cost.getCpuCost()),
                    formatAsDataSize(cost.getMaxMemory()),
                    formatAsDataSize(cost.getNetworkCost())));
        }

        return estimateStats;
    }

    private static Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost(NodeRepresentation node, boolean verbose)
    {
        if (verbose && node.getReorderJoinStatsAndCost().isPresent()) {
            return node.getReorderJoinStatsAndCost();
        }
        return Optional.empty();
    }

    private static Optional<DistributedFragmentStats> getStats(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getStats().isEmpty() || !(plan.getTotalCpuTime().isPresent() && plan.getTotalScheduledTime().isPresent())) {
            return Optional.empty();
        }

        PlanNodeStats nodeStats = node.getStats().get();

        double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / plan.getTotalScheduledTime().get().toMillis();
        double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / plan.getTotalCpuTime().get().toMillis();

        Map<String, Double> inputAverages = nodeStats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = nodeStats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = emptyMap();
        Map<String, Double> hashCollisionsStdDevs = emptyMap();
        Map<String, Double> expectedHashCollisionsAverages = emptyMap();
        if (nodeStats instanceof HashCollisionPlanNodeStats) {
            hashCollisionsAverages = ((HashCollisionPlanNodeStats) nodeStats).getOperatorHashCollisionsAverages();
            hashCollisionsStdDevs = ((HashCollisionPlanNodeStats) nodeStats).getOperatorHashCollisionsStdDevs();
            expectedHashCollisionsAverages = ((HashCollisionPlanNodeStats) nodeStats).getOperatorExpectedCollisionsAverages();
        }

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(nodeStats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    return Optional.of(new DistributedFragmentStats(
                            nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                            cpuTimeFraction,
                            nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                            scheduledTimeFraction,
                            nodeStats.getPlanNodeOutputPositions(),
                            nodeStats.getPlanNodeOutputDataSize(),
                            translatedOperatorType,
                            inputAverage,
                            100.0d * inputStdDevs.get(operator) / inputAverage,
                            hashCollisionsAverage,
                            hashCollisionsRatio * 100.d,
                            hashCollisionsStdDevRatio * 100.d,
                            nodeStats.getPlanNodeSpilledDataSize()));
                }
                else {
                    return Optional.of(new DistributedFragmentStats(
                            nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                            cpuTimeFraction,
                            nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                            scheduledTimeFraction,
                            nodeStats.getPlanNodeOutputPositions(),
                            nodeStats.getPlanNodeOutputDataSize(),
                            translatedOperatorType,
                            inputAverage,
                            100.0d * inputStdDevs.get(operator) / inputAverage,
                            hashCollisionsAverage,
                            0.0,
                            hashCollisionsStdDevRatio * 100.d,
                            nodeStats.getPlanNodeSpilledDataSize()));
                }
            }
            else {
                return Optional.of(new DistributedFragmentStats(
                        nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                        cpuTimeFraction,
                        nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                        scheduledTimeFraction,
                        nodeStats.getPlanNodeOutputPositions(),
                        nodeStats.getPlanNodeOutputDataSize(),
                        translatedOperatorType,
                        inputAverage,
                        100.0d * inputStdDevs.get(operator) / inputAverage,
                        nodeStats.getPlanNodeSpilledDataSize()));
            }
        }
        return Optional.empty();
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
    public static class DistributedFragmentStats
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
        public DistributedFragmentStats(
                @JsonProperty("nodeCpuTime") Duration nodeCpuTime,
                @JsonProperty("nodeCpuFraction") Double nodeCpuFraction,
                @JsonProperty("nodeScheduledTime") Duration nodeScheduledTime,
                @JsonProperty("nodeScheduledFraction") Double nodeScheduledFraction,
                @JsonProperty("nodeOutputRows") long nodeOutputRows,
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
        public DistributedFragmentStats(
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
