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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.DistributedPlanStats;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.EstimateStats;
import static io.trino.sql.planner.planprinter.DistributedPlanRepresentation.JsonWindowOperatorStats;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsCpuCost;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsDataSize;
import static io.trino.sql.planner.planprinter.util.RendererUtils.formatAsLong;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JsonRenderer
        implements Renderer<String>
{
    private static final JsonCodec<JsonRenderedNode> CODEC = JsonCodec.jsonCodec(JsonRenderedNode.class);
    private static final JsonCodec<JsonDistributedPlanFragments> DISTRIBUTED_PLAN_CODEC
            = JsonCodec.jsonCodec(JsonDistributedPlanFragments.class);

    private static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        return CODEC.toJson(renderJson(plan, plan.getRoot()));
    }

    public String render(JsonDistributedPlanFragments planFragments)
    {
        return DISTRIBUTED_PLAN_CODEC.toJson(planFragments);
    }

    public Map<Integer, List<DistributedPlanRepresentation>> render(PlanRepresentation plan, boolean verbose, Integer level)
    {
        NodeRepresentation node = plan.getRoot();
        Map<Integer, List<DistributedPlanRepresentation>> distributedPlanRepresentationMap = new HashMap<>();
        distributedPlanRepresentationMap.computeIfAbsent(level, i -> new ArrayList<>()).add(getPlanRepresentation(plan, node, verbose, level));
        return distributedPlanRepresentationMap;
    }

    private DistributedPlanRepresentation getPlanRepresentation(PlanRepresentation plan, NodeRepresentation node, Boolean verbose, Integer level)
    {
        Optional<PlanNodeStatsAndCostSummary> planNodeStatsAndCostSummary = getReorderJoinStatsAndCost(node, verbose);
        List<EstimateStats> estimateStats = getEstimates(plan, node);
        DistributedPlanStats distributedPlanStats = getStats(plan, node);
        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
        List<DistributedPlanRepresentation> childrenRepresentation = new ArrayList<>();
        for (NodeRepresentation child : children) {
            childrenRepresentation.add(getPlanRepresentation(plan, child, verbose, level + 1));
        }

        Optional<PlanNodeStats> nodeStats = node.getStats();
        Optional<JsonWindowOperatorStats> jsonWindowOperatorStats = Optional.empty();
        if (nodeStats.isPresent() && (nodeStats.get() instanceof WindowPlanNodeStats) && verbose) {
            WindowOperatorStats windowOperatorStats = ((WindowPlanNodeStats) nodeStats.get()).getWindowOperatorStats();
            jsonWindowOperatorStats = Optional.of(new JsonWindowOperatorStats(
                    windowOperatorStats.getActiveDrivers(),
                    windowOperatorStats.getTotalDrivers(),
                    windowOperatorStats.getIndexSizeStdDev(),
                    windowOperatorStats.getIndexPositionsStdDev(),
                    windowOperatorStats.getIndexCountPerDriverStdDev(),
                    windowOperatorStats.getRowsPerDriverStdDev(),
                    windowOperatorStats.getPartitionRowsStdDev()));
        }

        return new DistributedPlanRepresentation(
                node.getName(),
                node.getIdentifier(),
                node.getOutputs(),
                node.getDetails(),
                planNodeStatsAndCostSummary,
                estimateStats,
                distributedPlanStats,
                childrenRepresentation,
                jsonWindowOperatorStats);
    }

    private List<EstimateStats> getEstimates(PlanRepresentation plan, NodeRepresentation node)
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

    private Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost(NodeRepresentation node, boolean verbose)
    {
        if (verbose && node.getReorderJoinStatsAndCost().isPresent()) {
            return node.getReorderJoinStatsAndCost();
        }
        return Optional.empty();
    }

    private DistributedPlanStats getStats(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getStats().isEmpty() || !(plan.getTotalCpuTime().isPresent() && plan.getTotalScheduledTime().isPresent())) {
            return null;
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
                    return new DistributedPlanStats(
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
                            nodeStats.getPlanNodeSpilledDataSize());
                }
                else {
                    return new DistributedPlanStats(
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
                            nodeStats.getPlanNodeSpilledDataSize());
                }
            }
            else {
                return new DistributedPlanStats(
                        nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                        cpuTimeFraction,
                        nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                        scheduledTimeFraction,
                        nodeStats.getPlanNodeOutputPositions(),
                        nodeStats.getPlanNodeOutputDataSize(),
                        translatedOperatorType,
                        inputAverage,
                        100.0d * inputStdDevs.get(operator) / inputAverage,
                        nodeStats.getPlanNodeSpilledDataSize());
            }
        }
        return null;
    }

    private JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node.getId().toString(),
                node.getName(),
                node.getIdentifier(),
                node.getDetails(),
                children,
                node.getRemoteSources().stream()
                        .map(PlanFragmentId::toString)
                        .collect(toImmutableList()));
    }

    public static class JsonRenderedNode
    {
        private final String id;
        private final String name;
        private final String identifier;
        private final String details;
        private final List<JsonRenderedNode> children;
        private final List<String> remoteSources;

        public JsonRenderedNode(String id, String name, String identifier, String details, List<JsonRenderedNode> children, List<String> remoteSources)
        {
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.identifier = requireNonNull(identifier, "identifier is null");
            this.details = requireNonNull(details, "details is null");
            this.children = requireNonNull(children, "children is null");
            this.remoteSources = requireNonNull(remoteSources, "remoteSources is null");
        }

        @JsonProperty
        public String getId()
        {
            return id;
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
        public String getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }

        @JsonProperty
        public List<String> getRemoteSources()
        {
            return remoteSources;
        }
    }
}
