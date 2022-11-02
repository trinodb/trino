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

package io.trino.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.trino.execution.StageInfo;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StatsAndCosts
{
    private static final StatsAndCosts EMPTY = new StatsAndCosts(ImmutableMap.of(), ImmutableMap.of());

    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;
    private final Map<PlanNodeId, PlanCostEstimate> costs;

    public static StatsAndCosts empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public StatsAndCosts(
            @JsonProperty("stats") Map<PlanNodeId, PlanNodeStatsEstimate> stats,
            @JsonProperty("costs") Map<PlanNodeId, PlanCostEstimate> costs)
    {
        this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
        this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeStatsEstimate> getStats()
    {
        return stats;
    }

    @JsonProperty
    public Map<PlanNodeId, PlanCostEstimate> getCosts()
    {
        return costs;
    }

    public StatsAndCosts getForSubplan(PlanNode root)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> filteredStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> filteredCosts = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            if (stats.containsKey(node.getId())) {
                filteredStats.put(node.getId(), stats.get(node.getId()));
            }
            if (costs.containsKey(node.getId())) {
                filteredCosts.put(node.getId(), costs.get(node.getId()));
            }
        }
        return new StatsAndCosts(filteredStats.buildOrThrow(), filteredCosts.buildOrThrow());
    }

    public static StatsAndCosts create(PlanNode root, StatsProvider statsProvider, CostProvider costProvider)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> stats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> costs = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            stats.put(node.getId(), statsProvider.getStats(node));
            costs.put(node.getId(), costProvider.getCost(node));
        }
        return new StatsAndCosts(stats.buildOrThrow(), costs.buildOrThrow());
    }

    public static StatsAndCosts create(StageInfo stageInfo)
    {
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> planNodeStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> planNodeCosts = ImmutableMap.builder();
        reconstructStatsAndCosts(stageInfo, planNodeStats, planNodeCosts);
        return new StatsAndCosts(planNodeStats.buildOrThrow(), planNodeCosts.buildOrThrow());
    }

    private static void reconstructStatsAndCosts(
            StageInfo stage,
            ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> planNodeStats,
            ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> planNodeCosts)
    {
        PlanFragment planFragment = stage.getPlan();
        if (planFragment != null) {
            planNodeStats.putAll(planFragment.getStatsAndCosts().getStats());
            planNodeCosts.putAll(planFragment.getStatsAndCosts().getCosts());
        }
        for (StageInfo subStage : stage.getSubStages()) {
            reconstructStatsAndCosts(subStage, planNodeStats, planNodeCosts);
        }
    }
}
