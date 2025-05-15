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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import io.trino.cost.LocalCostEstimate;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String type;
    private final Map<String, String> descriptor;
    private final List<Symbol> outputs;
    private final List<PlanNodeId> children;
    private final List<PlanNodeId> initialChildren;
    private final Optional<PlanNodeStats> stats;
    private final List<PlanNodeStatsEstimate> estimatedStats;
    private final List<PlanCostEstimate> estimatedCost;
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;
    private final Metrics splitSourceMetrics;

    private final ImmutableList.Builder<String> details = ImmutableList.builder();

    public NodeRepresentation(
            PlanNodeId id,
            String name,
            String type,
            Map<String, String> descriptor,
            List<Symbol> outputs,
            Optional<PlanNodeStats> stats,
            List<PlanNodeStatsEstimate> estimatedStats,
            List<PlanCostEstimate> estimatedCost,
            Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            Metrics splitSourceMetrics,
            List<PlanNodeId> children,
            // This is used in the case of adaptive plan node
            List<PlanNodeId> initialChildren)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedStats = requireNonNull(estimatedStats, "estimatedStats is null");
        this.estimatedCost = requireNonNull(estimatedCost, "estimatedCost is null");
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        this.splitSourceMetrics = requireNonNull(splitSourceMetrics, "splitSourceMetrics is null");
        this.children = requireNonNull(children, "children is null");
        this.initialChildren = requireNonNull(initialChildren, "initialChildren is null");

        checkArgument(estimatedCost.size() == estimatedStats.size(), "size of cost and stats list does not match");
    }

    @FormatMethod
    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.add(string);
        }
        else {
            details.add(format(string, args));
        }
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public Map<String, String> getDescriptor()
    {
        return descriptor;
    }

    public List<Symbol> getOutputs()
    {
        return outputs;
    }

    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    public List<PlanNodeId> getInitialChildren()
    {
        return initialChildren;
    }

    public List<String> getDetails()
    {
        return details.build();
    }

    public Optional<PlanNodeStats> getStats()
    {
        return stats;
    }

    public List<PlanNodeStatsEstimate> getEstimatedStats()
    {
        return estimatedStats;
    }

    public List<PlanCostEstimate> getEstimatedCost()
    {
        return estimatedCost;
    }

    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    public Metrics getSplitSourceMetrics()
    {
        return splitSourceMetrics;
    }

    public List<PlanNodeStatsAndCostSummary> getEstimates()
    {
        if (getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                getEstimatedCost().stream().allMatch(c -> c.getRootNodeLocalCostEstimate().equals(LocalCostEstimate.unknown()))) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<PlanNodeStatsAndCostSummary> estimates = ImmutableList.builder();
        for (int i = 0; i < getEstimatedStats().size(); i++) {
            PlanNodeStatsEstimate stats = getEstimatedStats().get(i);
            LocalCostEstimate cost = getEstimatedCost().get(i).getRootNodeLocalCostEstimate();

            estimates.add(new PlanNodeStatsAndCostSummary(
                    stats.getOutputRowCount(),
                    stats.getOutputSizeInBytes(getOutputs()),
                    cost.getCpuCost(),
                    cost.getMaxMemory(),
                    cost.getNetworkCost()));
        }

        return estimates.build();
    }
}
