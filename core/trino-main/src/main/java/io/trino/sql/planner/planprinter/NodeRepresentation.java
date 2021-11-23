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
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String type;
    private final String identifier;
    private final List<TypedSymbol> outputs;
    private final List<PlanNodeId> children;
    private final List<PlanFragmentId> remoteSources;
    private final Optional<PlanNodeStats> stats;
    private final List<PlanNodeStatsEstimate> estimatedStats;
    private final List<PlanCostEstimate> estimatedCost;
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;
    private final List<NodeRepresentation> childrenNodeRepresentation;

    private final StringBuilder details = new StringBuilder();

    @JsonCreator
    public NodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("identifier") String identifier,
            @JsonProperty("outputs") List<TypedSymbol> outputs,
            @JsonProperty("stats") Optional<PlanNodeStats> stats,
            @JsonProperty("estimatedStats") List<PlanNodeStatsEstimate> estimatedStats,
            @JsonProperty("estimatedCost") List<PlanCostEstimate> estimatedCost,
            @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            @JsonProperty("children") List<PlanNodeId> children,
            @JsonProperty("remoteSources") List<PlanFragmentId> remoteSources,
            @JsonProperty("childrenNodeRepresentation") List<NodeRepresentation> childrenNodeRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedStats = requireNonNull(estimatedStats, "estimatedStats is null");
        this.estimatedCost = requireNonNull(estimatedCost, "estimatedCost is null");
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        this.children = requireNonNull(children, "children is null");
        this.remoteSources = requireNonNull(remoteSources, "remoteSources is null");
        this.childrenNodeRepresentation = requireNonNull(childrenNodeRepresentation, "childrenNodeRepresentation is null");

        checkArgument(estimatedCost.size() == estimatedStats.size(), "size of cost and stats list does not match");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.append(string);
        }
        else {
            details.append(format(string, args));
        }
    }

    public void appendDetailsLine(String string, Object... args)
    {
        appendDetails(string, args);
        details.append('\n');
    }

    @JsonProperty
    public PlanNodeId getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
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
    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    @JsonProperty
    public List<PlanFragmentId> getRemoteSources()
    {
        return remoteSources;
    }

    @JsonProperty
    public String getDetails()
    {
        return details.toString();
    }

    @JsonProperty
    public Optional<PlanNodeStats> getStats()
    {
        return stats;
    }

    @JsonProperty
    public List<PlanNodeStatsEstimate> getEstimatedStats()
    {
        return estimatedStats;
    }

    @JsonProperty
    public List<PlanCostEstimate> getEstimatedCost()
    {
        return estimatedCost;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    @JsonProperty
    public List<NodeRepresentation> getChildNodeRepresentations()
    {
        return childrenNodeRepresentation;
    }

    public static class TypedSymbol
    {
        private final Symbol symbol;
        private final String type;

        @JsonCreator
        public TypedSymbol(
                @JsonProperty("symbol") Symbol symbol,
                @JsonProperty("type") String type)
        {
            this.symbol = requireNonNull(symbol, "symbol is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public Symbol getSymbol()
        {
            return symbol;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }
    }
}
