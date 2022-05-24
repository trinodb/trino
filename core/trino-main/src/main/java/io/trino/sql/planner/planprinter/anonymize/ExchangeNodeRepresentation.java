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

package io.trino.sql.planner.planprinter.anonymize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class ExchangeNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final ExchangeNode.Type type;
    private final ExchangeNode.Scope scope;
    private final List<String> partitionArguments;
    private final Optional<Symbol> partitionHashColumn;
    private final boolean replicateNullsAndAny;
    private final Optional<Map<Symbol, SortOrder>> orderings;

    @JsonCreator
    public ExchangeNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("type") ExchangeNode.Type type,
            @JsonProperty("scope") ExchangeNode.Scope scope,
            @JsonProperty("partitionArguments") List<String> partitionArguments,
            @JsonProperty("partitionHashColumn") Optional<Symbol> partitionHashColumn,
            @JsonProperty("replicateNullsAndAny") boolean replicateNullsAndAny,
            @JsonProperty("orderings") Optional<Map<Symbol, SortOrder>> orderings)
    {
        super(id, outputLayout, sources);
        this.type = requireNonNull(type, "type is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.partitionArguments = requireNonNull(partitionArguments, "partitionArguments is null");
        this.partitionHashColumn = requireNonNull(partitionHashColumn, "partitionHashColumn is null");
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.orderings = requireNonNull(orderings, "orderings is null");
    }

    @JsonProperty
    public ExchangeNode.Type getType()
    {
        return type;
    }

    @JsonProperty
    public ExchangeNode.Scope getScope()
    {
        return scope;
    }

    @JsonProperty
    public List<String> getPartitionArguments()
    {
        return partitionArguments;
    }

    @JsonProperty
    public Optional<Symbol> getPartitionHashColumn()
    {
        return partitionHashColumn;
    }

    @JsonProperty
    public boolean isReplicateNullsAndAny()
    {
        return replicateNullsAndAny;
    }

    @JsonProperty
    public Optional<Map<Symbol, SortOrder>> getOrderings()
    {
        return orderings;
    }

    public static ExchangeNodeRepresentation fromPlanNode(ExchangeNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        List<String> partitionArguments = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                .map(AnonymizationUtils::anonymize)
                .map(Partitioning.ArgumentBinding::toString)
                .collect(toImmutableList());
        return new ExchangeNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getType(),
                node.getScope(),
                partitionArguments,
                node.getPartitioningScheme().getHashColumn().map(AnonymizationUtils::anonymize),
                node.getPartitioningScheme().isReplicateNullsAndAny(),
                node.getOrderingScheme().map(AnonymizationUtils::anonymize));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExchangeNodeRepresentation)) {
            return false;
        }
        ExchangeNodeRepresentation that = (ExchangeNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && replicateNullsAndAny == that.replicateNullsAndAny
                && type == that.type
                && scope == that.scope
                && partitionArguments.equals(that.partitionArguments)
                && partitionHashColumn.equals(that.partitionHashColumn)
                && orderings.equals(that.orderings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getId(),
                getOutputLayout(),
                getSources(),
                type,
                scope,
                partitionArguments,
                partitionHashColumn,
                replicateNullsAndAny,
                orderings);
    }
}
