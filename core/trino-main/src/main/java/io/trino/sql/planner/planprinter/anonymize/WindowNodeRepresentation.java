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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class WindowNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Map<Symbol, String> windowFunctions;
    private final List<Symbol> partitionBy;
    private final Optional<Map<Symbol, SortOrder>> orderings;
    private final Optional<Symbol> hashSymbol;
    private final Set<Symbol> prePartitionedInputs;
    private final int preSortedOrderPrefix;

    @JsonCreator
    public WindowNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("sources") List<AnonymizedNodeRepresentation> sources,
            @JsonProperty("windowFunctions") Map<Symbol, String> windowFunctions,
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("orderings") Optional<Map<Symbol, SortOrder>> orderings,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("prePartitionedInputs") Set<Symbol> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id, outputLayout, sources);
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderings = requireNonNull(orderings, "orderings is null");
        this.windowFunctions = requireNonNull(windowFunctions, "windowFunctions is null");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.prePartitionedInputs = requireNonNull(prePartitionedInputs, "prePartitionedInputs is null");
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @JsonProperty
    public Map<Symbol, String> getWindowFunctions()
    {
        return windowFunctions;
    }

    @JsonProperty
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public Optional<Map<Symbol, SortOrder>> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty
    public Set<Symbol> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    public static WindowNodeRepresentation fromPlanNode(WindowNode node, TypeProvider typeProvider, List<AnonymizedNodeRepresentation> sources)
    {
        return new WindowNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                sources,
                node.getWindowFunctions().entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> anonymize(entry.getKey()),
                                entry -> anonymize(entry.getValue()))),
                anonymize(node.getPartitionBy()),
                node.getOrderingScheme().map(AnonymizationUtils::anonymize),
                node.getHashSymbol().map(AnonymizationUtils::anonymize),
                node.getPrePartitionedInputs().stream()
                        .map(AnonymizationUtils::anonymize)
                        .collect(toImmutableSet()),
                node.getPreSortedOrderPrefix());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WindowNodeRepresentation)) {
            return false;
        }
        WindowNodeRepresentation that = (WindowNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && preSortedOrderPrefix == that.preSortedOrderPrefix
                && windowFunctions.equals(that.windowFunctions)
                && partitionBy.equals(that.partitionBy)
                && orderings.equals(that.orderings)
                && hashSymbol.equals(that.hashSymbol)
                && prePartitionedInputs.equals(that.prePartitionedInputs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getId(),
                getOutputLayout(),
                getSources(),
                windowFunctions,
                partitionBy,
                orderings,
                hashSymbol,
                prePartitionedInputs,
                preSortedOrderPrefix);
    }
}
