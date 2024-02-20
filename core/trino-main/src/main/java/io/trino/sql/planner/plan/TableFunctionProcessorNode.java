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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.TableFunctionHandle;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class TableFunctionProcessorNode
        extends PlanNode
{
    private final String name;

    // symbols produced by the function
    private final List<Symbol> properOutputs;

    // pre-planned sources
    private final Optional<PlanNode> source;
    // TODO do we need the info of which source has row semantics, or is it already included in the joins / join distribution?

    // specifies whether the function should be pruned or executed when the input is empty
    // pruneWhenEmpty is false if and only if all original input tables are KEEP WHEN EMPTY
    private final boolean pruneWhenEmpty;

    // all source symbols to be produced on output, ordered as table argument specifications
    private final List<PassThroughSpecification> passThroughSpecifications;

    // symbols required from each source, ordered as table argument specifications
    private final List<List<Symbol>> requiredSymbols;

    // mapping from source symbol to helper "marker" symbol which indicates whether the source value is valid
    // for processing or for pass-through. null value in the marker column indicates that the value at the same
    // position in the source column should not be processed or passed-through.
    // the mapping is only present if there are two or more sources.
    private final Optional<Map<Symbol, Symbol>> markerSymbols;

    // partitioning and ordering combined from sources
    private final Optional<DataOrganizationSpecification> specification;
    private final Set<Symbol> prePartitioned;
    private final int preSorted;
    private final Optional<Symbol> hashSymbol;

    private final TableFunctionHandle handle;

    @JsonCreator
    public TableFunctionProcessorNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("properOutputs") List<Symbol> properOutputs,
            @JsonProperty("source") Optional<PlanNode> source,
            @JsonProperty("pruneWhenEmpty") boolean pruneWhenEmpty,
            @JsonProperty("passThroughSpecifications") List<PassThroughSpecification> passThroughSpecifications,
            @JsonProperty("requiredSymbols") List<List<Symbol>> requiredSymbols,
            @JsonProperty("markerSymbols") Optional<Map<Symbol, Symbol>> markerSymbols,
            @JsonProperty("specification") Optional<DataOrganizationSpecification> specification,
            @JsonProperty("prePartitioned") Set<Symbol> prePartitioned,
            @JsonProperty("preSorted") int preSorted,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("handle") TableFunctionHandle handle)
    {
        super(id);
        this.name = requireNonNull(name, "name is null");
        this.properOutputs = ImmutableList.copyOf(properOutputs);
        this.source = requireNonNull(source, "source is null");
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughSpecifications = ImmutableList.copyOf(passThroughSpecifications);
        this.requiredSymbols = requiredSymbols.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
        this.markerSymbols = markerSymbols.map(ImmutableMap::copyOf);
        this.specification = requireNonNull(specification, "specification is null");
        this.prePartitioned = ImmutableSet.copyOf(prePartitioned);
        Set<Symbol> partitionBy = specification
                .map(DataOrganizationSpecification::getPartitionBy)
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
        checkArgument(partitionBy.containsAll(prePartitioned), "all pre-partitioned symbols must be contained in the partitioning list");
        this.preSorted = preSorted;
        checkArgument(
                specification
                        .flatMap(DataOrganizationSpecification::getOrderingScheme)
                        .map(OrderingScheme::getOrderBy)
                        .map(List::size)
                        .orElse(0) >= preSorted,
                "the number of pre-sorted symbols cannot be greater than the number of all ordering symbols");
        checkArgument(preSorted == 0 || partitionBy.equals(prePartitioned), "to specify pre-sorted symbols, it is required that all partitioning symbols are pre-partitioned");
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
        this.handle = requireNonNull(handle, "handle is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<Symbol> getProperOutputs()
    {
        return properOutputs;
    }

    @JsonProperty
    public Optional<PlanNode> getSource()
    {
        return source;
    }

    @JsonProperty
    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    @JsonProperty
    public List<PassThroughSpecification> getPassThroughSpecifications()
    {
        return passThroughSpecifications;
    }

    @JsonProperty
    public List<List<Symbol>> getRequiredSymbols()
    {
        return requiredSymbols;
    }

    @JsonProperty
    public Optional<Map<Symbol, Symbol>> getMarkerSymbols()
    {
        return markerSymbols;
    }

    @JsonProperty
    public Optional<DataOrganizationSpecification> getSpecification()
    {
        return specification;
    }

    @JsonProperty
    public Set<Symbol> getPrePartitioned()
    {
        return prePartitioned;
    }

    @JsonProperty
    public int getPreSorted()
    {
        return preSorted;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty
    public TableFunctionHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    @Override
    public List<PlanNode> getSources()
    {
        return source.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();

        symbols.addAll(properOutputs);

        passThroughSpecifications.stream()
                .map(PassThroughSpecification::columns)
                .flatMap(Collection::stream)
                .map(TableFunctionNode.PassThroughColumn::symbol)
                .forEach(symbols::add);

        return symbols.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunctionProcessor(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newSources)
    {
        Optional<PlanNode> newSource = newSources.isEmpty() ? Optional.empty() : Optional.of(getOnlyElement(newSources));
        return new TableFunctionProcessorNode(
                getId(),
                name,
                properOutputs,
                newSource,
                pruneWhenEmpty,
                passThroughSpecifications,
                requiredSymbols,
                markerSymbols,
                specification,
                prePartitioned,
                preSorted,
                hashSymbol,
                handle);
    }
}
