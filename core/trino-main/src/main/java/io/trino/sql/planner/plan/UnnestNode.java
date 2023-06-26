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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class UnnestNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> replicateSymbols;
    private final List<Mapping> mappings;
    private final Optional<Symbol> ordinalitySymbol;
    private final Type joinType;
    private final Optional<Expression> filter;

    @JsonCreator
    public UnnestNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("replicateSymbols") List<Symbol> replicateSymbols,
            @JsonProperty("mappings") List<Mapping> mappings,
            @JsonProperty("ordinalitySymbol") Optional<Symbol> ordinalitySymbol,
            @JsonProperty("joinType") Type joinType,
            @JsonProperty("filter") Optional<Expression> filter)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.replicateSymbols = ImmutableList.copyOf(requireNonNull(replicateSymbols, "replicateSymbols is null"));
        checkArgument(source.getOutputSymbols().containsAll(replicateSymbols), "Source does not contain all replicateSymbols");
        requireNonNull(mappings, "mappings is null");
        checkArgument(!mappings.isEmpty(), "mappings is empty");
        this.mappings = ImmutableList.copyOf(mappings);
        this.ordinalitySymbol = requireNonNull(ordinalitySymbol, "ordinalitySymbol is null");
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.filter = requireNonNull(filter, "filter is null");
        if (filter.isPresent()) {
            Set<Symbol> outputs = ImmutableSet.copyOf(getOutputSymbols());
            checkArgument(outputs.containsAll(SymbolsExtractor.extractUnique(filter.get())), "Outputs do not contain all filter symbols");
        }
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.<Symbol>builder()
                .addAll(replicateSymbols)
                .addAll(mappings.stream()
                        .map(Mapping::getOutputs)
                        .flatMap(Collection::stream)
                        .collect(toImmutableList()));

        ordinalitySymbol.ifPresent(outputSymbolsBuilder::add);
        return outputSymbolsBuilder.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getReplicateSymbols()
    {
        return replicateSymbols;
    }

    @JsonProperty
    public List<Mapping> getMappings()
    {
        return mappings;
    }

    @JsonProperty
    public Optional<Symbol> getOrdinalitySymbol()
    {
        return ordinalitySymbol;
    }

    @JsonProperty
    public Type getJoinType()
    {
        return joinType;
    }

    @JsonProperty
    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new UnnestNode(getId(), Iterables.getOnlyElement(newChildren), replicateSymbols, mappings, ordinalitySymbol, joinType, filter);
    }

    public static class Mapping
    {
        private final Symbol input;
        private final List<Symbol> outputs;

        @JsonCreator
        public Mapping(
                @JsonProperty("input") Symbol input,
                @JsonProperty("outputs") List<Symbol> outputs)
        {
            this.input = requireNonNull(input, "input is null");
            requireNonNull(outputs, "outputs is null");
            this.outputs = ImmutableList.copyOf(outputs);
        }

        @JsonProperty
        public Symbol getInput()
        {
            return input;
        }

        @JsonProperty
        public List<Symbol> getOutputs()
        {
            return outputs;
        }
    }
}
