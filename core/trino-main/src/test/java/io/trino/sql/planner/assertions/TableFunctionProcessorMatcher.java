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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public class TableFunctionProcessorMatcher
        implements Matcher
{
    private final String name;
    private final List<String> properOutputs;
    private final Set<String> passThroughSymbols;
    private final Optional<Map<String, String>> markerSymbols;
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;

    private TableFunctionProcessorMatcher(
            String name,
            List<String> properOutputs,
            Set<String> passThroughSymbols,
            Optional<Map<String, String>> markerSymbols,
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification)
    {
        this.name = requireNonNull(name, "name is null");
        this.properOutputs = ImmutableList.copyOf(properOutputs);
        this.passThroughSymbols = ImmutableSet.copyOf(passThroughSymbols);
        this.markerSymbols = markerSymbols.map(ImmutableMap::copyOf);
        this.specification = requireNonNull(specification, "specification is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TableFunctionProcessorNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        TableFunctionProcessorNode tableFunctionProcessorNode = (TableFunctionProcessorNode) node;

        if (!name.equals(tableFunctionProcessorNode.getName())) {
            return NO_MATCH;
        }

        if (properOutputs.size() != tableFunctionProcessorNode.getProperOutputs().size()) {
            return NO_MATCH;
        }

        Set<SymbolReference> expectedPassThrough = passThroughSymbols.stream()
                .map(symbolAliases::get)
                .collect(toImmutableSet());
        Set<SymbolReference> actualPassThrough = tableFunctionProcessorNode.getPassThroughSpecifications().stream()
                .map(PassThroughSpecification::columns)
                .flatMap(Collection::stream)
                .map(PassThroughColumn::symbol)
                .map(Symbol::toSymbolReference)
                .collect(toImmutableSet());
        if (!expectedPassThrough.equals(actualPassThrough)) {
            return NO_MATCH;
        }

        if (markerSymbols.isPresent() != tableFunctionProcessorNode.getMarkerSymbols().isPresent()) {
            return NO_MATCH;
        }
        if (markerSymbols.isPresent()) {
            Map<SymbolReference, SymbolReference> expectedMapping = markerSymbols.get().entrySet().stream()
                    .collect(toImmutableMap(entry -> symbolAliases.get(entry.getKey()), entry -> symbolAliases.get(entry.getValue())));
            Map<SymbolReference, SymbolReference> actualMapping = tableFunctionProcessorNode.getMarkerSymbols().orElseThrow().entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getKey().toSymbolReference(), entry -> entry.getValue().toSymbolReference()));
            if (!expectedMapping.equals(actualMapping)) {
                return NO_MATCH;
            }
        }

        if (specification.isPresent() != tableFunctionProcessorNode.getSpecification().isPresent()) {
            return NO_MATCH;
        }
        if (specification.isPresent()) {
            if (!specification.get().getExpectedValue(symbolAliases).equals(tableFunctionProcessorNode.getSpecification().orElseThrow())) {
                return NO_MATCH;
            }
        }

        ImmutableMap.Builder<String, SymbolReference> properOutputsMapping = ImmutableMap.builder();
        for (int i = 0; i < properOutputs.size(); i++) {
            properOutputsMapping.put(properOutputs.get(i), tableFunctionProcessorNode.getProperOutputs().get(i).toSymbolReference());
        }

        return match(SymbolAliases.builder()
                .putAll(symbolAliases)
                .putAll(properOutputsMapping.buildOrThrow())
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("name", name)
                .add("properOutputs", properOutputs)
                .add("passThroughSymbols", passThroughSymbols)
                .add("markerSymbols", markerSymbols)
                .add("specification", specification)
                .toString();
    }

    public static class Builder
    {
        private final Optional<PlanMatchPattern> source;
        private String name;
        private List<String> properOutputs = ImmutableList.of();
        private Set<String> passThroughSymbols = ImmutableSet.of();
        private Optional<Map<String, String>> markerSymbols = Optional.empty();
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();

        public Builder()
        {
            this.source = Optional.empty();
        }

        public Builder(PlanMatchPattern source)
        {
            this.source = Optional.of(source);
        }

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder properOutputs(List<String> properOutputs)
        {
            this.properOutputs = properOutputs;
            return this;
        }

        public Builder passThroughSymbols(Set<String> passThroughSymbols)
        {
            this.passThroughSymbols = passThroughSymbols;
            return this;
        }

        public Builder markerSymbols(Map<String, String> markerSymbols)
        {
            this.markerSymbols = Optional.of(markerSymbols);
            return this;
        }

        public Builder specification(ExpectedValueProvider<DataOrganizationSpecification> specification)
        {
            this.specification = Optional.of(specification);
            return this;
        }

        public PlanMatchPattern build()
        {
            PlanMatchPattern[] sources = source.map(sourcePattern -> new PlanMatchPattern[] {sourcePattern}).orElse(new PlanMatchPattern[] {});
            return node(TableFunctionProcessorNode.class, sources)
                    .with(new TableFunctionProcessorMatcher(name, properOutputs, passThroughSymbols, markerSymbols, specification));
        }
    }
}
