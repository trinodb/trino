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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule.Context;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SetOperationNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class SetOperationMerge
{
    private final Context context;
    private final SetOperationNode node;
    private List<PlanNode> newSources;
    private final SetOperationNodeInstantiator instantiator;

    public SetOperationMerge(SetOperationNode node, Context context, SetOperationNodeInstantiator instantiator)
    {
        this.node = node;
        this.context = context;
        this.newSources = new ArrayList<>();
        this.instantiator = instantiator;
    }

    /**
     * Only merge first source node, which is assumed to be used for non-associative set operation.
     *
     * @return Merged plan node if applied.
     */
    public Optional<SetOperationNode> mergeFirstSource()
    {
        Lookup lookup = context.getLookup();
        List<PlanNode> sources = node.getSources().stream()
                .flatMap(lookup::resolveGroup)
                .collect(Collectors.toList());

        // If the first child is not the same with the parent, do nothing.
        if (!sources.get(0).getClass().equals(node.getClass())) {
            return Optional.empty();
        }

        ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder = ImmutableListMultimap.builder();

        SetOperationNode child = (SetOperationNode) sources.get(0);
        // Merge all sources of the first source.
        addMergedMappings(child, 0, newMappingsBuilder);

        // Keep remaining as it is
        for (int i = 1; i < sources.size(); i++) {
            PlanNode source = sources.get(i);
            addOriginalMappings(source, i, newMappingsBuilder);
        }

        return Optional.of(instantiator.create(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols()));
    }

    /**
     * Constructs the new mapping and source nodes
     *
     * @return Merged plan node if applied.
     */
    public Optional<SetOperationNode> merge()
    {
        Lookup lookup = context.getLookup();
        List<PlanNode> sources = node.getSources().stream()
                .flatMap(lookup::resolveGroup)
                .collect(Collectors.toList());

        // There must be one same source node at least.
        if (sources.stream().noneMatch(node.getClass()::isInstance)) {
            return Optional.empty();
        }

        ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder = ImmutableListMultimap.builder();

        for (int i = 0; i < sources.size(); i++) {
            PlanNode source = sources.get(i);
            if (node.getClass().equals(source.getClass())) {
                SetOperationNode setOperationNode = (SetOperationNode) source;
                addMergedMappings(setOperationNode, i, newMappingsBuilder);
            }
            else {
                // Keep mapping as it is
                addOriginalMappings(source, i, newMappingsBuilder);
            }
        }

        return Optional.of(instantiator.create(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols()));
    }

    private void addMergedMappings(SetOperationNode child, int childIndex, ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder)
    {
        newSources.addAll(child.getSources());
        for (Map.Entry<Symbol, Collection<Symbol>> mapping : node.getSymbolMapping().asMap().entrySet()) {
            Symbol input = Iterables.get(mapping.getValue(), childIndex);
            newMappingsBuilder.putAll(mapping.getKey(), child.getSymbolMapping().get(input));
        }
    }

    private void addOriginalMappings(PlanNode child, int childIndex, ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder)
    {
        newSources.add(child);
        for (Map.Entry<Symbol, Collection<Symbol>> mapping : node.getSymbolMapping().asMap().entrySet()) {
            newMappingsBuilder.put(mapping.getKey(), Iterables.get(mapping.getValue(), childIndex));
        }
    }
}
