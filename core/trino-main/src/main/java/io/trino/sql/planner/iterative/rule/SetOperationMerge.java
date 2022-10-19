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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.UnionNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

class SetOperationMerge
{
    private final Context context;
    private final SetOperationNode node;
    private final List<PlanNode> newSources;

    public SetOperationMerge(SetOperationNode node, Context context)
    {
        this.node = node;
        this.context = context;
        this.newSources = new ArrayList<>();
    }

    /**
     * Only merge first source node. This method is assumed to be used for EXCEPT, which is a non-associative set operation.
     * Provides a correct plan transformation also for associative set operations: UNION and INTERSECT.
     *
     * @return Merged plan node if applied.
     */
    public Optional<SetOperationNode> mergeFirstSource()
    {
        Lookup lookup = context.getLookup();
        List<PlanNode> sources = node.getSources().stream()
                .map(lookup::resolve)
                .collect(toImmutableList());

        PlanNode child = sources.get(0);

        // Determine if set operations can be merged and whether the resulting set operation is quantified DISTINCT or ALL
        Optional<Boolean> mergedQuantifier = mergedQuantifierIsDistinct(node, child);
        if (mergedQuantifier.isEmpty()) {
            return Optional.empty();
        }

        ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder = ImmutableListMultimap.builder();

        // Merge all sources of the first source.
        addMergedMappings((SetOperationNode) child, 0, newMappingsBuilder);

        // Keep remaining as it is
        for (int i = 1; i < sources.size(); i++) {
            PlanNode source = sources.get(i);
            addOriginalMappings(source, i, newMappingsBuilder);
        }

        if (node instanceof UnionNode) {
            return Optional.of(new UnionNode(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols()));
        }
        if (node instanceof IntersectNode) {
            return Optional.of(new IntersectNode(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols(), mergedQuantifier.get()));
        }
        if (node instanceof ExceptNode) {
            return Optional.of(new ExceptNode(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols(), mergedQuantifier.get()));
        }
        throw new IllegalArgumentException("unexpected node type: " + node.getClass().getSimpleName());
    }

    /**
     * Merge all matching source nodes. This method is assumed to be used only for associative set operations: UNION and INTERSECT.
     *
     * @return Merged plan node if applied.
     */
    public Optional<SetOperationNode> merge()
    {
        checkState(node instanceof UnionNode || node instanceof IntersectNode, "unexpected node type: %s", node.getClass().getSimpleName());

        Lookup lookup = context.getLookup();
        List<PlanNode> sources = node.getSources().stream()
                .map(lookup::resolve)
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder = ImmutableListMultimap.builder();
        boolean resultIsDistinct = false;
        boolean rewritten = false;

        for (int i = 0; i < sources.size(); i++) {
            PlanNode source = sources.get(i);

            // Determine if set operations can be merged and whether the resulting set operation is quantified DISTINCT or ALL
            Optional<Boolean> mergedQuantifier = mergedQuantifierIsDistinct(node, source);
            if (mergedQuantifier.isPresent()) {
                addMergedMappings((SetOperationNode) source, i, newMappingsBuilder);
                resultIsDistinct |= mergedQuantifier.get();
                rewritten = true;
            }
            else {
                // Keep mapping as it is
                addOriginalMappings(source, i, newMappingsBuilder);
            }
        }

        if (!rewritten) {
            return Optional.empty();
        }
        if (node instanceof UnionNode) {
            return Optional.of(new UnionNode(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols()));
        }
        return Optional.of(new IntersectNode(node.getId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols(), resultIsDistinct));
    }

    /**
     * Check if node and child are mergable based on their set operation type and quantifier.
     * <p>
     * For parent and child of type UNION, merge is always possible and the assumed quantifier is ALL, because UnionNode always represents UNION ALL.
     * <p>
     * For parent and child of type INTERSECT, merge is always possible:
     * - if parent and child are both INTERSECT ALL, the resulting set operation is INTERSECT ALL
     * - otherwise, the resulting set operation is INTERSECT DISTINCT:
     * - if the parent is DISTINCT, the result has unique values, regardless of whether child branches were DISTINCT or ALL,
     * - if the child is DISTINCT, that branch is guaranteed to have unique values, so at most one element of the other branches will be
     * retained -- this is equivalent to just doing DISTINCT on the parent.
     * <p>
     * For parent and child of type EXCEPT:
     * - if parent is EXCEPT DISTINCT and child is EXCEPT ALL, merge is not possible
     * - if parent and child are both EXCEPT DISTINCT, the resulting set operation is EXCEPT DISTINCT
     * - if parent and child are both EXCEPT ALL, the resulting set operation is EXCEPT ALL
     * - if parent is EXCEPT ALL and child is EXCEPT DISTINCT, the resulting set operation is EXCEPT DISTINCT
     * <p>
     * Optional.empty() indicates that merge is not possible.
     */
    private Optional<Boolean> mergedQuantifierIsDistinct(SetOperationNode node, PlanNode child)
    {
        if (!node.getClass().equals(child.getClass())) {
            return Optional.empty();
        }

        if (node instanceof UnionNode) {
            return Optional.of(false);
        }

        if (node instanceof IntersectNode) {
            if (!((IntersectNode) node).isDistinct() && !((IntersectNode) child).isDistinct()) {
                return Optional.of(false);
            }
            return Optional.of(true);
        }

        checkState(node instanceof ExceptNode, "unexpected node type: %s", node.getClass().getSimpleName());
        if (((ExceptNode) node).isDistinct() && !((ExceptNode) child).isDistinct()) {
            return Optional.empty();
        }
        return Optional.of(((ExceptNode) child).isDistinct());
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
