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

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.DynamicFilters.getDescriptor;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.stream.Collectors.toList;

/**
 * Dynamic filters are supported only right after TableScan and only if the subtree is on the probe side of some downstream join node
 * Dynamic filters are removed from JoinNode if there is no consumer for it on probe side
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        PlanWithConsumedDynamicFilters result = plan.accept(new RemoveUnsupportedDynamicFilters.Rewriter(), ImmutableSet.of());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<PlanWithConsumedDynamicFilters, Set<String>>
    {
        @Override
        protected PlanWithConsumedDynamicFilters visitPlan(PlanNode node, Set<String> allowedDynamicFilterIds)
        {
            List<PlanWithConsumedDynamicFilters> children = node.getSources().stream()
                    .map(source -> source.accept(this, allowedDynamicFilterIds))
                    .collect(toImmutableList());

            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithConsumedDynamicFilters::getNode)
                            .collect(toList()));

            Set<String> consumedDynamicFilterIds = children.stream()
                    .map(PlanWithConsumedDynamicFilters::getConsumedDynamicFilterIds)
                    .flatMap(Set::stream)
                    .collect(toImmutableSet());

            return new PlanWithConsumedDynamicFilters(result, consumedDynamicFilterIds);
        }

        @Override
        public PlanWithConsumedDynamicFilters visitJoin(JoinNode node, Set<String> allowedDynamicFilterIds)
        {
            ImmutableSet<String> allowedDynamicFilterIdsProbeSide = ImmutableSet.<String>builder()
                    .addAll(node.getDynamicFilters().keySet())
                    .addAll(allowedDynamicFilterIds)
                    .build();

            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIdsProbeSide);
            Set<String> consumedProbeSide = leftResult.getConsumedDynamicFilterIds();
            Map<String, Symbol> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .filter(entry -> consumedProbeSide.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);
            Set<String> consumed = new HashSet<>(rightResult.getConsumedDynamicFilterIds());
            consumed.addAll(consumedProbeSide);
            consumed.removeAll(dynamicFilters.keySet());

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            if (!left.equals(node.getLeft()) || !right.equals(node.getRight()) || !dynamicFilters.equals(node.getDynamicFilters())) {
                return new PlanWithConsumedDynamicFilters(new JoinNode(
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        dynamicFilters),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitFilter(FilterNode node, Set<String> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters result = node.getSource().accept(this, allowedDynamicFilterIds);

            Expression original = node.getPredicate();
            ImmutableSet.Builder<String> consumedDynamicFilterIds = ImmutableSet.<String>builder()
                    .addAll(result.getConsumedDynamicFilterIds());

            PlanNode source = result.getNode();
            Expression modified;
            if (source instanceof TableScanNode) {
                // Keep only allowed dynamic filters
                modified = removeDynamicFilters(original, allowedDynamicFilterIds, consumedDynamicFilterIds);
            }
            else {
                modified = removeAllDynamicFilters(original);
            }

            if (TRUE_LITERAL.equals(modified)) {
                return new PlanWithConsumedDynamicFilters(source, consumedDynamicFilterIds.build());
            }

            if (!original.equals(modified) || source != node.getSource()) {
                return new PlanWithConsumedDynamicFilters(new FilterNode(node.getId(), source, modified),
                        consumedDynamicFilterIds.build());
            }

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        private Expression removeDynamicFilters(Expression expression, Set<String> allowedDynamicFilterIds, ImmutableSet.Builder<String> consumedDynamicFilterIds)
        {
            return combineConjuncts(extractConjuncts(expression)
                    .stream()
                    .filter(conjunct ->
                            getDescriptor(conjunct)
                                    .map(descriptor -> {
                                        if (allowedDynamicFilterIds.contains(descriptor.getId())) {
                                            consumedDynamicFilterIds.add(descriptor.getId());
                                            return true;
                                        }
                                        return false;
                                    }).orElse(true))
                    .collect(toImmutableList()));
        }

        private Expression removeAllDynamicFilters(Expression expression)
        {
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(expression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return expression;
            }
            return combineConjuncts(extractResult.getStaticConjuncts());
        }
    }

    private static class PlanWithConsumedDynamicFilters
    {
        private final PlanNode node;
        private final Set<String> consumedDynamicFilterIds;

        PlanWithConsumedDynamicFilters(PlanNode node, Set<String> consumedDynamicFilterIds)
        {
            this.node = node;
            this.consumedDynamicFilterIds = ImmutableSet.copyOf(consumedDynamicFilterIds);
        }

        PlanNode getNode()
        {
            return node;
        }

        Set<String> getConsumedDynamicFilterIds()
        {
            return consumedDynamicFilterIds;
        }
    }
}
