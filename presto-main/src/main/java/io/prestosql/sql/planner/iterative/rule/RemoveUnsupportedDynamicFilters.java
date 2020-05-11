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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
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
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.DynamicFilters.getDescriptor;
import static io.prestosql.sql.DynamicFilters.isDynamicFilter;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.combinePredicates;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Dynamic filters are supported only right after TableScan and only if the subtree is on the probe side of some downstream join node
 * Dynamic filters are removed from JoinNode if there is no consumer for it on probe side
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    private final Metadata metadata;

    public RemoveUnsupportedDynamicFilters(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

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

            Optional<Expression> filter = node
                    .getFilter().map(this::removeAllDynamicFilters)  // no DF support at Join operators.
                    .filter(expression -> !expression.equals(TRUE_LITERAL));

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            if (!left.equals(node.getLeft())
                    || !right.equals(node.getRight())
                    || !dynamicFilters.equals(node.getDynamicFilters())
                    || !filter.equals(node.getFilter())) {
                return new PlanWithConsumedDynamicFilters(new JoinNode(
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols(),
                        filter,
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        dynamicFilters,
                        node.getReorderJoinStatsAndCost()),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitSpatialJoin(SpatialJoinNode node, Set<String> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIds);
            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);

            Set<String> consumed = ImmutableSet.<String>builder()
                    .addAll(leftResult.consumedDynamicFilterIds)
                    .addAll(rightResult.consumedDynamicFilterIds)
                    .build();

            Expression filter = removeAllDynamicFilters(node.getFilter());

            if (!node.getFilter().equals(filter)
                    || leftResult.getNode() != node.getLeft()
                    || rightResult.getNode() != node.getRight()) {
                return new PlanWithConsumedDynamicFilters(
                        new SpatialJoinNode(
                                node.getId(),
                                node.getType(),
                                leftResult.getNode(),
                                rightResult.getNode(),
                                node.getOutputSymbols(),
                                filter,
                                node.getLeftPartitionSymbol(),
                                node.getRightPartitionSymbol(),
                                node.getKdbTree()),
                        consumed);
            }

            return new PlanWithConsumedDynamicFilters(node, consumed);
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
                return new PlanWithConsumedDynamicFilters(
                        new FilterNode(node.getId(), source, modified),
                        consumedDynamicFilterIds.build());
            }

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        private Expression removeDynamicFilters(Expression expression, Set<String> allowedDynamicFilterIds, ImmutableSet.Builder<String> consumedDynamicFilterIds)
        {
            return combineConjuncts(metadata, extractConjuncts(expression)
                    .stream()
                    .map(this::removeNestedDynamicFilters)
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
            Expression rewrittenExpression = removeNestedDynamicFilters(expression);
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(rewrittenExpression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return rewrittenExpression;
            }
            return combineConjuncts(metadata, extractResult.getStaticConjuncts());
        }

        private Expression removeNestedDynamicFilters(Expression expression)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    LogicalBinaryExpression rewrittenNode = treeRewriter.defaultRewrite(node, context);

                    boolean modified = (node != rewrittenNode);
                    ImmutableList.Builder<Expression> expressionBuilder = ImmutableList.builder();
                    if (isDynamicFilter(rewrittenNode.getLeft())) {
                        expressionBuilder.add(TRUE_LITERAL);
                        modified = true;
                    }
                    else {
                        expressionBuilder.add(rewrittenNode.getLeft());
                    }

                    if (isDynamicFilter(rewrittenNode.getRight())) {
                        expressionBuilder.add(TRUE_LITERAL);
                        modified = true;
                    }
                    else {
                        expressionBuilder.add(rewrittenNode.getRight());
                    }

                    if (!modified) {
                        return node;
                    }
                    return combinePredicates(metadata, node.getOperator(), expressionBuilder.build());
                }
            }, expression);
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
