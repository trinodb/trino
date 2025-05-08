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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.type.TypeCoercion;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.join.JoinUtils.getJoinDynamicFilters;
import static io.trino.operator.join.JoinUtils.getSemiJoinDynamicFilterId;
import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.DynamicFilters.getDescriptor;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.combinePredicates;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Dynamic filters are supported only right after TableScan and only if the subtree is on
 * 1. the probe side of some downstream JoinNode or
 * 2. the source side of some downstream SemiJoinNode node
 * Dynamic filters are removed from JoinNode/SemiJoinNode if there is no consumer for it on probe/source side
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;

    public RemoveUnsupportedDynamicFilters(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        // This is a limited type analyzer for the simple expressions used in dynamic filters
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        PlanWithConsumedDynamicFilters result = plan.accept(new RemoveUnsupportedDynamicFilters.Rewriter(), ImmutableSet.of());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<PlanWithConsumedDynamicFilters, Set<DynamicFilterId>>
    {
        private final TypeCoercion typeCoercion;

        public Rewriter()
        {
            this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        }

        @Override
        protected PlanWithConsumedDynamicFilters visitPlan(PlanNode node, Set<DynamicFilterId> allowedDynamicFilterIds)
        {
            List<PlanWithConsumedDynamicFilters> children = node.getSources().stream()
                    .map(source -> source.accept(this, allowedDynamicFilterIds))
                    .collect(toImmutableList());

            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithConsumedDynamicFilters::getNode)
                            .collect(toList()));

            Set<DynamicFilterId> consumedDynamicFilterIds = children.stream()
                    .map(PlanWithConsumedDynamicFilters::getConsumedDynamicFilterIds)
                    .flatMap(Set::stream)
                    .collect(toImmutableSet());

            return new PlanWithConsumedDynamicFilters(result, consumedDynamicFilterIds);
        }

        @Override
        public PlanWithConsumedDynamicFilters visitJoin(JoinNode node, Set<DynamicFilterId> allowedDynamicFilterIds)
        {
            Map<DynamicFilterId, Symbol> currentJoinDynamicFilters = getJoinDynamicFilters(node);
            Set<DynamicFilterId> allowedDynamicFilterIdsProbeSide = ImmutableSet.<DynamicFilterId>builder()
                    .addAll(currentJoinDynamicFilters.keySet())
                    .addAll(allowedDynamicFilterIds)
                    .build();

            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIdsProbeSide);
            Set<DynamicFilterId> consumedProbeSide = leftResult.getConsumedDynamicFilterIds();
            Map<DynamicFilterId, Symbol> dynamicFilters = currentJoinDynamicFilters.entrySet().stream()
                    .filter(entry -> consumedProbeSide.contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);
            Set<DynamicFilterId> consumed = new HashSet<>(rightResult.getConsumedDynamicFilterIds());
            consumed.addAll(consumedProbeSide);
            consumed.removeAll(dynamicFilters.keySet());

            Optional<Expression> filter = node
                    .getFilter().map(this::removeAllDynamicFilters)  // no DF support at Join operators.
                    .filter(expression -> !expression.equals(TRUE));

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            if (!left.equals(node.getLeft())
                    || !right.equals(node.getRight())
                    || !dynamicFilters.equals(currentJoinDynamicFilters)
                    || !filter.equals(node.getFilter())) {
                return new PlanWithConsumedDynamicFilters(new JoinNode(
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols(),
                        node.isMaySkipOutputDuplicates(),
                        filter,
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        // When there was no dynamic filter in join, it should remain empty
                        node.getDynamicFilters().isEmpty() ? ImmutableMap.of() : dynamicFilters,
                        node.getReorderJoinStatsAndCost()),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitSpatialJoin(SpatialJoinNode node, Set<DynamicFilterId> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIds);
            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);

            Set<DynamicFilterId> consumed = ImmutableSet.<DynamicFilterId>builder()
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
        public PlanWithConsumedDynamicFilters visitSemiJoin(SemiJoinNode node, Set<DynamicFilterId> allowedDynamicFilterIds)
        {
            Optional<DynamicFilterId> dynamicFilterIdOptional = getSemiJoinDynamicFilterId(node);
            if (dynamicFilterIdOptional.isEmpty()) {
                return visitPlan(node, allowedDynamicFilterIds);
            }

            DynamicFilterId dynamicFilterId = dynamicFilterIdOptional.get();

            Set<DynamicFilterId> allowedDynamicFilterIdsSourceSide = ImmutableSet.<DynamicFilterId>builder()
                    .add(dynamicFilterId)
                    .addAll(allowedDynamicFilterIds)
                    .build();
            PlanWithConsumedDynamicFilters sourceResult = node.getSource().accept(this, allowedDynamicFilterIdsSourceSide);
            PlanWithConsumedDynamicFilters filteringSourceResult = node.getFilteringSource().accept(this, allowedDynamicFilterIds);

            Set<DynamicFilterId> consumed = new HashSet<>(filteringSourceResult.getConsumedDynamicFilterIds());
            consumed.addAll(sourceResult.getConsumedDynamicFilterIds());
            Optional<DynamicFilterId> newFilterId;
            if (consumed.contains(dynamicFilterId)) {
                consumed.remove(dynamicFilterId);
                newFilterId = Optional.of(dynamicFilterId);
            }
            else {
                newFilterId = Optional.empty();
            }

            PlanNode newSource = sourceResult.getNode();
            PlanNode newFilteringSource = filteringSourceResult.getNode();
            if (!newSource.equals(node.getSource())
                    || !newFilteringSource.equals(node.getFilteringSource())
                    || !newFilterId.equals(dynamicFilterIdOptional)) {
                return new PlanWithConsumedDynamicFilters(new SemiJoinNode(
                        node.getId(),
                        newSource,
                        newFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        // When there was no dynamic filter in semi-join, it should remain empty
                        node.getDynamicFilterId().isEmpty() ? Optional.empty() : newFilterId),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitFilter(FilterNode node, Set<DynamicFilterId> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters result = node.getSource().accept(this, allowedDynamicFilterIds);

            Expression original = node.getPredicate();
            ImmutableSet.Builder<DynamicFilterId> consumedDynamicFilterIds = ImmutableSet.<DynamicFilterId>builder()
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

            if (TRUE.equals(modified)) {
                return new PlanWithConsumedDynamicFilters(source, consumedDynamicFilterIds.build());
            }

            if (!original.equals(modified) || source != node.getSource()) {
                return new PlanWithConsumedDynamicFilters(
                        new FilterNode(node.getId(), source, modified),
                        consumedDynamicFilterIds.build());
            }

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        private Expression removeDynamicFilters(Expression expression, Set<DynamicFilterId> allowedDynamicFilterIds, ImmutableSet.Builder<DynamicFilterId> consumedDynamicFilterIds)
        {
            return combineConjuncts(extractConjuncts(expression)
                    .stream()
                    .map(this::removeNestedDynamicFilters)
                    .filter(conjunct ->
                            getDescriptor(conjunct)
                                    .map(descriptor -> {
                                        if (allowedDynamicFilterIds.contains(descriptor.getId()) &&
                                                isSupportedDynamicFilterExpression(descriptor.getInput())) {
                                            consumedDynamicFilterIds.add(descriptor.getId());
                                            return true;
                                        }
                                        return false;
                                    }).orElse(true))
                    .collect(toImmutableList()));
        }

        private boolean isSupportedDynamicFilterExpression(Expression expression)
        {
            if (expression instanceof Reference) {
                return true;
            }
            if (!(expression instanceof Cast castExpression)) {
                return false;
            }
            if (!(castExpression.expression() instanceof Reference)) {
                return false;
            }
            Type castSourceType = castExpression.expression().type();
            Type castTargetType = castExpression.type();
            // CAST must be an implicit coercion
            if (!typeCoercion.canCoerce(castSourceType, castTargetType)) {
                return false;
            }
            return doesSaturatedFloorCastOperatorExist(castTargetType, castSourceType);
        }

        private boolean doesSaturatedFloorCastOperatorExist(Type fromType, Type toType)
        {
            try {
                plannerContext.getMetadata().getCoercion(SATURATED_FLOOR_CAST, fromType, toType);
            }
            catch (OperatorNotFoundException e) {
                return false;
            }
            return true;
        }

        private Expression removeAllDynamicFilters(Expression expression)
        {
            Expression rewrittenExpression = removeNestedDynamicFilters(expression);
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(rewrittenExpression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return rewrittenExpression;
            }
            return combineConjuncts(extractResult.getStaticConjuncts());
        }

        private Expression removeNestedDynamicFilters(Expression expression)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
            {
                @Override
                public Expression rewriteLogical(Logical node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Logical rewrittenNode = treeRewriter.defaultRewrite(node, context);

                    boolean modified = (node != rewrittenNode);
                    ImmutableList.Builder<Expression> expressionBuilder = ImmutableList.builder();

                    for (Expression term : rewrittenNode.terms()) {
                        if (isDynamicFilter(term)) {
                            expressionBuilder.add(TRUE);
                            modified = true;
                        }
                        else {
                            expressionBuilder.add(term);
                        }
                    }

                    if (!modified) {
                        return node;
                    }
                    return combinePredicates(node.operator(), expressionBuilder.build());
                }
            }, expression);
        }
    }

    private static class PlanWithConsumedDynamicFilters
    {
        private final PlanNode node;
        private final Set<DynamicFilterId> consumedDynamicFilterIds;

        PlanWithConsumedDynamicFilters(PlanNode node, Set<DynamicFilterId> consumedDynamicFilterIds)
        {
            this.node = node;
            this.consumedDynamicFilterIds = ImmutableSet.copyOf(consumedDynamicFilterIds);
        }

        PlanNode getNode()
        {
            return node;
        }

        Set<DynamicFilterId> getConsumedDynamicFilterIds()
        {
            return consumedDynamicFilterIds;
        }
    }
}
