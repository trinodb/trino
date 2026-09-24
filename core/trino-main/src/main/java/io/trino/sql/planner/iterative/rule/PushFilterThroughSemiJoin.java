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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.plan.Patterns.semiJoin;

public final class PushFilterThroughSemiJoin
        extends FilterPushdownRule<SemiJoinNode>
{
    public PushFilterThroughSemiJoin(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, semiJoin());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, SemiJoinNode node, Expression inheritedPredicate)
    {
        if (!extractConjuncts(inheritedPredicate).contains(node.getSemiJoinOutput().toSymbolReference())) {
            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            Set<Symbol> sourceScope = ImmutableSet.copyOf(node.getSource().getOutputSymbols());
            EqualityInference inheritedInference = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate);
            EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = inheritedInference.rewrite(conjunct, sourceScope);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            });

            // Add the inherited equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(sourceScope);
            sourceConjuncts.addAll(equalityPartition.scopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.scopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.scopeStraddlingEqualities());

            PlanNode rewrittenSource = pushdown.filter(node.getSource(), combineConjuncts(sourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        node.getFilteringSource(),
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getDistributionType(),
                        node.getDynamicFilterId());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = pushdown.filter(output, combineConjuncts(postJoinConjuncts));
            }
            return output;
        }
        Expression deterministicInheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);
        Expression sourceEffectivePredicate = filterDeterministicConjuncts(pushdown.effectivePredicateExtractor.extract(pushdown.session, pushdown.symbolAllocator, node.getSource()));
        Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(pushdown.effectivePredicateExtractor.extract(pushdown.session, pushdown.symbolAllocator, node.getFilteringSource()));
        Expression joinExpression = comparison(
                pushdown.metadata,
                getCharVarcharCoercion(pushdown.session),
                EQUAL,
                node.getSourceJoinSymbol().toSymbolReference(),
                node.getFilteringSourceJoinSymbol().toSymbolReference());

        List<Symbol> sourceSymbols = node.getSource().getOutputSymbols();
        List<Symbol> filteringSourceSymbols = node.getFilteringSource().getOutputSymbols();

        List<Expression> sourceConjuncts = new ArrayList<>();
        List<Expression> filteringSourceConjuncts = new ArrayList<>();
        List<Expression> postJoinConjuncts = new ArrayList<>();

        // Generate equality inferences
        EqualityInference allInference = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
        EqualityInference allInferenceWithoutSourceInferred = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
        EqualityInference allInferenceWithoutFilteringSourceInferred = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

        // Push inheritedPredicates down to the source if they don't involve the semi join output
        Set<Symbol> sourceScope = ImmutableSet.copyOf(sourceSymbols);
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate).forEach(conjunct -> {
            Expression rewrittenConjunct = allInference.rewrite(conjunct, sourceScope);
            // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
            if (rewrittenConjunct != null) {
                sourceConjuncts.add(rewrittenConjunct);
            }
            else {
                postJoinConjuncts.add(conjunct);
            }
        });

        // Push inheritedPredicates down to the filtering source if possible
        Set<Symbol> filterScope = ImmutableSet.copyOf(filteringSourceSymbols);
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), deterministicInheritedPredicate).forEach(conjunct -> {
            Expression rewrittenConjunct = allInference.rewrite(conjunct, filterScope);
            // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
            // logically reevaluated for each source row.
            if (rewrittenConjunct != null) {
                filteringSourceConjuncts.add(rewrittenConjunct);
            }
        });

        // move effective predicate conjuncts source <-> filter
        // See if we can push the filtering source effective predicate to the source side
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), filteringSourceEffectivePredicate)
                .map(conjunct -> allInference.rewrite(conjunct, sourceScope))
                .filter(Objects::nonNull)
                .forEach(sourceConjuncts::add);

        // See if we can push the source effective predicate to the filtering source side
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), sourceEffectivePredicate)
                .map(conjunct -> allInference.rewrite(conjunct, filterScope))
                .filter(Objects::nonNull)
                .forEach(filteringSourceConjuncts::add);

        // Add equalities from the inference back in
        sourceConjuncts.addAll(allInferenceWithoutSourceInferred.generateEqualitiesPartitionedBy(sourceScope).scopeEqualities());
        filteringSourceConjuncts.addAll(allInferenceWithoutFilteringSourceInferred.generateEqualitiesPartitionedBy(filterScope).scopeEqualities());

        // Add dynamic filtering predicate
        Optional<DynamicFilterId> dynamicFilterId = node.getDynamicFilterId();
        if (dynamicFilterId.isEmpty() && isEnableDynamicFiltering(pushdown.session) && pushdown.dynamicFiltering) {
            dynamicFilterId = Optional.of(new DynamicFilterId("df_" + pushdown.idAllocator.getNextId().toString()));
            Symbol sourceSymbol = node.getSourceJoinSymbol();
            sourceConjuncts.add(createDynamicFilterExpression(
                    pushdown.metadata,
                    getCharVarcharCoercion(pushdown.session),
                    dynamicFilterId.get(),
                    sourceSymbol.type(),
                    sourceSymbol.toSymbolReference(),
                    EQUAL));
        }

        PlanNode rewrittenSource = pushdown.filterWithNewConjuncts(node.getSource(), combineConjuncts(sourceConjuncts), sourceEffectivePredicate);
        PlanNode rewrittenFilteringSource = pushdown.filterWithNewConjuncts(node.getFilteringSource(), combineConjuncts(filteringSourceConjuncts), filteringSourceEffectivePredicate);

        PlanNode output = node;
        if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource() || !dynamicFilterId.equals(node.getDynamicFilterId())) {
            output = new SemiJoinNode(
                    node.getId(),
                    rewrittenSource,
                    rewrittenFilteringSource,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    node.getDistributionType(),
                    dynamicFilterId);
        }
        if (!postJoinConjuncts.isEmpty()) {
            output = pushdown.filter(output, combineConjuncts(postJoinConjuncts));
        }
        return output;
    }
}
