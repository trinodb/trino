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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.aggregation;

public final class PushFilterThroughAggregation
        extends FilterPushdownRule<AggregationNode>
{
    public PushFilterThroughAggregation(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, aggregation());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, AggregationNode node, Expression inheritedPredicate)
    {
        if (node.hasEmptyGroupingSet()) {
            // TODO: in case of grouping sets, we should be able to push the filters over grouping keys below the aggregation
            // and also preserve the filter above the aggregation if it has an empty grouping set
            return pushdown.filter(node, inheritedPredicate);
        }

        EqualityInference equalityInference = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate);

        List<Expression> pushdownConjuncts = new ArrayList<>();
        List<Expression> postAggregationConjuncts = new ArrayList<>();

        // Strip out non-deterministic conjuncts
        extractConjuncts(inheritedPredicate).stream()
                .filter(expression -> !isDeterministic(expression))
                .forEach(postAggregationConjuncts::add);
        inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

        Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());

        // Add the equality predicates back in
        EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(groupingKeys);
        pushdownConjuncts.addAll(equalityPartition.scopeEqualities());
        postAggregationConjuncts.addAll(equalityPartition.scopeComplementEqualities());
        postAggregationConjuncts.addAll(equalityPartition.scopeStraddlingEqualities());

        // Sort non-equality predicates by those that can be pushed down and those that cannot
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate).forEach(conjunct -> {
            if (node.getGroupIdSymbol().isPresent() && extractUnique(conjunct).contains(node.getGroupIdSymbol().get())) {
                // aggregation operator synthesizes outputs for group ids corresponding to the global grouping set (i.e., ()), so we
                // need to preserve any predicates that evaluate the group id to run after the aggregation
                // TODO: we should be able to infer if conditions on grouping() correspond to global grouping sets to determine whether
                // we need to do this for each specific case
                postAggregationConjuncts.add(conjunct);
            }
            else {
                Expression rewrittenConjunct = equalityInference.rewrite(conjunct, groupingKeys);
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }
        });

        PlanNode rewrittenSource = node.getSource();
        if (!pushdownConjuncts.isEmpty()) {
            rewrittenSource = pushdown.filterWithNewConjuncts(
                    node.getSource(),
                    combineConjuncts(pushdownConjuncts),
                    pushdown.effectivePredicateExtractor.extract(pushdown.session, pushdown.symbolAllocator, node.getSource()));
        }

        PlanNode output = node;
        if (rewrittenSource != node.getSource()) {
            output = AggregationNode.builderFrom(node)
                    .setSource(rewrittenSource)
                    .setPreGroupedSymbols(ImmutableList.of())
                    .build();
        }
        if (!postAggregationConjuncts.isEmpty()) {
            output = pushdown.filter(output, combineConjuncts(postAggregationConjuncts));
        }
        return output;
    }
}
