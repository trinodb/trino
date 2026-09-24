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
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.unnest;

public final class PushFilterThroughUnnest
        extends FilterPushdownRule<UnnestNode>
{
    public PushFilterThroughUnnest(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, unnest());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, UnnestNode node, Expression inheritedPredicate)
    {
        if (node.getJoinType() == RIGHT || node.getJoinType() == FULL) {
            return new FilterNode(pushdown.idAllocator.getNextId(), node, inheritedPredicate);
        }

        // TODO for LEFT or INNER join type, push down UnnestNode's filter on replicate symbols
        EqualityInference equalityInference = new EqualityInference(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate);

        List<Expression> pushdownConjuncts = new ArrayList<>();
        List<Expression> postUnnestConjuncts = new ArrayList<>();

        // Strip out non-deterministic conjuncts
        extractConjuncts(inheritedPredicate).stream()
                .filter(expression -> !isDeterministic(expression))
                .forEach(postUnnestConjuncts::add);
        inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

        Set<Symbol> replicatedSymbols = ImmutableSet.copyOf(node.getReplicateSymbols());

        // Add the equality predicates back in
        EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(replicatedSymbols);
        pushdownConjuncts.addAll(equalityPartition.scopeEqualities());
        postUnnestConjuncts.addAll(equalityPartition.scopeComplementEqualities());
        postUnnestConjuncts.addAll(equalityPartition.scopeStraddlingEqualities());

        // Sort non-equality predicates by those that can be pushed down and those that cannot
        EqualityInference.nonInferrableConjuncts(pushdown.plannerContext, getCharVarcharCoercion(pushdown.session), inheritedPredicate).forEach(conjunct -> {
            Expression rewrittenConjunct = equalityInference.rewrite(conjunct, replicatedSymbols);
            if (rewrittenConjunct != null) {
                pushdownConjuncts.add(rewrittenConjunct);
            }
            else {
                postUnnestConjuncts.add(conjunct);
            }
        });

        PlanNode rewrittenSource = pushdown.filter(node.getSource(), combineConjuncts(pushdownConjuncts));

        PlanNode output = node;
        if (rewrittenSource != node.getSource()) {
            output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateSymbols(), node.getMappings(), node.getOrdinalitySymbol(), node.getJoinType());
        }
        if (!postUnnestConjuncts.isEmpty()) {
            output = pushdown.filter(output, combineConjuncts(postUnnestConjuncts));
        }
        return output;
    }
}
