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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IsNotNullPredicate;

import java.util.Optional;

import static io.trino.SystemSessionProperties.deriveIsNotNullPredicates;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class DeriveNotNull
        implements Rule<JoinNode>
{
    // The purpose of this rule is to derive a conjunction of IS_NOT_NULL predicates from the join clauses for INNER joins.
    // Although this may not directly benefit the current join node itself, the main goal is to provide the null-rejected condition for potential OUTER joins from upstream,
    // and hopefully help with the transformation from outer joins to inner joins. Another benefit is possible cardinality reduction in upstream thanks to the derived predicates.

    // Only apply this rule to inner join nodes, as IS_NOT_NULL predicates derived from inner join clauses won't change the semantic. That is not the case for outer joins.
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> joinNode.getType() == JoinNode.Type.INNER && !joinNode.getCriteria().isEmpty());

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return deriveIsNotNullPredicates(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        Expression filter = node.getFilter().isPresent() ? node.getFilter().get() : TRUE_LITERAL;
        ImmutableSet<Expression> filterConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjuncts(filter));

        // In theory, we could derive "IS NOT NULL" information from both join conditions and filter conditions.
        // For simplicity, we will only use join conditions for now.
        // The rule can be extended later to support derivation from filter conditions.
        for (JoinNode.EquiJoinClause joinClause : node.getCriteria()) {
            Expression leftIsNotNullPredicate = new IsNotNullPredicate(joinClause.getLeft().toSymbolReference());
            Expression rightIsNotNullPredicate = new IsNotNullPredicate(joinClause.getRight().toSymbolReference());

            if (filterConjuncts.contains(leftIsNotNullPredicate)
                    && filterConjuncts.contains(rightIsNotNullPredicate)) {
                // this indicates we've matched and visited this node before
                return Result.empty();
            }

            filter = ExpressionUtils.and(filter, leftIsNotNullPredicate);
            filter = ExpressionUtils.and(filter, rightIsNotNullPredicate);
        }

        return Result.ofPlanNode(new JoinNode(node.getId(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getLeftOutputSymbols(),
                node.getRightOutputSymbols(),
                node.isMaySkipOutputDuplicates(),
                Optional.of(filter),    // the consolidated filter containing derived IS_NOT_NULL predicates
                node.getLeftHashSymbol(),
                node.getRightHashSymbol(),
                node.getDistributionType(),
                node.isSpillable(),
                node.getDynamicFilters(),
                node.getReorderJoinStatsAndCost()));
    }
}
