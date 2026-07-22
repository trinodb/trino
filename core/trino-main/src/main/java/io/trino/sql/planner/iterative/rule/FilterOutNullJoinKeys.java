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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.isFilterOutNullJoinKeys;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.optimizations.NonNullDerivation.deriveNonNullSymbols;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/// Adds `key IS NOT NULL` filters below a join for equi-join key symbols that may be null.
///
/// A row with a null equi-join key cannot match. On a non-preserved side of a join such rows
/// are discarded by the join itself, so rejecting them at the source — and ultimately inside
/// the connector, once predicate pushdown turns the filter into a `NOT NULL` scan domain —
/// avoids scanning, shuffling and hashing rows that contribute nothing. Unlike dynamic
/// filtering, this also prunes the build side.
///
/// [io.trino.sql.planner.optimizations.NonNullDerivation] supplies the guard: keys already
/// known to be non-null (a column the connector declares `NOT NULL`, an existing
/// null-rejecting predicate, or a filter added by a previous invocation) are skipped, which
/// also makes the rule converge in the iterative optimizer.
public class FilterOutNullJoinKeys
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> !node.getCriteria().isEmpty() && node.getType() != FULL);

    private final PlannerContext plannerContext;

    public FilterOutNullJoinKeys(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isFilterOutNullJoinKeys(session);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        // Only a non-preserved side can be filtered: the preserved side of an outer join must
        // keep its rows even when the key is null
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();
        if (node.getType() == INNER || node.getType() == RIGHT) {
            left = filterNullKeys(left, node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(toImmutableList()), context);
        }
        if (node.getType() == INNER || node.getType() == LEFT) {
            right = filterNullKeys(right, node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(toImmutableList()), context);
        }

        if (left == node.getLeft() && right == node.getRight()) {
            return Result.empty();
        }
        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(left, right)));
    }

    private PlanNode filterNullKeys(PlanNode source, List<Symbol> keys, Context context)
    {
        Set<Symbol> nonNull = deriveNonNullSymbols(plannerContext, context.getSession(), source, context.getLookup()::resolve);
        List<Expression> conjuncts = keys.stream()
                .distinct()
                .filter(key -> !nonNull.contains(key))
                .map(key -> not(plannerContext.getMetadata(), getCharVarcharCoercion(context.getSession()), new IsNull(key.toSymbolReference())))
                .collect(toImmutableList());
        if (conjuncts.isEmpty()) {
            return source;
        }
        return new FilterNode(context.getIdAllocator().getNextId(), source, and(conjuncts));
    }
}
