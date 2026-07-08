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
package io.trino.sql.ir.optimizer.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.or;
import static java.util.Objects.requireNonNull;

/// Merge Match clauses that produce the same result. E.g.,
/// - `Match(x, [When(p1, r1), When(p2, r1), When(p3, r2)], d) -> Match(x, [When(Or(p1, p2), r1), When(p3, r2)], d)`
/// - `Match(x, [When(p1, r), When(p2, d)], d) -> Match(x, [When(p1, r)], d)`
/// - `Match(x, [When(p, d)], d) -> d`
///
/// Only adjacent clauses are merged: merging a clause past one with a different result would
/// change which clause wins when both predicates match. Trailing clauses whose result equals
/// the default are dropped only when their predicates cannot fail — the predicate's value no
/// longer affects the output, but skipping it would also skip its error. Removing the last
/// clause additionally requires the operand to not fail, since it is no longer evaluated.
/// Clauses with capture-desugared (Bind-wrapped) predicates are left as-is.
public class MergeMatchClauses
        implements IrOptimizerRule
{
    private final PlannerContext plannerContext;

    public MergeMatchClauses(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Match(Expression operand, List<MatchClause> matchClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        boolean changed = false;

        List<MatchClause> clauses = new ArrayList<>(matchClauses);
        while (!clauses.isEmpty()) {
            MatchClause last = clauses.getLast();
            if (!last.result().equals(defaultValue) ||
                    last.bind() != null ||
                    mayFail(plannerContext, last.lambda().body())) {
                break;
            }
            if (clauses.size() == 1 && mayFail(plannerContext, operand)) {
                break;
            }
            clauses.removeLast();
            changed = true;
        }

        if (clauses.isEmpty()) {
            return Optional.of(defaultValue);
        }

        ImmutableList.Builder<MatchClause> merged = ImmutableList.builderWithExpectedSize(clauses.size());
        int index = 0;
        while (index < clauses.size()) {
            MatchClause clause = clauses.get(index);
            int start = index;
            do {
                index++;
            }
            while (index < clauses.size() &&
            clause.bind() == null &&
            clauses.get(index).bind() == null &&
            clauses.get(index).result().equals(clause.result()));

            if (index - start == 1) {
                merged.add(clause);
            }
            else {
                merged.add(new MatchClause(mergePredicates(clauses.subList(start, index)), clause.result()));
                changed = true;
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        return Optional.of(new Match(operand, merged.build(), defaultValue));
    }

    private static Lambda mergePredicates(List<MatchClause> clauses)
    {
        Lambda first = clauses.getFirst().lambda();
        Symbol parameter = getOnlyElement(first.arguments());

        List<Expression> disjuncts = new ArrayList<>();
        disjuncts.add(first.body());
        for (MatchClause clause : clauses.subList(1, clauses.size())) {
            Lambda lambda = clause.lambda();
            disjuncts.add(replaceParameter(lambda.body(), getOnlyElement(lambda.arguments()), parameter));
        }

        return new Lambda(first.arguments(), or(disjuncts));
    }

    private static Expression replaceParameter(Expression body, Symbol from, Symbol to)
    {
        if (from.equals(to)) {
            return body;
        }

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.name().equals(from.name())) {
                    return to.toSymbolReference();
                }
                return node;
            }
        }, body);
    }
}
