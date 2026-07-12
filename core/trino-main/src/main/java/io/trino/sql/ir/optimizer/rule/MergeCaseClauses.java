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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.or;
import static java.util.Objects.requireNonNull;

/// Merge Case clauses that produce the same result. E.g.,
/// - `Case([When(a, r1), When(b, r1), When(c, r2)], d) -> Case([When(Or(a, b), r1), When(c, r2)], d)`
/// - `Case([When(a, r), When(b, d)], d) -> Case([When(a, r)], d)`
/// - `Case([When(a, d)], d) -> d`
///
/// Only adjacent clauses are merged: merging a clause past one with a different result would
/// change which clause wins when both operands are true. Trailing clauses whose result equals
/// the default are dropped only when their operands cannot fail — the operand's value no longer
/// affects the output, but skipping it would also skip its error.
public class MergeCaseClauses
        implements IrOptimizerRule
{
    private final PlannerContext plannerContext;

    public MergeCaseClauses(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        boolean changed = false;

        List<WhenClause> clauses = new ArrayList<>(whenClauses);
        while (!clauses.isEmpty() &&
                clauses.getLast().getResult().equals(defaultValue) &&
                !mayFail(plannerContext, clauses.getLast().getOperand())) {
            clauses.removeLast();
            changed = true;
        }

        if (clauses.isEmpty()) {
            return Optional.of(defaultValue);
        }

        ImmutableList.Builder<WhenClause> merged = ImmutableList.builderWithExpectedSize(clauses.size());
        int index = 0;
        while (index < clauses.size()) {
            WhenClause clause = clauses.get(index);
            int start = index;
            index++;
            while (index < clauses.size() && clauses.get(index).getResult().equals(clause.getResult())) {
                index++;
            }

            if (index - start == 1) {
                merged.add(clause);
            }
            else {
                List<Expression> operands = clauses.subList(start, index).stream()
                        .map(WhenClause::getOperand)
                        .collect(toImmutableList());
                merged.add(new WhenClause(or(operands), clause.getResult()));
                changed = true;
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        return Optional.of(new Case(merged.build(), defaultValue));
    }
}
