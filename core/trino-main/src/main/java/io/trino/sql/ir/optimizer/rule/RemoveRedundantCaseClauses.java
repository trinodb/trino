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

import io.trino.Session;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Remove duplicated and redundant clauses in Case. E.g.,
 * <ul>
 *     <li>{@code Case([When(a, r1), When(b, r2), When(a, r3)], d) -> Switch([When(a, r1), When(b, r2)], d)}
 *     <li>{@code Case([When(a, r1), When(true, r2), When(a, r3)], d) -> Switch([When(a, r1)], r2)}
 *     <li>{@code Case([When(a, r1), When(false, r2), When(a, r3)], d) -> Switch([When(a, r1)], d)}
 *     <li>{@code Case([When(true, r)], d) -> r}
 *     <li>{@code Case([When(false, r)], d) -> d}
 * </ul>
 */
public class RemoveRedundantCaseClauses
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        List<WhenClause> newClauses = new ArrayList<>();
        Expression newDefault = defaultValue;

        Set<Expression> seen = new HashSet<>();
        boolean changed = false;
        for (WhenClause whenClause : whenClauses) {
            if (seen.contains(whenClause.getOperand()) || whenClause.getOperand().equals(FALSE) || whenClause.getOperand().equals(NULL_BOOLEAN)) {
                changed = true;
            }
            else if (whenClause.getOperand().equals(TRUE)) {
                changed = true;
                newDefault = whenClause.getResult();
                break;
            }
            else {
                newClauses.add(whenClause);

                if (isDeterministic(whenClause.getOperand())) {
                    seen.add(whenClause.getOperand());
                }
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        if (newClauses.isEmpty()) {
            return Optional.of(newDefault);
        }

        return Optional.of(new Case(newClauses, newDefault));
    }
}
