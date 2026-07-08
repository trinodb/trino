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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.IrUtils.and;

/// Flatten Case expressions nested inside another Case. E.g.,
/// - `Case([When(a, r1)], Case([When(b, r2)], d)) -> Case([When(a, r1), When(b, r2)], d)`
/// - `Case([When(a, Case([When(b, r)], d))], d) -> Case([When(And(a, b), r)], d)`
///
/// The second pattern applies only to the last clause: for an earlier clause the original
/// expression produces the shared default when `a` is true and `b` is not, whereas
/// the flattened form would fall through to the remaining clauses.
public class FlattenNestedCase
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        boolean changed = false;
        List<WhenClause> clauses = whenClauses;

        WhenClause lastClause = clauses.getLast();
        if (lastClause.getResult() instanceof Case(List<WhenClause> innerClauses, Expression innerDefault) &&
                innerClauses.size() == 1 &&
                innerDefault.equals(defaultValue)) {
            WhenClause innerClause = innerClauses.getFirst();
            clauses = ImmutableList.<WhenClause>builderWithExpectedSize(clauses.size())
                    .addAll(clauses.subList(0, clauses.size() - 1))
                    .add(new WhenClause(and(lastClause.getOperand(), innerClause.getOperand()), innerClause.getResult()))
                    .build();
            changed = true;
        }

        if (defaultValue instanceof Case(List<WhenClause> innerClauses, Expression innerDefault)) {
            clauses = ImmutableList.<WhenClause>builderWithExpectedSize(clauses.size() + innerClauses.size())
                    .addAll(clauses)
                    .addAll(innerClauses)
                    .build();
            defaultValue = innerDefault;
            changed = true;
        }

        if (!changed) {
            return Optional.empty();
        }

        return Optional.of(new Case(clauses, defaultValue));
    }
}
