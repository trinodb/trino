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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

/// Transforms:
/// ```
/// $is_null(Match(x, [When(p1, r1), When(p2, r2), ...], d))
/// ```
/// into:
/// ```
/// Match(x, [When(p1, $is_null(r1)), When(p2, $is_null(r2)), ...], $is_null(d))
/// ```
/// so that the per-clause nullability can be evaluated further (e.g., constant results fold
/// to true or false).
public class DistributeIsNullOverMatch
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof IsNull(Match match))) {
            return Optional.empty();
        }

        return Optional.of(new Match(
                match.operand(),
                match.clauses().stream()
                        .map(clause -> new MatchClause(clause.predicate(), new IsNull(clause.result())))
                        .toList(),
                new IsNull(match.defaultValue())));
    }
}
