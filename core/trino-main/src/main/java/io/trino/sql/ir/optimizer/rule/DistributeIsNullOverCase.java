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
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.IrExpressions.matchNullIf;

/// Transforms:
/// ```
/// $is_null(Case([When(c1, r1), When(c2, r2), ...], d))
/// ```
/// into:
/// ```
/// Case([When(c1, $is_null(r1)), When(c2, $is_null(r2)), ...], $is_null(d))
/// ```
/// so that the per-branch nullability can be evaluated further (e.g., constant branches fold
/// to true or false).
public class DistributeIsNullOverCase
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof IsNull(Case caseTerm))) {
            return Optional.empty();
        }

        // Leave a desugared NULLIF intact so it stays recognizable for connector pushdown.
        if (matchNullIf(caseTerm) != null) {
            return Optional.empty();
        }

        return Optional.of(new Case(
                caseTerm.whenClauses().stream()
                        .map(clause -> new WhenClause(clause.getOperand(), new IsNull(clause.getResult())))
                        .toList(),
                new IsNull(caseTerm.defaultValue())));
    }
}
