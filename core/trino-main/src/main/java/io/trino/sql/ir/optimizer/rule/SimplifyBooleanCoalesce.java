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
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.not;
import static java.util.Objects.requireNonNull;

/// Dissolve a boolean Coalesce with a constant fallback into a comparison. E.g.,
///
/// - `Coalesce(v, false) -> $identical(v, true)`
/// - `Coalesce(v, true) -> $not($identical(v, false))`
///
/// `Coalesce(v, false)` is true exactly when `v` is true, and `Coalesce(v, true)` is false exactly when
/// `v` is false, so `$identical` expresses both without a null result.
public class SimplifyBooleanCoalesce
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SimplifyBooleanCoalesce(PlannerContext context)
    {
        this.metadata = requireNonNull(context, "context is null").getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Coalesce(List<Expression> operands)) || !BOOLEAN.equals(expression.type())) {
            return Optional.empty();
        }

        Expression fallback = operands.getLast();
        if (!fallback.equals(TRUE) && !fallback.equals(FALSE)) {
            return Optional.empty();
        }

        List<Expression> operandsBeforeFallback = operands.subList(0, operands.size() - 1);
        Expression value = operandsBeforeFallback.size() == 1
                ? operandsBeforeFallback.getFirst()
                : new Coalesce(operandsBeforeFallback);

        if (fallback.equals(FALSE)) {
            return Optional.of(comparison(metadata, IDENTICAL, value, TRUE));
        }
        return Optional.of(not(metadata, comparison(metadata, IDENTICAL, value, FALSE)));
    }
}
