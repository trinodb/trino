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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;

/// Inline a [Let] whose value is trivial ([Reference] or [Constant]), substituting the value for
/// the bound reference in the body. Duplicating a trivial value is harmless, and the substitution
/// lets the rest of the optimizer see through the binding — e.g. once the value of
/// `Let(s := 1 + 2, s >= 0 AND s <= 5)` folds to a constant, the whole predicate folds to `TRUE`.
public class InlineTrivialLet
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Let(Symbol name, Expression value, Expression body) &&
                (value instanceof Reference || value instanceof Constant)) {
            return Optional.of(inlineSymbols(symbol -> symbol.equals(name) ? value : symbol.toSymbolReference(), body));
        }
        return Optional.empty();
    }
}
