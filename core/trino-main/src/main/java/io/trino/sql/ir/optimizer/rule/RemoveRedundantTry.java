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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.sql.ir.IrExpressions.mayFail;

/// Remove a `$try` wrapper when its body can never fail. E.g,
///
/// - `$try(() -> a + b) -> a + b`  (when the body is infallible)
///
/// `TRY(e)` desugars to `$try(() -> e)`, which catches a [io.trino.spi.TrinoException] raised while
/// evaluating `e` and returns `NULL` instead.
public class RemoveRedundantTry
        implements IrOptimizerRule
{
    private final PlannerContext context;

    public RemoveRedundantTry(PlannerContext context)
    {
        this.context = context;
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call call) || !call.function().name().equals(builtinFunctionName(TRY_FUNCTION_NAME))) {
            return Optional.empty();
        }

        return switch (call.arguments().getFirst()) {
            case Lambda lambda -> mayFail(context, lambda.body()) ? Optional.empty() : Optional.of(lambda.body());
            case Bind bind -> {
                Expression body = bind.function().body();
                if (mayFail(context, body)) {
                    yield Optional.empty();
                }
                List<Symbol> parameters = bind.function().arguments();
                List<Expression> values = bind.values();
                Expression result = body;
                for (int i = values.size() - 1; i >= 0; i--) {
                    result = new Let(parameters.get(i), values.get(i), result);
                }
                yield Optional.of(result);
            }
            default -> Optional.empty();
        };
    }
}
