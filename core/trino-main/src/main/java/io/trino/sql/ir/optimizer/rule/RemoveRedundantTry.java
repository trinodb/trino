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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.sql.ir.IrExpressions.mayFail;

/**
 * Remove a {@code $try} wrapper when its body can never fail. E.g,
 * <ul>
 *     <li>{@code $try(() -> a + b) -> a + b}  (when the body is infallible)
 * </ul>
 * <p>
 * {@code TRY(e)} desugars to {@code $try(() -> e)}, which catches a {@link io.trino.spi.TrinoException}
 * raised while evaluating {@code e} and returns {@code NULL} instead. When {@code e} is infallible that
 * handler is dead, so the two are equivalent for every input. Removing the wrapper drops the per-row lambda
 * dispatch and exception handling and, because {@code $try} is non-deterministic, restores the body's own
 * determinism, re-enabling the optimizations that determinism gates (dictionary processing, constant folding,
 * common-subexpression extraction, ...).
 */
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

        if (!(call.arguments().getFirst() instanceof Lambda lambda)) {
            return Optional.empty();
        }

        Expression body = lambda.body();
        if (mayFail(context, body)) {
            return Optional.empty();
        }

        return Optional.of(body);
    }
}
