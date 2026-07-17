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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.TryCastFunction.TRY_CAST_FUNCTION_NAME;
import static io.trino.sql.ir.IrExpressions.cast;

/// Replace a `$try_cast` with a plain [Cast] when the underlying coercion can never fail. E.g,
///
/// - `$try_cast(x::integer as bigint) -> Cast(x, bigint)`
///
/// `$try_cast` differs from `Cast` only by mapping coercion failures to `NULL`. When the coercion is
/// infallible that failure path is dead, so the two are equivalent for every input. The check is on the
/// coercion alone, not the argument: both forms evaluate the argument identically, so the rewrite is sound
/// even if the argument itself can fail.
public class SimplifyRedundantTryCast
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final TypeManager typeManager;

    public SimplifyRedundantTryCast(PlannerContext context)
    {
        this.metadata = context.getMetadata();
        this.typeManager = context.getTypeManager();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call call) || !call.function().name().equals(builtinFunctionName(TRY_CAST_FUNCTION_NAME))) {
            return Optional.empty();
        }

        Expression value = call.arguments().getFirst();
        Type targetType = call.type();
        ResolvedFunction coercion = metadata.getCoercion(value.type(), targetType);
        if (!coercion.neverFails()) {
            return Optional.empty();
        }

        return Optional.of(cast(typeManager, value, targetType));
    }
}
