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
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.FunctionType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrExpressions.mayFailIndependentlyOfArguments;

/// Narrow a `$try` so it wraps only the fallible parts of its body. E.g,
///
/// - `$try(() -> a + risky(b)) -> a + $try(() -> risky(b))`
///
/// `TRY(e)` desugars to `$try(() -> e)`, which maps a failure raised while evaluating `e` to `NULL`. When
/// the root operation of `e` is itself infallible, the only failures come from its arguments, so the `$try`
/// can be pushed onto those arguments and the wrapper around the (mostly infallible) body dropped. Because
/// `$try` is non-deterministic, shrinking it restores determinism to everything left outside it, re-enabling
/// the optimizations that determinism gates (dictionary processing, constant folding, common-subexpression
/// extraction, ...) and shrinking the bytecode that must run under an exception handler.
///
/// Soundness requires the `NULL` produced by a pushed `$try` to propagate up to the same `NULL` the original
/// `$try` would have produced. This holds only when:
///
/// - the root operation is infallible on its own (otherwise dropping the outer `$try` would let its failure
///   escape uncaught), and
/// - every fallible argument sits in a `RETURNS NULL ON NULL INPUT` (strict) position, so a `NULL` there
///   forces the whole result to `NULL`.
///
/// Constructs that can absorb a `NULL` into a non-null result (e.g. `COALESCE`, `CASE`, `ROW`,
/// `IS DISTINCT FROM`) are therefore not narrowed. A [Cast] is always strict, so it is narrowed whenever the
/// coercion itself is infallible.
public class PushDownTry
        implements IrOptimizerRule
{
    private final PlannerContext context;

    public PushDownTry(PlannerContext context)
    {
        this.context = context;
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call call) || !call.function().name().equals(builtinFunctionName(TRY_FUNCTION_NAME))) {
            return Optional.empty();
        }

        if (!(call.arguments().getFirst() instanceof Lambda lambda) || !lambda.arguments().isEmpty()) {
            return Optional.empty();
        }

        Expression body = lambda.body();
        // The root operation must be infallible on its own; otherwise dropping the outer $try would let its
        // own failure escape uncaught. A fully infallible body is left to RemoveRedundantTry.
        if (mayFailIndependentlyOfArguments(context, body)) {
            return Optional.empty();
        }

        return switch (body) {
            case Call bodyCall -> narrow(bodyCall);
            case Cast cast -> narrow(cast);
            default -> Optional.empty();
        };
    }

    private Optional<Expression> narrow(Call call)
    {
        FunctionNullability nullability = call.function().functionNullability();
        List<Expression> arguments = call.arguments();

        boolean narrowed = false;
        ImmutableList.Builder<Expression> rewritten = ImmutableList.builderWithExpectedSize(arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            Expression argument = arguments.get(i);
            if (!mayFail(context, argument)) {
                rewritten.add(argument);
                continue;
            }
            // A fallible argument in a non-strict position could be absorbed into a non-null result, so the
            // $try cannot be narrowed past this node at all.
            if (nullability.isArgumentNullable(i)) {
                return Optional.empty();
            }
            rewritten.add(wrapInTry(argument));
            narrowed = true;
        }

        if (!narrowed) {
            return Optional.empty();
        }
        return Optional.of(new Call(call.function(), rewritten.build()));
    }

    private Optional<Expression> narrow(Cast cast)
    {
        // A cast is strict: a NULL input yields a NULL result. The coercion is infallible here (checked by
        // mayFailIndependentlyOfArguments), so only the cast argument can fail.
        if (!mayFail(context, cast.expression())) {
            return Optional.empty();
        }
        return Optional.of(new Cast(wrapInTry(cast.expression()), cast.type()));
    }

    private Expression wrapInTry(Expression expression)
    {
        return BuiltinFunctionCallBuilder.resolve(context.getMetadata())
                .setName(TRY_FUNCTION_NAME)
                .addArgument(new FunctionType(ImmutableList.of(), expression.type()), new Lambda(ImmutableList.of(), expression))
                .build();
    }
}
