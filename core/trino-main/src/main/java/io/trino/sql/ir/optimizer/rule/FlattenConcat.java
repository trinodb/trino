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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;

/// Flatten nested calls to the variadic `concat` function into a single call. E.g.,
/// - `concat(concat(a, b), c) -> concat(a, b, c)`
///
/// The `||` operator parses into nested two-argument `concat` calls, so `a || b || c` evaluates
/// through an intermediate copy. A single variadic call produces the same value without
/// materializing intermediate results: `concat` is strict, deterministic, preserves argument
/// order, and its failure behavior does not depend on association (the accumulated length
/// exceeds the limit for some nesting if and only if the total length does).
///
/// Only the uniform variants (all argument types equal to the result type: `varchar`,
/// `varbinary`, and `array` concatenation) participate. The `element || array` form has a
/// different signature and cannot be merged into a variadic call.
public class FlattenConcat
        implements IrOptimizerRule
{
    private static final CatalogSchemaFunctionName CONCAT_NAME = builtinFunctionName("concat");

    private final Metadata metadata;

    public FlattenConcat(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call(ResolvedFunction function, List<Expression> arguments)) || !isUniformConcat(function)) {
            return Optional.empty();
        }

        if (arguments.stream().noneMatch(FlattenConcat::isUniformConcatCall)) {
            return Optional.empty();
        }

        ImmutableList.Builder<Expression> flattened = ImmutableList.builder();
        for (Expression argument : arguments) {
            if (argument instanceof Call(ResolvedFunction innerFunction, List<Expression> innerArguments) && isUniformConcat(innerFunction)) {
                flattened.addAll(innerArguments);
            }
            else {
                flattened.add(argument);
            }
        }

        List<Expression> newArguments = flattened.build();
        return Optional.of(new Call(
                metadata.resolveBuiltinFunction("concat", fromTypes(newArguments.stream()
                        .map(Expression::type)
                        .collect(toImmutableList()))),
                newArguments));
    }

    private static boolean isUniformConcatCall(Expression expression)
    {
        return expression instanceof Call call && isUniformConcat(call.function());
    }

    private static boolean isUniformConcat(ResolvedFunction function)
    {
        return function.name().equals(CONCAT_NAME) &&
                function.signature().getArgumentTypes().stream()
                        .allMatch(type -> type.equals(function.signature().getReturnType()));
    }
}
