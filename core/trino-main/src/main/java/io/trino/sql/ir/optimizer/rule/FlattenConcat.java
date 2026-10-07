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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;

/// Flattens nested varchar concat calls, avoiding intermediate string allocations.
/// Argument order and null short-circuiting are preserved. The final concat still
/// enforces the output length limit, but a later null or failure can now be reached
/// before an oversized intermediate result would have failed its length check.
public class FlattenConcat
        implements IrOptimizerRule
{
    // Keep generated calls within the argument limit enforced by ExpressionAnalyzer.
    private static final int MAX_ARGUMENTS = 127;

    private final Metadata metadata;

    public FlattenConcat(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call call) || !isVarcharConcat(call)) {
            return Optional.empty();
        }

        int argumentCount = call.arguments().size();
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression argument : call.arguments()) {
            if (argument instanceof Call nested && isVarcharConcat(nested) &&
                    (argumentCount + (nested.arguments().size() - 1)) <= MAX_ARGUMENTS) {
                builder.addAll(nested.arguments());
                argumentCount += nested.arguments().size() - 1;
            }
            else {
                builder.add(argument);
            }
        }

        if (argumentCount == call.arguments().size()) {
            return Optional.empty();
        }

        List<Expression> arguments = builder.build();
        ResolvedFunction function = metadata.resolveBuiltinFunction(getCharVarcharCoercion(session), "concat", nCopies(arguments.size(), VARCHAR));
        return Optional.of(new Call(function, arguments));
    }

    private static boolean isVarcharConcat(Call call)
    {
        return call.function().name().equals(builtinFunctionName("concat")) &&
                call.type().equals(VARCHAR) &&
                call.arguments().size() >= 2 &&
                call.function().signature().getArgumentTypes().stream().allMatch(VARCHAR::equals);
    }
}
