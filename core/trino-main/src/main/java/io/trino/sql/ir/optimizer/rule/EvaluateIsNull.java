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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.FunctionType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayBeNull;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.type.BooleanOperators.NOT_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Partial evaluation of IsNull. E.g.,
 * <ul>
 *     <li>{@code $is_null(Row(...)) -> false}
 *     <li>{@code $is_null(null) -> true}
 *     <li>{@code $is_null(1::bigint) -> false}
 *     <li>{@code $is_null($is_null(...) -> false}
 *     <li>{@code $is_null(Cast(x, t)) -> $is_null(x)}
 *     <li>{@code $is_null(Coalesce(a, b)) -> And($is_null(a), $is_null(b))}
 *     <li>{@code $is_null($add(a, b)) -> Or($is_null(a), $is_null(b))} for strict functions that cannot fail
 *     <li>...
 * </ul>
 */
public class EvaluateIsNull
        implements IrOptimizerRule
{
    private final PlannerContext plannerContext;

    public EvaluateIsNull(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof IsNull(Expression value))) {
            return Optional.empty();
        }

        if (value instanceof Constant inner) {
            return Optional.of(inner.value() == null ? TRUE : FALSE);
        }

        if (matchComparison(value) instanceof Comparison comparison && comparison.operator() != IDENTICAL) {
            return Optional.of(new Logical(OR, ImmutableList.of(
                    new IsNull(comparison.left()),
                    new IsNull(comparison.right()))));
        }

        if (value instanceof Call(ResolvedFunction function, List<Expression> arguments) && function.name().equals(builtinFunctionName(NOT_FUNCTION_NAME))) {
            return Optional.of(new IsNull(arguments.getFirst()));
        }

        if (!mayBeNull(plannerContext, value) && !mayFail(plannerContext, value)) {
            return Optional.of(FALSE);
        }

        if (value instanceof Coalesce(List<Expression> operands)) {
            // null if and only if all operands are null. The conjunction preserves laziness:
            // both forms evaluate an operand only when all preceding operands are null.
            return Optional.of(and(operands.stream()
                    .map(IsNull::new)
                    .collect(toImmutableList())));
        }

        if (value instanceof Call call &&
                returnsNullIfAndOnlyIfAnyArgumentIsNull(call.function().functionNullability()) &&
                call.arguments().stream().noneMatch(argument -> argument.type() instanceof FunctionType) &&
                !mayFail(plannerContext, call)) {
            // The function must not fail, since it is no longer evaluated.
            return Optional.of(or(call.arguments().stream()
                    .map(IsNull::new)
                    .collect(toImmutableList())));
        }

        return Optional.empty();
    }

    private static boolean returnsNullIfAndOnlyIfAnyArgumentIsNull(FunctionNullability nullability)
    {
        return !nullability.isReturnNullable() &&
                nullability.getArgumentNullable().stream().noneMatch(nullable -> nullable);
    }
}
