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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Logical.Operator.OR;

/**
 * Partial evaluation of IsNull. E.g.,
 * <ul>
 *     <li>{@code $is_null(Row(...)) -> false}
 *     <li>{@code $is_null(null) -> true}
 *     <li>{@code $is_null(1::bigint) -> false}
 *     <li>{@code $is_null($is_null(...) -> false}
 *     <li>{@code $is_null(Cast(x, t)) -> $is_null(x)}
 *     <li>...
 * </ul>
 */
public class EvaluateIsNull
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof IsNull isNull)) {
            return Optional.empty();
        }

        return switch (isNull.value()) {
            case Constant inner -> Optional.of(inner.value() == null ? TRUE : FALSE);
            case IsNull _ -> Optional.of(FALSE);
            case Row _ -> Optional.of(FALSE);
            case Comparison inner when inner.operator() != IDENTICAL -> Optional.of(new Logical(OR, ImmutableList.of(
                    new IsNull(inner.left()),
                    new IsNull(inner.right()))));
            case Comparison inner when inner.operator() == IDENTICAL -> Optional.of(FALSE);
            case Call inner when inner.function().name().equals(builtinFunctionName("$not")) -> Optional.of(new IsNull(inner.arguments().getFirst()));
            case Call inner when
                    inner.function().deterministic() &&
                            !inner.function().functionNullability().isReturnNullable() &&
                            inner.function().functionNullability().getArgumentNullable().stream().allMatch(Boolean.TRUE::equals) -> Optional.of(FALSE);
                    // TODO: if function can't return null and non-nullable arguments are passed a non-null value, then the
                    //       the call is guaranteed to not return null
            default -> Optional.empty();
        };
    }
}
