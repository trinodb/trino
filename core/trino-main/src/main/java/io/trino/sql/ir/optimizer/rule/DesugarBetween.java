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
import io.trino.spi.function.OperatorType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;

public class DesugarBetween
        implements IrOptimizerRule
{
    private final InterpretedFunctionInvoker functionInvoker;
    private final Metadata metadata;

    public DesugarBetween(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Between)) {
            return Optional.empty();
        }

        return switch (expression) {
            case Between(Constant value, Expression min, Expression max) -> Optional.of(new Logical(AND, ImmutableList.of(
                    new Comparison(LESS_THAN_OR_EQUAL, min, value),
                    new Comparison(LESS_THAN_OR_EQUAL, value, max))));
            case Between(Expression value, Constant min, Constant max) -> {
                if (min.value() == null && max.value() == null) {
                    yield Optional.of(NULL_BOOLEAN);
                }
                else if (min.value() == null) {
                    yield Optional.of(ifExpression(new Comparison(GREATER_THAN, value, max), FALSE, NULL_BOOLEAN));
                }
                else if (max.value() == null) {
                    yield Optional.of(ifExpression(new Comparison(LESS_THAN, value, min), FALSE, NULL_BOOLEAN));
                }

                ResolvedFunction lessThanOrEqual = metadata.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(value.type(), value.type()));
                yield !Boolean.TRUE.equals(functionInvoker.invoke(lessThanOrEqual, session.toConnectorSession(), min.value(), max.value())) ?
                        Optional.of(ifExpression(not(metadata, new IsNull(value)), FALSE)) :
                        Optional.empty();
            }
            case Between(Expression value, Constant min, Expression max) -> min.value() == null ?
                    Optional.of(ifExpression(new Comparison(GREATER_THAN, value, max), FALSE, NULL_BOOLEAN)) :
                    Optional.empty();
            case Between(Expression value, Expression min, Constant max) -> max.value() == null ?
                    Optional.of(ifExpression(new Comparison(LESS_THAN, value, min), FALSE, NULL_BOOLEAN)) :
                    Optional.empty();
            default -> Optional.empty();
        };
    }
}
