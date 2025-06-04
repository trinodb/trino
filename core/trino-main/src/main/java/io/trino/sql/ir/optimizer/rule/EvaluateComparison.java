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
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.isConstantNull;

/**
 * Evaluates a constant Comparison expression
 */
public class EvaluateComparison
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public EvaluateComparison(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        return switch (expression) {
            case Comparison(Operator operator, Constant left, Constant right) when operator == IDENTICAL -> Optional.of(evaluate(OperatorType.IDENTICAL, left, right, session));
            case Comparison(Operator operator, Constant left, Constant right) when operator == EQUAL -> Optional.of(evaluate(OperatorType.EQUAL, left, right, session));
            case Comparison(Operator operator, Constant left, Constant right) when operator == NOT_EQUAL -> Optional.of(switch (evaluate(OperatorType.EQUAL, left, right, session)) {
                case Constant constant when isConstantNull(constant) -> constant;
                case Constant(Type type, Boolean value) -> new Constant(type, !value);
                default -> throw new IllegalStateException("Unexpected value for boolean expression");
            });
            case Comparison(Operator operator, Constant left, Constant right) when operator == LESS_THAN -> Optional.of(evaluate(OperatorType.LESS_THAN, left, right, session));
            case Comparison(Operator operator, Constant left, Constant right) when operator == LESS_THAN_OR_EQUAL -> Optional.of(evaluate(OperatorType.LESS_THAN_OR_EQUAL, left, right, session));
            case Comparison(Operator operator, Constant left, Constant right) when operator == GREATER_THAN -> Optional.of(evaluate(OperatorType.LESS_THAN, right, left, session));
            case Comparison(Operator operator, Constant left, Constant right) when operator == GREATER_THAN_OR_EQUAL -> Optional.of(evaluate(OperatorType.LESS_THAN_OR_EQUAL, right, left, session));
            case Comparison(Operator operator, Expression left, Expression right) when operator == IDENTICAL && isConstantNull(left) -> Optional.of(new IsNull(right));
            case Comparison(Operator operator, Expression left, Expression right) when operator == IDENTICAL && isConstantNull(right) -> Optional.of(new IsNull(left));
            case Comparison(Operator operator, Expression left, Expression right) when operator != IDENTICAL && (isConstantNull(left) || isConstantNull(right)) -> Optional.of(NULL_BOOLEAN);
            default -> Optional.empty();
        };
    }

    private Constant evaluate(OperatorType operator, Constant left, Constant right, Session session)
    {
        return new Constant(BOOLEAN, functionInvoker.invoke(
                metadata.resolveOperator(operator, ImmutableList.of(left.type(), right.type())),
                session.toConnectorSession(),
                Arrays.asList(left.value(), right.value())));
    }
}
