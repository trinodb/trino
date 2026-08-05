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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.isConstantNull;
import static io.trino.sql.ir.IrExpressions.matchComparison;

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
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call)) {
            return Optional.empty();
        }

        if (!(matchComparison(expression) instanceof Comparison comparison)) {
            return Optional.empty();
        }

        ComparisonOperator operator = comparison.operator();
        Expression left = comparison.left();
        Expression right = comparison.right();

        if (left instanceof Constant leftConstant && right instanceof Constant rightConstant) {
            return switch (operator) {
                case IDENTICAL -> Optional.of(evaluate(OperatorType.IDENTICAL, leftConstant, rightConstant, session));
                case EQUAL -> Optional.of(evaluate(OperatorType.EQUAL, leftConstant, rightConstant, session));
                case NOT_EQUAL -> Optional.of(switch (evaluate(OperatorType.EQUAL, leftConstant, rightConstant, session)) {
                    case Constant constant when isConstantNull(constant) -> constant;
                    case Constant(Type type, Boolean value) -> new Constant(type, !value);
                    default -> throw new IllegalStateException("Unexpected value for boolean expression");
                });
                case LESS_THAN -> Optional.of(evaluate(OperatorType.LESS_THAN, leftConstant, rightConstant, session));
                case LESS_THAN_OR_EQUAL -> Optional.of(evaluate(OperatorType.LESS_THAN_OR_EQUAL, leftConstant, rightConstant, session));
                default -> throw new IllegalStateException("Unexpected operator: " + operator);
            };
        }

        if (operator == IDENTICAL && isConstantNull(left)) {
            return Optional.of(new IsNull(right));
        }
        if (operator == IDENTICAL && isConstantNull(right)) {
            return Optional.of(new IsNull(left));
        }
        if (operator != IDENTICAL && (isConstantNull(left) || isConstantNull(right))) {
            return Optional.of(NULL_BOOLEAN);
        }

        return Optional.empty();
    }

    private Constant evaluate(OperatorType operator, Constant left, Constant right, Session session)
    {
        return new Constant(BOOLEAN, functionInvoker.invoke(
                metadata.resolveOperator(operator, ImmutableList.of(left.type(), right.type())),
                session.toConnectorSession(),
                Arrays.asList(left.value(), right.value())));
    }
}
