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
package io.trino.spi.type;

import io.trino.spi.type.NumericExpression.Comparison;
import io.trino.spi.type.NumericExpression.Conditional;
import io.trino.spi.type.NumericExpression.Literal;
import io.trino.spi.type.NumericExpression.Operation;
import io.trino.spi.type.NumericExpression.Operator;
import io.trino.spi.type.NumericExpression.Variable;

import java.math.BigInteger;
import java.util.Map;

public final class NumericExpressions
{
    private NumericExpressions() {}

    public static String render(NumericExpression expression)
    {
        return switch (expression) {
            case Literal(long value) -> Long.toString(value);
            case Variable(String name) -> name;
            case Operation(Operator operator, NumericExpression left, NumericExpression right) -> switch (operator) {
                case MIN -> "min(%s, %s)".formatted(render(left), render(right));
                case MAX -> "max(%s, %s)".formatted(render(left), render(right));
                case ADD -> "(%s + %s)".formatted(render(left), render(right));
                case SUBTRACT -> "(%s - %s)".formatted(render(left), render(right));
                case MULTIPLY -> "(%s * %s)".formatted(render(left), render(right));
                case DIVIDE -> "(%s / %s)".formatted(render(left), render(right));
            };
            case Conditional(Comparison condition, NumericExpression ifTrue, NumericExpression ifFalse) -> "if(%s, %s, %s)".formatted(render(condition), render(ifTrue), render(ifFalse));
        };
    }

    private static String render(Comparison comparison)
    {
        String operator = switch (comparison.operator()) {
            case GREATER_THAN -> ">";
            case LESS_THAN -> "<";
            case GREATER_THAN_OR_EQUAL -> ">=";
            case LESS_THAN_OR_EQUAL -> "<=";
            case EQUAL -> "=";
            case NOT_EQUAL -> "!=";
        };
        return render(comparison.left()) + " " + operator + " " + render(comparison.right());
    }

    /// Evaluates the expression against the given numeric-variable bindings. The result is a
    /// [BigInteger]: a calculated parameter such as `min(2147483647, x + max(x * y / 2, y) * (x + 1))`
    /// relies on a large intermediate being clamped back into range, so the intermediate must be computed
    /// exactly rather than wrapping in long. Callers narrow with `longValueExact()` at the boundary.
    public static BigInteger evaluate(NumericExpression expression, Map<String, Long> bindings)
    {
        return switch (expression) {
            case Literal(long value) -> BigInteger.valueOf(value);
            case Variable(String name) -> {
                Long value = bindings.get(name);
                if (value == null) {
                    throw new IllegalArgumentException("No binding for numeric variable " + name);
                }
                yield BigInteger.valueOf(value);
            }
            case Operation(Operator operator, NumericExpression left, NumericExpression right) -> {
                BigInteger leftValue = evaluate(left, bindings);
                BigInteger rightValue = evaluate(right, bindings);
                yield switch (operator) {
                    case ADD -> leftValue.add(rightValue);
                    case SUBTRACT -> leftValue.subtract(rightValue);
                    case MULTIPLY -> leftValue.multiply(rightValue);
                    case DIVIDE -> leftValue.divide(rightValue);
                    case MIN -> leftValue.min(rightValue);
                    case MAX -> leftValue.max(rightValue);
                };
            }
            case Conditional(Comparison condition, NumericExpression ifTrue, NumericExpression ifFalse) -> evaluate(condition, bindings) ? evaluate(ifTrue, bindings) : evaluate(ifFalse, bindings);
        };
    }

    private static boolean evaluate(Comparison comparison, Map<String, Long> bindings)
    {
        int order = evaluate(comparison.left(), bindings).compareTo(evaluate(comparison.right(), bindings));
        return switch (comparison.operator()) {
            case GREATER_THAN -> order > 0;
            case LESS_THAN -> order < 0;
            case GREATER_THAN_OR_EQUAL -> order >= 0;
            case LESS_THAN_OR_EQUAL -> order <= 0;
            case EQUAL -> order == 0;
            case NOT_EQUAL -> order != 0;
        };
    }
}
