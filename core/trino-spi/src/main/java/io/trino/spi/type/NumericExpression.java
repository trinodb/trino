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

import static java.util.Objects.requireNonNull;

/// A numeric-valued expression over a function signature's numeric variables, used for a calculated type
/// parameter such as a decimal precision/scale or varchar length (e.g. `min(38, p + 1)`).
public sealed interface NumericExpression
        permits NumericExpression.Conditional,
                NumericExpression.Literal,
                NumericExpression.Operation,
                NumericExpression.Variable
{
    /// A constant, e.g. the `38` in `min(38, p + 1)`.
    record Literal(long value)
            implements NumericExpression {}

    /// A reference to a numeric variable bound from an argument, e.g. the `p` in `decimal(p, s)`.
    record Variable(String name)
            implements NumericExpression
    {
        public Variable
        {
            requireNonNull(name, "name is null");
        }
    }

    /// A binary operation over two sub-expressions.
    record Operation(Operator operator, NumericExpression left, NumericExpression right)
            implements NumericExpression
    {
        public Operation
        {
            requireNonNull(operator, "operator is null");
            requireNonNull(left, "left is null");
            requireNonNull(right, "right is null");
        }
    }

    /// A conditional, e.g. `if(a > b, x, y)` — used by the decimal multiply/divide precision formulas.
    record Conditional(Comparison condition, NumericExpression ifTrue, NumericExpression ifFalse)
            implements NumericExpression
    {
        public Conditional
        {
            requireNonNull(condition, "condition is null");
            requireNonNull(ifTrue, "ifTrue is null");
            requireNonNull(ifFalse, "ifFalse is null");
        }
    }

    /// A comparison between two numeric expressions, used only as the condition of a [Conditional].
    record Comparison(ComparisonOperator operator, NumericExpression left, NumericExpression right)
    {
        public Comparison
        {
            requireNonNull(operator, "operator is null");
            requireNonNull(left, "left is null");
            requireNonNull(right, "right is null");
        }
    }

    enum ComparisonOperator
    {
        GREATER_THAN,
        LESS_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN_OR_EQUAL,
        EQUAL,
        NOT_EQUAL,
    }

    enum Operator
    {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MIN,
        MAX,
    }
}
