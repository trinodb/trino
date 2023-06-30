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
package io.trino.plugin.deltalake.expression;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ArithmeticBinaryExpression
        extends SparkExpression
{
    public enum Operator
    {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%"),
        BITWISE_AND("&"),
        BITWISE_XOR("^");

        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }

    private final Operator operator;
    private final SparkExpression left;
    private final SparkExpression right;

    public ArithmeticBinaryExpression(Operator operator, SparkExpression left, SparkExpression right)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    public Operator getOperator()
    {
        return operator;
    }

    public SparkExpression getLeft()
    {
        return left;
    }

    public SparkExpression getRight()
    {
        return right;
    }

    @Override
    <R, C> R accept(SparkExpressionTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticBinary(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) o;
        return operator == that.operator &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, left, right);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operator", operator)
                .add("left", left)
                .add("right", right)
                .toString();
    }
}
