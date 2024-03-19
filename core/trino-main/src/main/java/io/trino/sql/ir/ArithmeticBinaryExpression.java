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
package io.trino.sql.ir;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonSerialize
public record ArithmeticBinaryExpression(ResolvedFunction function, Operator operator, Expression left, Expression right)
        implements Expression
{
    public enum Operator
    {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%");
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

    public ArithmeticBinaryExpression
    {
        requireNonNull(function, "function is null");
        requireNonNull(operator, "operator is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
    }

    @Deprecated
    public ResolvedFunction getFunction()
    {
        return function;
    }

    @Deprecated
    public Operator getOperator()
    {
        return operator;
    }

    @Deprecated
    public Expression getLeft()
    {
        return left;
    }

    @Deprecated
    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticBinary(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public String toString()
    {
        return "%s(%s, %s)".formatted(operator.getValue(), left, right);
    }
}
