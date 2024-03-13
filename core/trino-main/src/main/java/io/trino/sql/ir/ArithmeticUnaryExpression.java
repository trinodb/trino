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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ArithmeticUnaryExpression
        extends Expression
{
    public enum Sign
    {
        PLUS,
        MINUS
    }

    private final Expression value;
    private final Sign sign;

    @JsonCreator
    public ArithmeticUnaryExpression(Sign sign, Expression value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");

        this.value = value;
        this.sign = sign;
    }

    public static ArithmeticUnaryExpression positive(Expression value)
    {
        return new ArithmeticUnaryExpression(Sign.PLUS, value);
    }

    public static ArithmeticUnaryExpression negative(Expression value)
    {
        return new ArithmeticUnaryExpression(Sign.MINUS, value);
    }

    @JsonProperty
    public Expression getValue()
    {
        return value;
    }

    @JsonProperty
    public Sign getSign()
    {
        return sign;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticUnary(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of(value);
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

        ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;
        return Objects.equals(value, that.value) &&
                (sign == that.sign);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, sign);
    }

    @Override
    public String toString()
    {
        return "%s(%s)".formatted(
                switch (sign) {
                    case PLUS -> "+";
                    case MINUS -> "-";
                },
                value);
    }
}
