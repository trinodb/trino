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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Cast
        extends Expression
{
    private final Expression expression;
    private final Type type;
    private final boolean safe;

    public Cast(Expression expression, Type type)
    {
        this(expression, type, false);
    }

    @JsonCreator
    public Cast(Expression expression, Type type, boolean safe)
    {
        requireNonNull(expression, "expression is null");

        this.expression = expression;
        this.type = type;
        this.safe = safe;
    }

    @JsonProperty
    public Expression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isSafe()
    {
        return safe;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of(expression);
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
        Cast cast = (Cast) o;
        return safe == cast.safe &&
                expression.equals(cast.expression) &&
                type.equals(cast.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, type, safe);
    }

    @Override
    public String toString()
    {
        return "%sCast(%s, %s)".formatted(safe ? "Try" : "", expression, type);
    }
}
