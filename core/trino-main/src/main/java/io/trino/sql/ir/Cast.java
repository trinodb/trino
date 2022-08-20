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

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class Cast
        extends Expression
{
    private final Expression expression;
    private final DataType type;
    private final boolean safe;
    private final boolean typeOnly;

    public Cast(Expression expression, DataType type)
    {
        this(expression, type, false, false);
    }

    public Cast(Expression expression, DataType type, boolean safe)
    {
        this(expression, type, safe, false);
    }

    public Cast(
            Expression expression,
            DataType type,
            boolean safe,
            boolean typeOnly)
    {
        requireNonNull(expression, "expression is null");
        this.expression = expression;
        this.type = type;
        this.safe = safe;
        this.typeOnly = typeOnly;
    }

    @JsonCreator
    public static Cast of(
            @JsonProperty("expression") Expression expression,
            @JsonProperty("type") DataType type,
            @JsonProperty("safe") boolean safe,
            @JsonProperty("typeOnly") boolean typeOnly)
    {
        requireNonNull(expression, "expression is null");
        return new Cast(expression, type, safe, false);
    }

    @JsonProperty
    public Expression getExpression()
    {
        return expression;
    }

    @JsonProperty
    public DataType getType()
    {
        return type;
    }

    @JsonProperty(value = "safe")
    public boolean isSafe()
    {
        return safe;
    }

    @JsonProperty(value = "typeOnly")
    public boolean isTypeOnly()
    {
        return typeOnly;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(expression, type);
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
                typeOnly == cast.typeOnly &&
                expression.equals(cast.expression) &&
                type.equals(cast.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, type, safe, typeOnly);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Cast otherCast = (Cast) other;
        return safe == otherCast.safe &&
                typeOnly == otherCast.typeOnly;
    }
}
