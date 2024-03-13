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

public final class SubscriptExpression
        extends Expression
{
    private final Expression base;
    private final Expression index;

    @JsonCreator
    public SubscriptExpression(Expression base, Expression index)
    {
        this.base = requireNonNull(base, "base is null");
        this.index = requireNonNull(index, "index is null");
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSubscriptExpression(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of(base, index);
    }

    @JsonProperty
    public Expression getBase()
    {
        return base;
    }

    @JsonProperty
    public Expression getIndex()
    {
        return index;
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

        SubscriptExpression that = (SubscriptExpression) o;

        return Objects.equals(this.base, that.base) && Objects.equals(this.index, that.index);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, index);
    }

    @Override
    public String toString()
    {
        return "Subscript(%s, %s)".formatted(base, index);
    }
}
