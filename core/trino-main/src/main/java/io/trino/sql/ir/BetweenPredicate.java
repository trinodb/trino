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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

@JsonSerialize
public record BetweenPredicate(Expression value, Expression min, Expression max)
        implements Expression
{
    @JsonCreator
    public BetweenPredicate
    {
        requireNonNull(value, "value is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");
    }

    @Override
    public Type type()
    {
        return BOOLEAN;
    }

    @Deprecated
    public Expression getValue()
    {
        return value;
    }

    @Deprecated
    public Expression getMin()
    {
        return min;
    }

    @Deprecated
    public Expression getMax()
    {
        return max;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of(value, min, max);
    }

    @Override
    public String toString()
    {
        return "Between(%s, %s, %s)".formatted(value, min, max);
    }
}
