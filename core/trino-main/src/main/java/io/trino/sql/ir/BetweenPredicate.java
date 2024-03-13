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

public final class BetweenPredicate
        extends Expression
{
    private final Expression value;
    private final Expression min;
    private final Expression max;

    @JsonCreator
    public BetweenPredicate(Expression value, Expression min, Expression max)
    {
        requireNonNull(value, "value is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");

        this.value = value;
        this.min = min;
        this.max = max;
    }

    @JsonProperty
    public Expression getValue()
    {
        return value;
    }

    @JsonProperty
    public Expression getMin()
    {
        return min;
    }

    @JsonProperty
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BetweenPredicate that = (BetweenPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, min, max);
    }

    @Override
    public String toString()
    {
        return "Between(%s, %s, %s)".formatted(value, min, max);
    }
}
