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
import io.trino.sql.tree.IntervalDayTimeDataType.Field;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class IntervalDayTimeDataType
        extends DataType
{
    private final Field from;
    private final Field to;

    @JsonCreator
    public IntervalDayTimeDataType(
            @JsonProperty("from") Field from,
            @JsonProperty("to") Field to)
    {
        this.from = requireNonNull(from, "from is null");
        this.to = requireNonNull(to, "to is null");
    }

    @JsonProperty
    public Field getFrom()
    {
        return from;
    }

    @JsonProperty
    public Field getTo()
    {
        return to;
    }

    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntervalDataType(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
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
        IntervalDayTimeDataType that = (IntervalDayTimeDataType) o;
        return from == that.from &&
                to == that.to;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(from, to);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        IntervalDayTimeDataType otherType = (IntervalDayTimeDataType) other;
        return from.equals(otherType.from) &&
                to == otherType.to;
    }
}
