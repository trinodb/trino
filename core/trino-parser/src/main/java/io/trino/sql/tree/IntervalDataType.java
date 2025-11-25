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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class IntervalDataType
        extends DataType
{
    public enum Field
    {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
    }

    private final Field from;
    private final Field to;

    public IntervalDataType(NodeLocation location, Field from, Field to)
    {
        this(Optional.of(location), from, to);
    }

    public IntervalDataType(Optional<NodeLocation> location, Field from, Field to)
    {
        super(location);
        this.from = requireNonNull(from, "from is null");
        this.to = requireNonNull(to, "to is null");
    }

    public Field getFrom()
    {
        return from;
    }

    public Field getTo()
    {
        return to;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
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
        IntervalDataType that = (IntervalDataType) o;
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

        IntervalDataType otherType = (IntervalDataType) other;
        return from.equals(otherType.from) &&
                to == otherType.to;
    }
}
