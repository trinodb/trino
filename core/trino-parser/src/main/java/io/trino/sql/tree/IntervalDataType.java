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
    private final IntervalQualifier qualifier;

    public IntervalDataType(NodeLocation location, IntervalQualifier qualifier)
    {
        this(Optional.of(location), qualifier);
    }

    public IntervalDataType(Optional<NodeLocation> location, IntervalQualifier qualifier)
    {
        super(location);
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
    }

    public IntervalQualifier qualifier()
    {
        return qualifier;
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
        if (!(o instanceof IntervalDataType that)) {
            return false;
        }
        return Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(qualifier);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        IntervalDataType otherType = (IntervalDataType) other;
        return qualifier.equals(otherType.qualifier);
    }
}
