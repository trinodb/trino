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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class GroupingOperation
        extends Expression
{
    private final List<Expression> groupingColumns;

    public GroupingOperation(NodeLocation location, List<QualifiedName> groupingColumns)
    {
        super(location);
        requireNonNull(groupingColumns);
        checkArgument(!groupingColumns.isEmpty(), "grouping operation columns cannot be empty");
        this.groupingColumns = groupingColumns.stream()
                .map(DereferenceExpression::from)
                .collect(toImmutableList());
    }

    public List<Expression> getGroupingColumns()
    {
        return groupingColumns;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return groupingColumns;
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
        GroupingOperation other = (GroupingOperation) o;
        return Objects.equals(groupingColumns, other.groupingColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupingColumns);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
