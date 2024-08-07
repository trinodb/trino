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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.GroupBy.Type.ALL;
import static io.trino.sql.tree.GroupBy.Type.DISTINCT;
import static java.util.Objects.requireNonNull;

public class GroupBy
        extends Node
{
    public enum Type
    {
        DISTINCT,
        ALL,
        /**/
    }

    private final Optional<Type> type;
    private final List<GroupingElement> groupingElements;

    public GroupBy(Optional<Type> type, List<GroupingElement> groupingElements)
    {
        this(Optional.empty(), type, groupingElements);
    }

    public GroupBy(NodeLocation location, Optional<Type> type, List<GroupingElement> groupingElements)
    {
        this(Optional.of(location), type, groupingElements);
    }

    private GroupBy(Optional<NodeLocation> location, Optional<Type> type, List<GroupingElement> groupingElements)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.groupingElements = ImmutableList.copyOf(requireNonNull(groupingElements));
        if (type.isPresent() && type.get() == DISTINCT) {
            checkArgument(!groupingElements.isEmpty(), "groupingElements must not be empty when type is DISTINCT");
        }
    }

    public Optional<Type> getType()
    {
        return type;
    }

    public boolean isDistinct()
    {
        return type.isPresent() && type.get() == DISTINCT;
    }

    public boolean isAll()
    {
        return type.isPresent() && type.get() == ALL;
    }

    public List<GroupingElement> getGroupingElements()
    {
        return groupingElements;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return groupingElements;
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
        GroupBy groupBy = (GroupBy) o;
        return Objects.equals(type, groupBy.type) &&
                Objects.equals(groupingElements, groupBy.groupingElements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, groupingElements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type.orElse(null))
                .add("groupingElements", groupingElements)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return type.equals(((GroupBy) other).type);
    }
}
