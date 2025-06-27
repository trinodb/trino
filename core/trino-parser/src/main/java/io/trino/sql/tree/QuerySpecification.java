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
import static java.util.Objects.requireNonNull;

public class QuerySpecification
        extends QueryBody
{
    private final Select select;
    private final Optional<Relation> from;
    private final Optional<Expression> where;
    private final Optional<GroupBy> groupBy;
    private final Optional<Expression> having;
    private final List<WindowDefinition> windows;
    private final Optional<OrderBy> orderBy;
    private final Optional<Offset> offset;
    private final Optional<Node> limit;

    public QuerySpecification(
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        this(Optional.empty(), select, from, where, groupBy, having, windows, orderBy, offset, limit);
    }

    public QuerySpecification(
            NodeLocation location,
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        this(Optional.of(location), select, from, where, groupBy, having, windows, orderBy, offset, limit);
    }

    private QuerySpecification(
            Optional<NodeLocation> location,
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            List<WindowDefinition> windows,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        super(location);
        requireNonNull(select, "select is null");
        requireNonNull(from, "from is null");
        requireNonNull(where, "where is null");
        requireNonNull(groupBy, "groupBy is null");
        requireNonNull(having, "having is null");
        requireNonNull(windows, "windows is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(offset, "offset is null");
        requireNonNull(limit, "limit is null");
        checkArgument(
                !limit.isPresent()
                        || limit.get() instanceof FetchFirst
                        || limit.get() instanceof Limit,
                "limit must be optional of either FetchFirst or Limit type");

        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windows = windows;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
    }

    public Select getSelect()
    {
        return select;
    }

    public Optional<Relation> getFrom()
    {
        return from;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public Optional<GroupBy> getGroupBy()
    {
        return groupBy;
    }

    public Optional<Expression> getHaving()
    {
        return having;
    }

    public List<WindowDefinition> getWindows()
    {
        return windows;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public Optional<Offset> getOffset()
    {
        return offset;
    }

    public Optional<Node> getLimit()
    {
        return limit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuerySpecification(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(select)
                .addAll(from.stream().toList())
                .addAll(where.stream().toList())
                .addAll(groupBy.stream().toList())
                .addAll(having.stream().toList())
                .addAll(windows)
                .addAll(orderBy.stream().toList())
                .addAll(offset.stream().toList())
                .addAll(limit.stream().toList())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("select", select)
                .add("from", from.orElse(null))
                .add("where", where.orElse(null))
                .add("groupBy", groupBy.orElse(null))
                .add("having", having.orElse(null))
                .add("windows", windows)
                .add("orderBy", orderBy.orElse(null))
                .add("offset", offset.orElse(null))
                .add("limit", limit.orElse(null))
                .omitNullValues()
                .omitEmptyValues()
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QuerySpecification o = (QuerySpecification) obj;
        return Objects.equals(select, o.select) &&
                Objects.equals(from, o.from) &&
                Objects.equals(where, o.where) &&
                Objects.equals(groupBy, o.groupBy) &&
                Objects.equals(having, o.having) &&
                Objects.equals(windows, o.windows) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(offset, o.offset) &&
                Objects.equals(limit, o.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(select, from, where, groupBy, having, windows, orderBy, offset, limit);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
