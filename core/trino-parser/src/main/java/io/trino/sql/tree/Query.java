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

public class Query
        extends Statement
{
    private final List<SessionProperty> sessionProperties;
    private final List<FunctionSpecification> functions;
    private final Optional<With> with;
    private final QueryBody queryBody;
    private final Optional<OrderBy> orderBy;
    private final Optional<Offset> offset;
    private final Optional<Node> limit;

    @Deprecated
    public Query(
            List<SessionProperty> sessionProperties,
            List<FunctionSpecification> functions,
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        this(Optional.empty(), sessionProperties, functions, with, queryBody, orderBy, offset, limit);
    }

    public Query(
            NodeLocation location,
            List<SessionProperty> sessionProperties,
            List<FunctionSpecification> functions,
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        this(Optional.of(location), sessionProperties, functions, with, queryBody, orderBy, offset, limit);
    }

    private Query(
            Optional<NodeLocation> location,
            List<SessionProperty> sessionProperties,
            List<FunctionSpecification> functions,
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<Offset> offset,
            Optional<Node> limit)
    {
        super(location);
        requireNonNull(sessionProperties, "sessionProperties is null");
        requireNonNull(functions, "functions is null");
        requireNonNull(with, "with is null");
        requireNonNull(queryBody, "queryBody is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(offset, "offset is null");
        requireNonNull(limit, "limit is null");
        checkArgument(!limit.isPresent() || limit.get() instanceof FetchFirst || limit.get() instanceof Limit, "limit must be optional of either FetchFirst or Limit type");

        this.sessionProperties = ImmutableList.copyOf(sessionProperties);
        this.functions = ImmutableList.copyOf(functions);
        this.with = with;
        this.queryBody = queryBody;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
    }

    public List<SessionProperty> getSessionProperties()
    {
        return sessionProperties;
    }

    public List<FunctionSpecification> getFunctions()
    {
        return functions;
    }

    public Optional<With> getWith()
    {
        return with;
    }

    public QueryBody getQueryBody()
    {
        return queryBody;
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
        return visitor.visitQuery(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(functions)
                .addAll(sessionProperties)
                .addAll(with.stream().toList())
                .add(queryBody)
                .addAll(orderBy.stream().toList())
                .addAll(offset.stream().toList())
                .addAll(limit.stream().toList())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sessionProperties", sessionProperties)
                .add("functions", functions)
                .add("with", with.orElse(null))
                .add("queryBody", queryBody)
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
        Query o = (Query) obj;
        return Objects.equals(sessionProperties, o.sessionProperties) &&
                Objects.equals(functions, o.functions) &&
                Objects.equals(with, o.with) &&
                Objects.equals(queryBody, o.queryBody) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(offset, o.offset) &&
                Objects.equals(limit, o.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sessionProperties, functions, with, queryBody, orderBy, offset, limit);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
