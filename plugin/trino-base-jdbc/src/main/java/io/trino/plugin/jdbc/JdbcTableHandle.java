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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final JdbcRelationHandle relationHandle;

    private final TupleDomain<ColumnHandle> constraint;
    // Additional to constraint
    private final List<ParameterizedExpression> constraintExpressions;

    // semantically sort order is applied after constraint
    private final Optional<List<JdbcSortItem>> sortOrder;

    // semantically limit is applied after sort order
    private final OptionalLong limit;

    // columns of the relation described by this handle
    private final Optional<List<JdbcColumnHandle>> columns;

    /**
     * Remote tables referenced by the query other than {@code this.relationHandle.schemaTableName}.
     * {@link Optional#empty()} when unknown.
     */
    private final Optional<Set<SchemaTableName>> otherReferencedTables;

    private final int nextSyntheticColumnId;
    private final Optional<String> authorization;

    public JdbcTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, Optional<String> comment)
    {
        this(
                new JdbcNamedRelationHandle(schemaTableName, remoteTableName, comment),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(ImmutableSet.of()),
                0,
                Optional.empty());
    }

    @JsonCreator
    public JdbcTableHandle(
            @JsonProperty("relationHandle") JdbcRelationHandle relationHandle,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("constraintExpressions") List<ParameterizedExpression> constraintExpressions,
            @JsonProperty("sortOrder") Optional<List<JdbcSortItem>> sortOrder,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("columns") Optional<List<JdbcColumnHandle>> columns,
            @JsonProperty("otherReferencedTables") Optional<Set<SchemaTableName>> otherReferencedTables,
            @JsonProperty("nextSyntheticColumnId") int nextSyntheticColumnId,
            @JsonProperty("authorization") Optional<String> authorization)
    {
        this.relationHandle = requireNonNull(relationHandle, "relationHandle is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.constraintExpressions = ImmutableList.copyOf(requireNonNull(constraintExpressions, "constraintExpressions is null"));
        this.sortOrder = sortOrder.map(ImmutableList::copyOf);
        this.limit = requireNonNull(limit, "limit is null");

        this.columns = columns.map(ImmutableList::copyOf);
        this.otherReferencedTables = otherReferencedTables.map(ImmutableSet::copyOf);
        this.nextSyntheticColumnId = nextSyntheticColumnId;
        this.authorization = requireNonNull(authorization, "authorization is null");
    }

    public JdbcTableHandle intersectedWithConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new JdbcTableHandle(
                relationHandle,
                constraint.intersect(newConstraint),
                constraintExpressions,
                sortOrder,
                limit,
                columns,
                otherReferencedTables,
                nextSyntheticColumnId,
                authorization);
    }

    public JdbcNamedRelationHandle asPlainTable()
    {
        checkState(!isSynthetic(), "The table handle does not represent a plain table: %s", this);
        return getRequiredNamedRelation();
    }

    @JsonIgnore
    public JdbcNamedRelationHandle getRequiredNamedRelation()
    {
        checkState(isNamedRelation(), "The table handle does not represent a named relation: %s", this);
        return (JdbcNamedRelationHandle) relationHandle;
    }

    @JsonProperty
    public JdbcRelationHandle getRelationHandle()
    {
        return relationHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public List<ParameterizedExpression> getConstraintExpressions()
    {
        return constraintExpressions;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<List<JdbcColumnHandle>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<List<JdbcSortItem>> getSortOrder()
    {
        return sortOrder;
    }

    @JsonProperty
    public Optional<Set<SchemaTableName>> getOtherReferencedTables()
    {
        return otherReferencedTables;
    }

    /**
     * Remote tables referenced by the query. {@link Optional#empty()} when unknown.
     */
    @JsonIgnore // not a getter
    public Optional<Set<SchemaTableName>> getAllReferencedTables()
    {
        return getOtherReferencedTables().map(otherReferencedTables -> {
            if (!isNamedRelation()) {
                return otherReferencedTables;
            }
            return ImmutableSet.<SchemaTableName>builder()
                    .addAll(otherReferencedTables)
                    .add(getRequiredNamedRelation().getSchemaTableName())
                    .build();
        });
    }

    boolean mayReference(SchemaTableName schemaTableName)
    {
        if (isNamedRelation() && getRequiredNamedRelation().getSchemaTableName().equals(schemaTableName)) {
            return true;
        }
        return getOtherReferencedTables()
                .map(otherReferencedTables -> otherReferencedTables.contains(schemaTableName))
                .orElse(true);
    }

    @JsonProperty
    public int getNextSyntheticColumnId()
    {
        return nextSyntheticColumnId;
    }

    @JsonProperty
    public Optional<String> getAuthorization()
    {
        return authorization;
    }

    @JsonIgnore
    public boolean isSynthetic()
    {
        return !isNamedRelation() || !constraint.isAll() || !constraintExpressions.isEmpty() || sortOrder.isPresent() || limit.isPresent();
    }

    @JsonIgnore
    public boolean isNamedRelation()
    {
        return relationHandle instanceof JdbcNamedRelationHandle;
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
        JdbcTableHandle o = (JdbcTableHandle) obj;
        return Objects.equals(this.relationHandle, o.relationHandle) &&
                Objects.equals(this.constraint, o.constraint) &&
                Objects.equals(this.constraintExpressions, o.constraintExpressions) &&
                Objects.equals(this.sortOrder, o.sortOrder) &&
                Objects.equals(this.limit, o.limit) &&
                Objects.equals(this.columns, o.columns) &&
                this.nextSyntheticColumnId == o.nextSyntheticColumnId &&
                Objects.equals(this.authorization, o.authorization);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationHandle, constraint, constraintExpressions, sortOrder, limit, columns, nextSyntheticColumnId, authorization);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(relationHandle);
        if (constraint.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!constraint.isAll()) {
            builder.append(" constraint on ");
            builder.append(constraint.getDomains().orElseThrow().keySet().stream()
                    // TODO constraint should be defined on JdbcTableHandle
                    .map(columnHandle -> ((JdbcColumnHandle) columnHandle).getColumnName())
                    .collect(Collectors.joining(", ", "[", "]")));
        }
        if (!constraintExpressions.isEmpty()) {
            builder.append(" constraints=").append(constraintExpressions);
        }
        sortOrder.ifPresent(value -> builder.append(" sortOrder=").append(value));
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        columns.ifPresent(value -> builder.append(" columns=").append(value));
        authorization.ifPresent(value -> builder.append(" authorization=").append(value));
        return builder.toString();
    }
}
