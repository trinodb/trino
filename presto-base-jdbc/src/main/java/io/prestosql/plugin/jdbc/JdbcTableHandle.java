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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    private final TupleDomain<ColumnHandle> constraint;

    // semantically aggregation is applied after constraint
    private final Optional<List<List<JdbcColumnHandle>>> groupingSets;

    // semantically limit is applied after aggregation
    private final OptionalLong limit;

    // columns of the relation described by this handle, after projections, aggregations, etc.
    private final Optional<List<JdbcColumnHandle>> columns;

    public JdbcTableHandle(SchemaTableName schemaTableName, @Nullable String catalogName, @Nullable String schemaName, String tableName)
    {
        this(
                schemaTableName,
                catalogName,
                schemaName,
                tableName,
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty());
    }

    @JsonCreator
    public JdbcTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("groupingSets") Optional<List<List<JdbcColumnHandle>>> groupingSets,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("columns") Optional<List<JdbcColumnHandle>> columns)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");

        this.constraint = requireNonNull(constraint, "constraint is null");

        requireNonNull(groupingSets, "groupingSets is null");
        checkArgument(groupingSets.isEmpty() || !groupingSets.get().isEmpty(), "Global aggregation should be represented by [[]]");
        this.groupingSets = groupingSets.map(JdbcTableHandle::copy);

        this.limit = requireNonNull(limit, "limit is null");

        requireNonNull(columns, "columns is null");
        checkArgument(groupingSets.isEmpty() || columns.isPresent(), "columns should be present when groupingSets is present");
        this.columns = columns.map(ImmutableList::copyOf);
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<List<JdbcColumnHandle>>> getGroupingSets()
    {
        return groupingSets;
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

    @JsonIgnore
    public boolean isSynthetic()
    {
        return !constraint.isAll() || groupingSets.isPresent() || limit.isPresent();
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
        return Objects.equals(this.schemaTableName, o.schemaTableName) &&
                Objects.equals(this.constraint, o.constraint) &&
                Objects.equals(this.groupingSets, o.groupingSets) &&
                Objects.equals(this.limit, o.limit) &&
                Objects.equals(this.columns, o.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, constraint, groupingSets, limit, columns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaTableName).append(" ");
        Joiner.on(".").skipNulls().appendTo(builder, catalogName, schemaName, tableName);
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        columns.ifPresent(value -> builder.append(" columns=").append(value));
        groupingSets.ifPresent(value -> builder.append(" groupingSets=").append(value));
        return builder.toString();
    }

    private static <T> List<List<T>> copy(List<List<T>> listOfLists)
    {
        return listOfLists.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
    }
}
