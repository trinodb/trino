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

package io.trino.plugin.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InfluxTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<InfluxColumnHandle> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final List<ColumnHandle> projections;
    private final OptionalInt limit;

    public InfluxTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, ImmutableList.of(), TupleDomain.all(), ImmutableList.of(), OptionalInt.empty());
    }

    public InfluxTableHandle(String schemaName, String tableName, List<InfluxColumnHandle> columns)
    {
        this(schemaName, tableName, columns, TupleDomain.all(), ImmutableList.of(), OptionalInt.empty());
    }

    @JsonCreator
    public InfluxTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<InfluxColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projections") List<ColumnHandle> projections,
            @JsonProperty("limit") OptionalInt limit)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.limit = requireNonNull(limit, "limit is null");
    }

    public InfluxTableHandle withProjections(List<ColumnHandle> projections)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    public InfluxTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    public InfluxTableHandle withLimit(OptionalInt limit)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    @JsonProperty
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
    public List<InfluxColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public List<ColumnHandle> getProjections()
    {
        return projections;
    }

    @JsonProperty
    public OptionalInt getLimit()
    {
        return limit;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
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
        InfluxTableHandle that = (InfluxTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projections, that.projections) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("columns", columns)
                .add("constraint", constraint)
                .add("projections", projections)
                .add("limit", limit)
                .toString();
    }
}
