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
package io.trino.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.TableInfo;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BigQueryTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final RemoteTableName remoteTableName;
    private final String type;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> projectedColumns;
    private final Optional<String> comment;

    @JsonCreator
    public BigQueryTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("type") String type,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.type = requireNonNull(type, "type is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public BigQueryTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, TableInfo tableInfo)
    {
        this(
                schemaTableName,
                remoteTableName,
                tableInfo.getDefinition().getType().toString(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.ofNullable(tableInfo.getDescription()));
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
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
        BigQueryTableHandle that = (BigQueryTableHandle) o;
        // NOTE remoteTableName is not compared here because two handles differing in only remoteTableName will create ambiguity
        // TODO: Add tests for this (see TestJdbcTableHandle#testEquivalence for reference)
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(type, that.type) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, type, constraint, projectedColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("remoteTableName", remoteTableName)
                .add("schemaTableName", schemaTableName)
                .add("type", type)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .add("comment", comment)
                .toString();
    }

    BigQueryTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new BigQueryTableHandle(schemaTableName, remoteTableName, type, newConstraint, projectedColumns, comment);
    }

    BigQueryTableHandle withProjectedColumns(List<ColumnHandle> newProjectedColumns)
    {
        return new BigQueryTableHandle(schemaTableName, remoteTableName, type, constraint, Optional.of(newProjectedColumns), comment);
    }
}
