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
package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StarrocksTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final List<StarrocksColumnHandle> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> comment;
    private final Optional<List<String>> partitionKey;
    private final Optional<Map<String, Object>> properties;

    @JsonCreator
    public StarrocksTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columns") List<StarrocksColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("partitionKey") Optional<List<String>> partitionKey,
            @JsonProperty("properties") Optional<Map<String, Object>> properties)

    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.comment = comment;
        this.partitionKey = partitionKey;
        this.properties = properties;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<StarrocksColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<List<String>> getPartitionKey()
    {
        return partitionKey;
    }

    @JsonProperty
    public Optional<Map<String, Object>> getProperties()
    {
        return properties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StarrocksTableHandle other = (StarrocksTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.constraint, other.constraint);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaTableName);
        if (constraint.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!constraint.isAll()) {
            builder.append(" constraint on ");
            builder.append(constraint.getDomains().orElseThrow().keySet().stream()
                    .map(columnHandle -> ((StarrocksColumnHandle) columnHandle).getColumnName())
                    .collect(Collectors.joining(", ", "[", "]")));
        }
        if (!constraint.isNone()) {
            builder.append(" constraints=").append(constraint);
        }
        if (!columns.isEmpty()) {
            builder.append(" columns=").append(columns);
        }
        return builder.toString();
    }
}
