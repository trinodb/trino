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
package io.trino.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.kudu.client.KuduTable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class KuduTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private transient KuduTable table;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> desiredColumns;
    private final boolean requiresRowId;
    private final OptionalInt bucketCount;
    private final OptionalLong limit;

    @JsonCreator
    public KuduTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("desiredColumns") Optional<List<ColumnHandle>> desiredColumns,
            @JsonProperty("requiresRowId") boolean requiresRowId,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("limit") OptionalLong limit)
    {
        this(schemaTableName, null, constraint, desiredColumns, requiresRowId, bucketCount, limit);
    }

    public KuduTableHandle(
            SchemaTableName schemaTableName,
            KuduTable table,
            TupleDomain<ColumnHandle> constraint,
            Optional<List<ColumnHandle>> desiredColumns,
            boolean requiresRowId,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.table = table;
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desiredColumns is null");
        this.requiresRowId = requiresRowId;
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is empty");
        this.limit = requireNonNull(limit, "limit is null");
    }

    public KuduTableHandle withRequiresRowId(boolean requiresRowId)
    {
        return new KuduTableHandle(
            schemaTableName,
            table,
            constraint,
            desiredColumns,
            requiresRowId,
            bucketCount,
            limit);
    }

    public KuduTable getTable(KuduClientSession session)
    {
        if (table == null) {
            table = session.openTable(schemaTableName);
        }
        return table;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getDesiredColumns()
    {
        return desiredColumns;
    }

    @JsonProperty
    public boolean isRequiresRowId()
    {
        return requiresRowId;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, constraint, desiredColumns, requiresRowId, bucketCount, limit);
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

        KuduTableHandle other = (KuduTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.constraint, other.constraint) &&
                Objects.equals(this.desiredColumns, other.desiredColumns) &&
                Objects.equals(this.requiresRowId, other.requiresRowId) &&
                Objects.equals(this.bucketCount, other.bucketCount) &&
                Objects.equals(this.limit, other.limit);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
