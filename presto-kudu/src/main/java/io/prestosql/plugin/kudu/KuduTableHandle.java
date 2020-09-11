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
package io.prestosql.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.kudu.client.KuduTable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class KuduTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private transient KuduTable table;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> desiredColumns;
    private final boolean isDeleteHandle;
    private final OptionalInt bucketCount;

    @JsonCreator
    public KuduTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("desiredColumns") Optional<List<ColumnHandle>> desiredColumns,
            @JsonProperty("isDeleteHandle") boolean isDeleteHandle,
            @JsonProperty("bucketCount") OptionalInt bucketCount)
    {
        this(schemaTableName, null, constraint, desiredColumns, isDeleteHandle, bucketCount);
    }

    public KuduTableHandle(
            SchemaTableName schemaTableName,
            KuduTable table,
            TupleDomain<ColumnHandle> constraint,
            Optional<List<ColumnHandle>> desiredColumns,
            boolean isDeleteHandle,
            OptionalInt bucketCount)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.table = table;
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desiredColumns is null");
        this.isDeleteHandle = isDeleteHandle;
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is empty");
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
    public boolean isDeleteHandle()
    {
        return isDeleteHandle;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, constraint, desiredColumns, isDeleteHandle);
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
                Objects.equals(this.isDeleteHandle, other.isDeleteHandle);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
