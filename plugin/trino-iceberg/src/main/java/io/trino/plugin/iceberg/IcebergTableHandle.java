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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Schema tableSchema;
    private final Optional<Long> snapshotId;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("tableSchema") Schema tableSchema,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
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
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty("tableSchema")
    @JsonSerialize(using = SchemaSerializer.class)
    @JsonDeserialize(using = SchemaDeserializer.class)
    public Schema getTableSchema()
    {
        return tableSchema;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getEnforcedPredicate()
    {
        return enforcedPredicate;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName getSchemaTableNameWithType()
    {
        return new SchemaTableName(schemaName, tableName + "$" + tableType.name().toLowerCase(Locale.ROOT));
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

        IcebergTableHandle that = (IcebergTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                tableSchema == that.tableSchema &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableType, tableSchema, snapshotId, unenforcedPredicate, enforcedPredicate);
    }

    @Override
    public String toString()
    {
        return getSchemaTableNameWithType() + "@" + snapshotId;
    }

    public static class SchemaSerializer
            extends JsonSerializer<Schema>
    {
        @Override
        public void serialize(Schema value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException
        {
            generator.writeString(SchemaParser.toJson(verifyNotNull(value, "value is null")));
        }
    }

    public static class SchemaDeserializer
            extends JsonDeserializer<Schema>
    {
        @Override
        public Schema deserialize(JsonParser parser, DeserializationContext context)
                throws IOException
        {
            return SchemaParser.fromJson(parser.readValueAs(String.class));
        }
    }
}
