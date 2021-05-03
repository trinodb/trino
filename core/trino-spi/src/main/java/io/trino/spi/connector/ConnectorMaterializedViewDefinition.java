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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.TypeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ConnectorMaterializedViewDefinition
{
    private final String originalSql;
    private final Optional<CatalogSchemaTableName> storageTable;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<Column> columns;
    private final Optional<String> comment;
    private final String owner;
    private final Map<String, Object> properties;

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("storageTable") Optional<CatalogSchemaTableName> storageTable,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("owner") String owner,
            @JsonProperty("properties") Map<String, Object> properties)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.storageTable = requireNonNull(storageTable, "storageTable is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.comment = requireNonNull(comment, "comment is null");
        this.owner = requireNonNull(owner, "owner is null");

        if (catalog.isEmpty() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
        this.properties = requireNonNull(properties, "properties are null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<CatalogSchemaTableName> getStorageTable()
    {
        return storageTable;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }

    @JsonProperty
    public Map<String, Object> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        joiner.add("originalSql=[" + originalSql + "]");
        storageTable.ifPresent(value -> joiner.add("storageTable=" + value));
        catalog.ifPresent(value -> joiner.add("catalog=" + value));
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("columns=" + columns);
        comment.ifPresent(value -> joiner.add("comment=" + value));
        joiner.add("owner=" + owner);
        joiner.add("properties=" + properties);
        return getClass().getSimpleName() + joiner.toString();
    }

    public static final class Column
    {
        private final String name;
        private final TypeId type;

        @JsonCreator
        public Column(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeId type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public TypeId getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return name + " " + type;
        }
    }
}
