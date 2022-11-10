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
package io.trino.metadata;

import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;
import static java.util.Objects.requireNonNull;

public final class TableSchema
{
    private final String catalogName;
    private final ConnectorTableSchema tableSchema;

    public TableSchema(String catalogName, ConnectorTableSchema tableSchema)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableSchema, "metadata is null");

        this.catalogName = catalogName;
        this.tableSchema = tableSchema;
    }

    public QualifiedObjectName getQualifiedName()
    {
        return new QualifiedObjectName(catalogName, tableSchema.getTable().getSchemaName(), tableSchema.getTable().getTableName());
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public ConnectorTableSchema getTableSchema()
    {
        return tableSchema;
    }

    public SchemaTableName getTable()
    {
        return tableSchema.getTable();
    }

    public List<ColumnSchema> getColumns()
    {
        return tableSchema.getColumns();
    }

    public ColumnSchema getColumn(String name)
    {
        return tableSchema.getColumns().stream()
                .filter(columnMetadata -> columnMetadata.getName().equals(name))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Invalid column name: " + name));
    }
}
