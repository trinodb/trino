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

import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;
import static java.util.Objects.requireNonNull;

public record TableSchema(CatalogName catalogName, ConnectorTableSchema tableSchema)
{
    public TableSchema
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableSchema, "metadata is null");
    }

    public QualifiedObjectName qualifiedName()
    {
        return new QualifiedObjectName(catalogName.toString(), tableSchema.getTable().getSchemaName(), tableSchema.getTable().getTableName());
    }

    public SchemaTableName table()
    {
        return tableSchema.getTable();
    }

    public List<ColumnSchema> columns()
    {
        return tableSchema.getColumns();
    }

    public ColumnSchema column(String name)
    {
        return tableSchema.getColumns().stream()
                .filter(columnMetadata -> columnMetadata.getName().equals(name))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Invalid column name: " + name));
    }
}
