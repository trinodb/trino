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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableMetadata
{
    private final String catalogName;
    private final ConnectorTableMetadata metadata;

    public TableMetadata(String catalogName, ConnectorTableMetadata metadata)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(metadata, "metadata is null");

        this.catalogName = catalogName;
        this.metadata = metadata;
    }

    public QualifiedObjectName getQualifiedName()
    {
        return new QualifiedObjectName(catalogName, metadata.getTable().getSchemaName(), metadata.getTable().getTableName());
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public ConnectorTableMetadata getMetadata()
    {
        return metadata;
    }

    public SchemaTableName getTable()
    {
        return metadata.getTable();
    }

    public List<ColumnMetadata> getColumns()
    {
        return metadata.getColumns();
    }

    public ColumnMetadata getColumn(String name)
    {
        return getColumns().stream()
                .filter(columnMetadata -> columnMetadata.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid column name: " + name));
    }
}
