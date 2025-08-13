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
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.plugin.lance.catalog.BaseTable;
import io.trino.plugin.lance.catalog.TrinoCatalog;
import io.trino.plugin.lance.metadata.Manifest;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class LanceMetadata
        implements ConnectorMetadata
{
    private final TrinoCatalog catalog;

    @Inject
    public LanceMetadata(TrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);

        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> schemaTableNames;
        if (prefix.getTable().isEmpty()) {
            schemaTableNames = catalog.listTables(session, prefix.getSchema());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        for (SchemaTableName tableName : schemaTableNames) {
            Optional<BaseTable> table = catalog.loadTable(session, tableName);
            if (table.isPresent()) {
                Manifest manifest = table.get().loadManifest(Optional.empty());
                List<ColumnMetadata> columns = manifest.fields().stream().map(field -> new ColumnMetadata(field.name(), field.toTrinoType())).collect(toImmutableList());
                relationColumns.put(tableName, RelationColumnsMetadata.forTable(tableName, columns));
            }
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        Optional<BaseTable> loadedTable = catalog.loadTable(session, tableName);
        if (loadedTable.isEmpty()) {
            return null;
        }
        BaseTable baseTable = loadedTable.get();
        return new LanceTableHandle(tableName, baseTable.loadManifest(Optional.empty()), baseTable.getTableLocation().toString());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle table = (LanceTableHandle) tableHandle;

        List<ColumnMetadata> columns = table.manifest().fields().stream().map(field -> new ColumnMetadata(field.name(), field.toTrinoType())).collect(toImmutableList());
        return new ConnectorTableMetadata(table.name(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle table = (LanceTableHandle) tableHandle;
        return table.manifest().fields().stream().collect(toImmutableMap(Field::name, field -> new LanceColumnHandle(field.id(), field.name(), field.toTrinoType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        LanceColumnHandle column = (LanceColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }
}
