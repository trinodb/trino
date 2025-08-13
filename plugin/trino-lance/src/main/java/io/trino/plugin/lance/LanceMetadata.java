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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class LanceMetadata
        implements ConnectorMetadata
{
    public static final int GET_METADATA_BATCH_SIZE = 1000;

    private final TrinoCatalog catalog;

    @Inject
    public LanceMetadata(TrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    private static long getSnapshotIdFromVersion(ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        return switch (version.getPointerType()) {
            // TODO: list and search versions to do temporal time travel
            case TEMPORAL -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables with temporal version");
            case TARGET_ID -> {
                if (versionType != BIGINT) {
                    throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables: unsupported type for table version " + versionType.getDisplayName());
                }
                // TODO: support String type target id
                yield (long) version.getVersion();
            }
        };
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
                List<ColumnMetadata> columns = manifest.getFields().stream().map(field -> new ColumnMetadata(field.getName(), field.toTrinoType())).collect(toImmutableList());
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
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with startRowPosition version is not supported");
        }
        Optional<BaseTable> loadedTable = catalog.loadTable(session, tableName);
        if (loadedTable.isEmpty()) {
            return null;
        }
        BaseTable baseTable = loadedTable.get();
        Optional<Long> version;
        if (endVersion.isPresent()) {
            version = Optional.of(getSnapshotIdFromVersion(endVersion.get()));
        }
        else {
            version = Optional.empty();
        }
        return new LanceTableHandle(tableName, baseTable.loadManifest(version), baseTable.getTableLocation().toString());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof LanceTableHandle);
        LanceTableHandle table = (LanceTableHandle) tableHandle;

        List<ColumnMetadata> columns = table.manifest().getFields().stream().map(field -> new ColumnMetadata(field.getName(), field.toTrinoType())).collect(toImmutableList());
        return new ConnectorTableMetadata(table.name(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof LanceTableHandle);
        LanceTableHandle table = (LanceTableHandle) tableHandle;
        return table.manifest().getFields().stream().collect(toImmutableMap(Field::getName, field -> new LanceColumnHandle(field.getId(), field.getName(), field.toTrinoType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkArgument(tableHandle instanceof LanceTableHandle);
        LanceColumnHandle column = (LanceColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), column.getType());
    }
}
