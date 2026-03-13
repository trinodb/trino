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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Metadata implementation for Ducklake connector.
 * Provides read-only access to Ducklake tables via SQL catalog.
 */
public class DucklakeMetadata
        implements ConnectorMetadata
{
    private final DucklakeCatalog catalog;
    private final DucklakeTypeConverter typeConverter;

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        return catalog.listSchemas(snapshotId).stream()
                .map(DucklakeSchema::schemaName)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(tableName, "tableName is null");

        // Get snapshot ID (current or from version if time travel is requested)
        long snapshotId = catalog.getCurrentSnapshotId();
        // TODO: Handle startVersion for time travel queries

        Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
        if (table.isEmpty()) {
            return null;
        }

        return new DucklakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId(),
                snapshotId);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.columnName())
                        .setType(typeConverter.toTrinoType(column.columnType()))
                        .setNullable(column.nullsAllowed())
                        .build())
                .collect(toImmutableList());

        return new ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = catalog.getCurrentSnapshotId();

        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableList.of();
            }

            return catalog.listTables(schema.get().schemaId(), snapshotId).stream()
                    .map(table -> new SchemaTableName(schemaName.get(), table.tableName()))
                    .collect(toImmutableList());
        }

        // List all tables across all schemas
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (DucklakeSchema schema : catalog.listSchemas(snapshotId)) {
            for (DucklakeTable table : catalog.listTables(schema.schemaId(), snapshotId)) {
                tables.add(new SchemaTableName(schema.schemaName(), table.tableName()));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DucklakeColumn column : columns) {
            columnHandles.put(
                    column.columnName(),
                    new DucklakeColumnHandle(
                            column.columnId(),
                            column.columnName(),
                            typeConverter.toTrinoType(column.columnType()),
                            column.nullsAllowed()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        DucklakeColumnHandle ducklakeColumnHandle = (DucklakeColumnHandle) columnHandle;

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName())
                .setType(ducklakeColumnHandle.columnType())
                .setNullable(ducklakeColumnHandle.nullable())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        long snapshotId = catalog.getCurrentSnapshotId();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tables = prefix.getTable()
                .map(table -> List.of(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        for (SchemaTableName tableName : tables) {
            Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
            if (table.isPresent()) {
                List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.get().tableId(), snapshotId);
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map(column -> ColumnMetadata.builder()
                                        .setName(column.columnName())
                                        .setType(typeConverter.toTrinoType(column.columnType()))
                                        .setNullable(column.nullsAllowed())
                                        .build())
                                .collect(toImmutableList()));
            }
        }

        return columns.buildOrThrow();
    }
}
