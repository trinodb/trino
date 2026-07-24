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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DorisMetadata
        implements ConnectorMetadata
{
    private final DorisMetadataClient metadataClient;
    private final DorisTypeMapper typeMapper;

    @Inject
    public DorisMetadata(DorisMetadataClient metadataClient, DorisTypeMapper typeMapper)
    {
        this.metadataClient = requireNonNull(metadataClient, "metadataClient is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadataClient.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return metadataClient.getTable(session, tableName)
                .map(remoteTable -> new DorisTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        remoteTable.remoteSchemaName(),
                        remoteTable.remoteTableName(),
                        remoteTable.relationType()))
                .orElse(null);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return metadataClient.listTables(session, optionalSchemaName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle tableHandle = (DorisTableHandle) table;
        return getRemoteTable(session, tableHandle)
                .map(this::toTableMetadata)
                .orElse(null);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;
        DorisRemoteTable remoteTable = getRemoteTable(session, dorisTableHandle)
                .orElseThrow(() -> new TableNotFoundException(dorisTableHandle.toSchemaTableName()));

        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        for (DorisColumnHandle columnHandle : toColumnHandles(remoteTable.columns())) {
            columnHandles.put(columnHandle.columnName(), columnHandle);
        }
        return Collections.unmodifiableMap(columnHandles);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        requireNonNull(relationFilter, "relationFilter is null");

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new LinkedHashMap<>();
        for (SchemaTableName tableName : listTables(session, schemaName)) {
            getRemoteTable(session, tableName).ifPresent(remoteTable -> {
                List<ColumnMetadata> columns = toColumnMetadata(remoteTable.columns());
                if (remoteTable.relationType() == DorisRelationType.VIEW) {
                    relationColumns.put(tableName, RelationColumnsMetadata.forView(tableName, toViewColumns(columns)));
                }
                else {
                    relationColumns.put(tableName, RelationColumnsMetadata.forTable(tableName, columns));
                }
            });
        }
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DorisColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        DorisTableHandle handle = (DorisTableHandle) table;
        OptionalLong rowCount = metadataClient.getTableRowCount(session, handle.toSchemaTableName());
        if (rowCount.isEmpty()) {
            return TableStatistics.empty();
        }

        return TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount.getAsLong()))
                .build();
    }

    private Optional<DorisRemoteTable> getRemoteTable(ConnectorSession session, DorisTableHandle tableHandle)
    {
        return getRemoteTable(session, tableHandle.toSchemaTableName());
    }

    private Optional<DorisRemoteTable> getRemoteTable(ConnectorSession session, SchemaTableName tableName)
    {
        return metadataClient.getTable(session, tableName);
    }

    private ConnectorTableMetadata toTableMetadata(DorisRemoteTable remoteTable)
    {
        return new ConnectorTableMetadata(remoteTable.schemaTableName(), toColumnMetadata(remoteTable.columns()));
    }

    private List<ColumnMetadata> toColumnMetadata(List<DorisRemoteColumn> columns)
    {
        List<ColumnMetadata> columnMetadata = new ArrayList<>(columns.size());
        for (DorisRemoteColumn column : columns) {
            columnMetadata.add(new ColumnMetadata(column.columnName(), typeMapper.toTrinoType(column)));
        }
        return List.copyOf(columnMetadata);
    }

    private static List<ConnectorViewDefinition.ViewColumn> toViewColumns(List<ColumnMetadata> columns)
    {
        List<ConnectorViewDefinition.ViewColumn> viewColumns = new ArrayList<>(columns.size());
        for (ColumnMetadata column : columns) {
            viewColumns.add(new ConnectorViewDefinition.ViewColumn(
                    column.getName(),
                    column.getType().getTypeId(),
                    column.getComment()));
        }
        return List.copyOf(viewColumns);
    }

    private List<DorisColumnHandle> toColumnHandles(List<DorisRemoteColumn> columns)
    {
        List<DorisColumnHandle> columnHandles = new ArrayList<>(columns.size());
        for (DorisRemoteColumn column : columns) {
            columnHandles.add(new DorisColumnHandle(
                    column.columnName(),
                    typeMapper.toTrinoType(column),
                    Math.max(0, column.ordinalPosition() - 1)));
        }
        return List.copyOf(columnHandles);
    }
}
