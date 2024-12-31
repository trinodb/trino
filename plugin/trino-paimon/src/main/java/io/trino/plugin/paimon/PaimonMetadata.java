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
package io.trino.plugin.paimon;

import io.trino.plugin.paimon.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_FETCH_FAILED;
import static io.trino.plugin.paimon.PaimonTypeUtils.fieldNames;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public record PaimonMetadata(TrinoCatalog catalog)
        implements ConnectorMetadata
{
    public PaimonMetadata(TrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.databaseExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listDatabases(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        try {
            catalog.loadTable(session, tableName);
            return new PaimonTableHandle(tableName.getSchemaName(), tableName.getTableName(), TupleDomain.all());
        }
        catch (TableNotFoundException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        try {
            Table table = table(session, paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
            return tableMetadata(table, paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_METADATA_FETCH_FAILED, "Failed to get table metadata!", e);
        }
    }

    public ConnectorTableMetadata tableMetadata(Table table, String schemaName, String tableName)
    {
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(table),
                Collections.emptyMap(),
                table.comment());
    }

    public Table table(ConnectorSession session, String schemaName, String tableName)
    {
        return catalog.loadTable(session, SchemaTableName.schemaTableName(schemaName, tableName));
    }

    public List<ColumnMetadata> columnMetadatas(Table table)
    {
        return table.rowType().getFields().stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(PaimonTypeUtils.toTrinoType(column.type()))
                        .setNullable(column.type().isNullable())
                        .setComment(Optional.ofNullable(column.description()))
                        .build())
                .collect(toList());
    }

    public PaimonColumnHandle columnHandle(Table paimonTable, String field)
    {
        List<String> lowerCaseFieldNames = fieldNames(paimonTable.rowType());
        List<String> originFieldNames = paimonTable.rowType().getFieldNames();
        int index = lowerCaseFieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(format("Cannot find field %s in schema %s", field, lowerCaseFieldNames));
        }
        DataField dataField = paimonTable.rowType().getFields().get(index);
        return PaimonColumnHandle.of(originFieldNames.get(index), dataField.type(), dataField.id());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName.map(Collections::singletonList)
                .orElseGet(() -> catalog.listDatabases(session))
                .forEach(schema -> tables.addAll(listTables(session, schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        return catalog.listTables(session, Optional.of(schema)).stream()
                .map(table -> new SchemaTableName(schema, table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle handle = (PaimonTableHandle) tableHandle;
        Table table = table(session, handle.getSchemaName(), handle.getTableName());
        Map<String, ColumnHandle> columnHandles = new HashMap<>();
        for (ColumnMetadata column : columnMetadatas(table)) {
            columnHandles.put(column.getName(), columnHandle(table, column.getName()));
        }
        return columnHandles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        PaimonColumnHandle column = (PaimonColumnHandle) columnHandle;
        return new ColumnMetadata(column.columnName(), column.trinoType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return tableNames.stream()
                .map(table -> {
                    List<ColumnMetadata> columnMetadata;
                    try {
                        getTableHandle(session, table, Optional.empty(), Optional.empty());
                        Table paimonTable = table(session, table.getSchemaName(), table.getTableName());
                        columnMetadata = columnMetadatas(paimonTable);
                    }
                    catch (RuntimeException e) {
                        // Error when getting column metadata, return null
                        return Pair.of(table, (List<ColumnMetadata>) null);
                    }

                    return Pair.of(table, columnMetadata);
                }).filter(p -> p.getRight() != null)
                .collect(toMap(Pair::getLeft, Pair::getRight));
    }

    public void rollback()
    {
        // do nothing
    }
}
