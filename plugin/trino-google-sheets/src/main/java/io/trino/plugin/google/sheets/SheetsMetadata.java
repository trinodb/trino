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
package io.trino.plugin.google.sheets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.google.sheets.SheetsConnectorTableHandle.tableNotFound;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static io.trino.plugin.google.sheets.ptf.Sheet.SheetFunctionHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SheetsMetadata
        implements ConnectorMetadata
{
    private final SheetsClient sheetsClient;
    private static final List<String> SCHEMAS = ImmutableList.of("default");

    @Inject
    public SheetsMetadata(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return SCHEMAS;
    }

    @Override
    public SheetsNamedTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }

        return new SheetsNamedTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SheetsConnectorTableHandle tableHandle = (SheetsConnectorTableHandle) table;
        SheetsTable sheetsTable = sheetsClient.getTable(tableHandle)
                .orElseThrow(() -> new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Metadata not found for table " + tableNotFound(tableHandle)));
        return new ConnectorTableMetadata(getSchemaTableName(tableHandle), sheetsTable.getColumnsMetadata());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsConnectorTableHandle sheetsTableHandle = (SheetsConnectorTableHandle) tableHandle;
        SheetsTable table = sheetsClient.getTable(sheetsTableHandle)
                .orElseThrow(() -> tableNotFound(sheetsTableHandle));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new SheetsColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata.isPresent()) {
                columns.put(tableName, tableMetadata.get().getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return Optional.empty();
        }
        Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
        if (table.isPresent()) {
            return Optional.of(new ConnectorTableMetadata(tableName, table.get().getColumnsMetadata()));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schema = schemaName.orElseGet(() -> getOnlyElement(SCHEMAS));

        if (listSchemaNames().contains(schema)) {
            return sheetsClient.getTableNames().stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SheetsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        if (!(tableHandle instanceof SheetsNamedTableHandle namedTableHandle)) {
            throw new TrinoException(NOT_SUPPORTED, format("Can only insert into named tables. Found table handle type: %s", tableHandle));
        }

        SheetsTable table = sheetsClient.getTable(namedTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(namedTableHandle.getSchemaTableName()));

        List<SheetsColumnHandle> columnHandles = new ArrayList<>(table.getColumnsMetadata().size());
        for (int id = 0; id < table.getColumnsMetadata().size(); id++) {
            columnHandles.add(new SheetsColumnHandle(table.getColumnsMetadata().get(id).getName(), table.getColumnsMetadata().get(id).getType(), id));
        }
        return new SheetsConnectorInsertTableHandle(namedTableHandle.getTableName(), columnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof SheetFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((SheetFunctionHandle) handle).getTableHandle();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    private static SchemaTableName getSchemaTableName(SheetsConnectorTableHandle handle)
    {
        if (handle instanceof SheetsNamedTableHandle namedTableHandle) {
            return new SchemaTableName(namedTableHandle.getSchemaName(), namedTableHandle.getTableName());
        }
        if (handle instanceof SheetsSheetTableHandle) {
            // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
            return new SchemaTableName("_generated", "_generated");
        }
        throw new IllegalStateException("Found unexpected table handle type " + handle);
    }
}
