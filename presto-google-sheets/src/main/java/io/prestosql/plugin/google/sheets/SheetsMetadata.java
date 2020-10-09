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
package io.prestosql.plugin.google.sheets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_SCHEMA_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SheetsMetadata
        implements ConnectorMetadata
{
    private final SheetsClient sheetsClient;
    private static final List<String> SCHEMAS = ImmutableList.of("default");

    @Inject
    public SheetsMetadata(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "client is null");
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
    public SheetsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        try {
            Optional<SheetsTable> table = sheetsClient.getTable(tableName.getTableName());
            if (table.isEmpty()) {
                return null;
            }
            return new SheetsTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (PrestoException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        Optional<ConnectorTableMetadata> connectorTableMetadata = getTableMetadata(((SheetsTableHandle) table).toSchemaTableName());
        if (connectorTableMetadata.isEmpty()) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Metadata not found for table " + ((SheetsTableHandle) table).getTableName());
        }
        return connectorTableMetadata.get();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        Optional<SheetsTable> table = sheetsClient.getTable(sheetsTableHandle.getTableName());
        if (table.isEmpty()) {
            throw new TableNotFoundException(sheetsTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.get().getColumnsMetadata()) {
            columnHandles.put(column.getName(), new SheetsColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
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
        return columns.build();
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
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isEmpty()) {
            throw new PrestoException(SHEETS_UNKNOWN_SCHEMA_ERROR, "Schema not present - " + schemaName);
        }
        if (schemaName.get().equalsIgnoreCase("information_schema")) {
            return ImmutableList.of();
        }
        Set<String> tables = sheetsClient.getTableNames();
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        tables.forEach(t -> schemaTableNames.add(new SchemaTableName(schemaName.get(), t)));
        return schemaTableNames;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle sheetsTableHandle = (SheetsTableHandle) tableHandle;
        return new SheetsInsertTableHandle(sheetsTableHandle.getSchemaName(),
                sheetsTableHandle.getTableName(), buildColumnHandles(sheetsTableHandle));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        List<SheetsColumnHandle> columns = buildColumnHandles(tableMetadata);
        sheetsClient.createSheet(tableMetadata.getTable().getTableName(), columns);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        List<SheetsColumnHandle> columns = buildColumnHandles(tableMetadata);
        sheetsClient.createSheet(tableMetadata.getTable().getTableName(), columns);
        return new SheetsOutputTableHandle(tableMetadata.getTable(), columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SheetsTableHandle table = (SheetsTableHandle) tableHandle;
        sheetsClient.dropSheet(table.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        SheetsTableHandle handle = (SheetsTableHandle) tableHandle;
        sheetsClient.addColumn(handle.getTableName(), Arrays.asList(Arrays.asList(column.getName())), column.getType());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((SheetsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    private static List<SheetsColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        AtomicInteger counter = new AtomicInteger(0);
        return tableMetadata.getColumns().stream()
                .map(m -> new SheetsColumnHandle(m.getName(), m.getType(), counter.getAndIncrement()))
                .collect(toList());
    }

    private List<SheetsColumnHandle> buildColumnHandles(SheetsTableHandle sheetsTableHandle)
    {
        AtomicInteger counter = new AtomicInteger(0);
        Optional<SheetsTable> table = sheetsClient.getTable(sheetsTableHandle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(sheetsTableHandle.toSchemaTableName());
        }
        return table.get().getColumnsMetadata().stream()
                .map(m -> new SheetsColumnHandle(m.getName(), m.getType(), counter.getAndIncrement()))
                .collect(toList());
    }
}
