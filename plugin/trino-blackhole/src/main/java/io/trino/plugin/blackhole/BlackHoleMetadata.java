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
package io.trino.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.blackhole.BlackHoleConnector.DISTRIBUTED_ON;
import static io.trino.plugin.blackhole.BlackHoleConnector.FIELD_LENGTH_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.PAGE_PROCESSING_DELAY;
import static io.trino.plugin.blackhole.BlackHoleConnector.ROWS_PER_PAGE_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.SPLIT_COUNT_PROPERTY;
import static io.trino.plugin.blackhole.BlackHolePageSourceProvider.isNumericType;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class BlackHoleMetadata
        implements ConnectorMetadata
{
    private static final String SCHEMA_NAME = "default";

    private final List<String> schemas = new ArrayList<>();
    private final Map<SchemaTableName, BlackHoleTableHandle> tables = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new ConcurrentHashMap<>();

    public BlackHoleMetadata()
    {
        schemas.add(SCHEMA_NAME);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (schemas.contains(schemaName)) {
            throw new TrinoException(ALREADY_EXISTS, format("Schema [%s] already exists", schemaName));
        }
        schemas.add(schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return tables.get(tableName);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        return new ConnectorAnalyzeMetadata(tableHandle, TableStatisticsMetadata.empty());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics) {}

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = (BlackHoleTableHandle) tableHandle;
        return blackHoleTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        // Deduplicate with set because state may change concurrently
        return ImmutableSet.<SchemaTableName>builder()
                .addAll(tables.values().stream()
                        .filter(table -> schemaName.isEmpty() || table.schemaName().equals(schemaName.get()))
                        .map(BlackHoleTableHandle::toSchemaTableName)
                        .collect(toList()))
                .addAll(listViews(session, schemaName))
                .addAll(listMaterializedViews(session, schemaName))
                .build().asList();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = (BlackHoleTableHandle) tableHandle;
        return blackHoleTableHandle.columnHandles().stream()
                .collect(toImmutableMap(BlackHoleColumnHandle::name, Function.identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        BlackHoleColumnHandle blackHoleColumnHandle = (BlackHoleColumnHandle) columnHandle;
        return blackHoleColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .map(handle -> TableColumnsMetadata.forTable(handle.toSchemaTableName(), handle.toTableMetadata().getColumns()))
                .iterator();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;
        List<BlackHoleColumnHandle> columns = ImmutableList.<BlackHoleColumnHandle>builderWithExpectedSize(table.columnHandles().size() + 1)
                .addAll(table.columnHandles())
                .add(new BlackHoleColumnHandle(column.getName(), column.getType()))
                .build();

        tables.put(table.toSchemaTableName(), table.withColumnHandles(columns));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;
        BlackHoleColumnHandle column = (BlackHoleColumnHandle) columnHandle;

        List<BlackHoleColumnHandle> columns = table.columnHandles().stream()
                .filter(c -> !c.name().equals(column.name()))
                .collect(toImmutableList());

        tables.put(table.toSchemaTableName(), table.withColumnHandles(columns));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String target)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;
        BlackHoleColumnHandle column = (BlackHoleColumnHandle) columnHandle;

        List<BlackHoleColumnHandle> columns = table.columnHandles().stream()
                .map(c -> c.name().equals(column.name()) ? new BlackHoleColumnHandle(target, c.columnType()) : c)
                .collect(toImmutableList());

        tables.put(table.toSchemaTableName(), table.withColumnHandles(columns));
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Type type)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;
        BlackHoleColumnHandle column = (BlackHoleColumnHandle) columnHandle;
        List<BlackHoleColumnHandle> columns = new ArrayList<>(table.columnHandles());
        columns.set(columns.indexOf(column), new BlackHoleColumnHandle(column.name(), type));

        tables.put(table.toSchemaTableName(), table.withColumnHandles(ImmutableList.copyOf(columns)));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle table = (BlackHoleTableHandle) tableHandle;
        TableStatistics.Builder tableStats = TableStatistics.builder();

        double rows = (double) table.splitCount() * table.pagesPerSplit() * table.rowsPerPage();
        tableStats.setRowCount(Estimate.of(rows));

        for (BlackHoleColumnHandle column : table.columnHandles()) {
            ColumnStatistics.Builder stats = ColumnStatistics.builder()
                    .setDistinctValuesCount(Estimate.of(1))
                    .setNullsFraction(Estimate.of(0));
            if (isNumericType(column.columnType())) {
                stats.setRange(new DoubleRange(0, 0));
            }
            tableStats.setColumnStatistics(column, stats.build());
        }

        return tableStats.build();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = (BlackHoleTableHandle) tableHandle;
        tables.remove(blackHoleTableHandle.toSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        BlackHoleTableHandle oldTableHandle = (BlackHoleTableHandle) tableHandle;
        BlackHoleTableHandle newTableHandle = new BlackHoleTableHandle(
                oldTableHandle.schemaName(),
                newTableName.getTableName(),
                oldTableHandle.columnHandles(),
                oldTableHandle.splitCount(),
                oldTableHandle.pagesPerSplit(),
                oldTableHandle.rowsPerPage(),
                oldTableHandle.fieldsLength(),
                oldTableHandle.pageProcessingDelay());
        tables.remove(oldTableHandle.toSchemaTableName());
        tables.put(newTableName, newTableHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES, saveMode == REPLACE);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession connectorSession, ConnectorTableMetadata tableMetadata)
    {
        @SuppressWarnings("unchecked")
        List<String> distributeColumns = (List<String>) tableMetadata.getProperties().get(DISTRIBUTED_ON);
        if (distributeColumns.isEmpty()) {
            return Optional.empty();
        }

        Set<String> undefinedColumns = Sets.difference(
                ImmutableSet.copyOf(distributeColumns),
                tableMetadata.getColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toSet()));
        if (!undefinedColumns.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Distribute columns not defined on table: " + undefinedColumns);
        }

        return Optional.of(new ConnectorTableLayout(BlackHolePartitioningHandle.INSTANCE, distributeColumns, true));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        if (!replace && (tables.containsKey(tableMetadata.getTable()) || views.containsKey(tableMetadata.getTable()))) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, "Table %s already exists".formatted(tableMetadata.getTable()));
        }
        int splitCount = (Integer) tableMetadata.getProperties().get(SPLIT_COUNT_PROPERTY);
        int pagesPerSplit = (Integer) tableMetadata.getProperties().get(PAGES_PER_SPLIT_PROPERTY);
        int rowsPerPage = (Integer) tableMetadata.getProperties().get(ROWS_PER_PAGE_PROPERTY);
        int fieldsLength = (Integer) tableMetadata.getProperties().get(FIELD_LENGTH_PROPERTY);

        if (splitCount < 0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, SPLIT_COUNT_PROPERTY + " property is negative");
        }
        if (pagesPerSplit < 0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, PAGES_PER_SPLIT_PROPERTY + " property is negative");
        }
        if (rowsPerPage < 0) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, ROWS_PER_PAGE_PROPERTY + " property is negative");
        }

        if (((splitCount > 0) || (pagesPerSplit > 0) || (rowsPerPage > 0)) &&
                ((splitCount == 0) || (pagesPerSplit == 0) || (rowsPerPage == 0))) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("All properties [%s, %s, %s] must be set if any are set",
                    SPLIT_COUNT_PROPERTY, PAGES_PER_SPLIT_PROPERTY, ROWS_PER_PAGE_PROPERTY));
        }

        Duration pageProcessingDelay = (Duration) tableMetadata.getProperties().get(PAGE_PROCESSING_DELAY);

        BlackHoleTableHandle handle = new BlackHoleTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream()
                        .map(column -> new BlackHoleColumnHandle(column.getName(), column.getType()))
                        .collect(toList()),
                splitCount,
                pagesPerSplit,
                rowsPerPage,
                fieldsLength,
                pageProcessingDelay);
        return new BlackHoleOutputTableHandle(handle, pageProcessingDelay);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        BlackHoleOutputTableHandle blackHoleOutputTableHandle = (BlackHoleOutputTableHandle) tableHandle;
        BlackHoleTableHandle table = blackHoleOutputTableHandle.table();
        tables.put(table.toSchemaTableName(), table);
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        BlackHoleTableHandle handle = (BlackHoleTableHandle) tableHandle;
        return new BlackHoleInsertTableHandle(handle.pageProcessingDelay());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new BlackHoleColumnHandle("row_id", BIGINT);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        return new BlackHoleMergeTableHandle((BlackHoleTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {}

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle) {}

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        views.put(viewName, definition);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        views.remove(viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return ImmutableList.copyOf(views.keySet());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(views.get(viewName));
    }

    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        ConnectorViewDefinition view = getView(session, viewName).orElseThrow(() -> new ViewNotFoundException(viewName));
        views.put(viewName, new ConnectorViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns(),
                comment,
                view.getOwner(),
                view.isRunAsInvoker(),
                view.getPath()));
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        ConnectorViewDefinition view = getView(session, viewName).orElseThrow(() -> new ViewNotFoundException(viewName));
        views.put(viewName, new ConnectorViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns().stream()
                        .map(currentViewColumn -> Objects.equals(columnName, currentViewColumn.getName()) ? new ConnectorViewDefinition.ViewColumn(currentViewColumn.getName(), currentViewColumn.getType(), comment) : currentViewColumn)
                        .collect(toImmutableList()),
                view.getComment(),
                view.getOwner(),
                view.isRunAsInvoker(),
                view.getPath()));
    }
}
