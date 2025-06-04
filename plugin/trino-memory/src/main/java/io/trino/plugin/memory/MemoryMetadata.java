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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.memory.MemoryInsertTableHandle.InsertMode;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.SampleType.SYSTEM;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@ThreadSafe
public class MemoryMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    @GuardedBy("this")
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    @GuardedBy("this")
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    @GuardedBy("this")
    private final Map<Long, TableInfo> tables = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
    private final Map<SchemaFunctionName, Map<String, LanguageFunction>> functions = new HashMap<>();

    @Inject
    public MemoryMetadata(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.schemas.add(SCHEMA_NAME);
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
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
    public synchronized void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!schemas.contains(schemaName)) {
            throw new TrinoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        if (cascade) {
            Set<SchemaTableName> viewNames = views.keySet().stream()
                    .filter(view -> view.getSchemaName().equals(schemaName))
                    .collect(toImmutableSet());
            viewNames.forEach(viewName -> dropView(session, viewName));

            Set<SchemaTableName> tableNames = tables.values().stream()
                    .filter(table -> table.schemaName().equals(schemaName))
                    .map(TableInfo::getSchemaTableName)
                    .collect(toImmutableSet());
            tableNames.forEach(tableName -> dropTable(session, getTableHandle(session, tableName, Optional.empty(), Optional.empty())));
        }
        // DropSchemaTask has the same logic, but needs to check in connector side considering concurrent operations
        if (!isSchemaEmpty(schemaName)) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    @Override
    public synchronized void renameSchema(ConnectorSession session, String source, String target)
    {
        if (!schemas.remove(source)) {
            throw new SchemaNotFoundException(source);
        }
        schemas.add(target);

        for (Map.Entry<SchemaTableName, Long> table : tableIds.entrySet()) {
            if (table.getKey().getSchemaName().equals(source)) {
                tableIds.remove(table.getKey());
                tableIds.put(new SchemaTableName(target, table.getKey().getTableName()), table.getValue());
            }
        }

        for (TableInfo table : tables.values()) {
            if (table.schemaName().equals(source)) {
                tables.put(table.id(), new TableInfo(table.id(), target, table.tableName(), table.columns(), false, table.dataFragments(), table.comment()));
            }
        }

        for (Map.Entry<SchemaTableName, ConnectorViewDefinition> view : views.entrySet()) {
            if (view.getKey().getSchemaName().equals(source)) {
                views.remove(view.getKey());
                views.put(new SchemaTableName(target, view.getKey().getTableName()), view.getValue());
            }
        }

        for (Map.Entry<SchemaFunctionName, Map<String, LanguageFunction>> function : functions.entrySet()) {
            if (function.getKey().getSchemaName().equals(source)) {
                functions.remove(function.getKey());
                functions.put(new SchemaFunctionName(target, function.getKey().getFunctionName()), function.getValue());
            }
        }
    }

    @GuardedBy("this")
    private boolean isSchemaEmpty(String schemaName)
    {
        return tables.values().stream().noneMatch(table -> table.schemaName().equals(schemaName)) &&
                views.keySet().stream().noneMatch(view -> view.getSchemaName().equals(schemaName));
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        Long id = tableIds.get(schemaTableName);
        if (id == null) {
            return null;
        }

        return new MemoryTableHandle(id, OptionalLong.empty(), OptionalDouble.empty());
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return tables.get(handle.id()).getMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        views.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .forEach(builder::add);

        tables.values().stream()
                .filter(table -> schemaName.map(table.schemaName()::contentEquals).orElse(true))
                .map(TableInfo::getSchemaTableName)
                .forEach(builder::add);

        return builder.build();
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return tables.get(handle.id())
                .columns().stream()
                .collect(toImmutableMap(ColumnInfo::name, ColumnInfo::handle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return tables.get(handle.id())
                .getColumn(columnHandle)
                .getMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated streamTableColumns is not supported because streamRelationColumns is implemented instead");
    }

    @Override
    public synchronized Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationsColumns = Streams.concat(
                        tables.values().stream()
                                .map(tableInfo -> RelationColumnsMetadata.forTable(tableInfo.getSchemaTableName(), tableInfo.getMetadata().getColumns())),
                        views.entrySet().stream()
                                .map(entry -> RelationColumnsMetadata.forView(entry.getKey(), entry.getValue().getColumns())))
                .collect(toImmutableMap(RelationColumnsMetadata::name, identity()));
        return relationFilter.apply(relationsColumns.keySet()).stream()
                .map(relationsColumns::get)
                .iterator();
    }

    @Override
    public synchronized Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationCommentMetadata> relationsColumns = Streams.concat(
                        tables.values().stream()
                                .map(tableInfo -> RelationCommentMetadata.forRelation(tableInfo.getSchemaTableName(), tableInfo.getMetadata().getComment())),
                        views.entrySet().stream()
                                .map(entry -> RelationCommentMetadata.forRelation(entry.getKey(), entry.getValue().getComment())))
                .collect(toImmutableMap(RelationCommentMetadata::name, identity()));
        return relationFilter.apply(relationsColumns.keySet()).stream()
                .map(relationsColumns::get)
                .iterator();
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.remove(handle.id());
        if (info != null) {
            tableIds.remove(info.getSchemaTableName());
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);

        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        long tableId = handle.id();

        TableInfo oldInfo = tables.get(tableId);
        tables.put(tableId, new TableInfo(tableId, newTableName.getSchemaName(), newTableName.getTableName(), oldInfo.columns(), oldInfo.truncated(), oldInfo.dataFragments(), oldInfo.comment()));

        tableIds.remove(oldInfo.getSchemaTableName());
        tableIds.put(newTableName, tableId);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES, false);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized MemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long tableId = nextTableId.getAndIncrement();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            columns.add(new ColumnInfo(new MemoryColumnHandle(i, column.getType()), column.getName(), column.getType(), column.isNullable(), Optional.ofNullable(column.getComment())));
        }

        tableIds.put(tableMetadata.getTable(), tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columns.build(),
                false,
                new HashMap<>(),
                tableMetadata.getComment()));

        return new MemoryOutputTableHandle(tableId, ImmutableSet.copyOf(tableIds.values()));
    }

    @GuardedBy("this")
    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    @GuardedBy("this")
    private void checkTableNotExists(SchemaTableName tableName)
    {
        if (tableIds.containsKey(tableName)) {
            throw new TrinoException(ALREADY_EXISTS, format("Table [%s] already exists", tableName));
        }
        if (views.containsKey(tableName)) {
            throw new TrinoException(ALREADY_EXISTS, format("View [%s] already exists", tableName));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        MemoryOutputTableHandle memoryOutputHandle = (MemoryOutputTableHandle) tableHandle;

        updateRowsOnHosts(memoryOutputHandle.table(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized MemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo tableInfo = tables.get(memoryTableHandle.id());
        InsertMode mode = tableInfo.truncated() ? InsertMode.OVERWRITE : InsertMode.APPEND;
        tables.put(tableInfo.id(), new TableInfo(tableInfo.id(), tableInfo.schemaName(), tableInfo.tableName(), tableInfo.columns(), false, tableInfo.dataFragments(), tableInfo.comment()));
        return new MemoryInsertTableHandle(memoryTableHandle.id(), mode, ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryInsertTableHandle memoryInsertHandle = (MemoryInsertTableHandle) insertHandle;

        updateRowsOnHosts(memoryInsertHandle.table(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        long tableId = handle.id();
        TableInfo info = tables.get(handle.id());
        tables.put(tableId, new TableInfo(tableId, info.schemaName(), info.tableName(), info.columns(), true, ImmutableMap.of(), info.comment()));
    }

    @Override
    public synchronized void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        if (position instanceof ColumnPosition.First) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with FIRST clause");
        }
        if (position instanceof ColumnPosition.After) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with AFTER clause");
        }
        verify(position instanceof ColumnPosition.Last, "ColumnPosition must be instance of Last");

        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        long tableId = handle.id();
        TableInfo table = tables.get(handle.id());

        if (!column.isNullable() && !table.dataFragments().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, format("Unable to add NOT NULL column '%s' for non-empty table: %s", column.getName(), table.getSchemaTableName()));
        }

        List<ColumnInfo> columns = ImmutableList.<ColumnInfo>builderWithExpectedSize(table.columns().size() + 1)
                .addAll(table.columns())
                .add(new ColumnInfo(new MemoryColumnHandle(table.columns().size(), column.getType()), column.getName(), column.getType(), column.isNullable(), Optional.ofNullable(column.getComment())))
                .build();

        tables.put(tableId, new TableInfo(tableId, table.schemaName(), table.tableName(), columns, table.truncated(), table.dataFragments(), table.comment()));
    }

    @Override
    public synchronized void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String target)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        MemoryColumnHandle column = (MemoryColumnHandle) columnHandle;
        long tableId = handle.id();
        TableInfo table = tables.get(handle.id());

        List<ColumnInfo> columns = new ArrayList<>(table.columns());
        ColumnInfo columnInfo = columns.get(column.columnIndex());
        columns.set(column.columnIndex(), new ColumnInfo(columnInfo.handle(), target, columnInfo.type(), columnInfo.nullable(), columnInfo.comment()));

        tables.put(tableId, new TableInfo(tableId, table.schemaName(), table.tableName(), ImmutableList.copyOf(columns), table.truncated(), table.dataFragments(), table.comment()));
    }

    @Override
    public synchronized void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        MemoryColumnHandle column = (MemoryColumnHandle) columnHandle;
        long tableId = handle.id();
        TableInfo table = tables.get(handle.id());

        List<ColumnInfo> columns = new ArrayList<>(table.columns());
        ColumnInfo columnInfo = columns.get(column.columnIndex());
        columns.set(column.columnIndex(), new ColumnInfo(columnInfo.handle(), columnInfo.name(), columnInfo.type(), true, columnInfo.comment()));

        tables.put(tableId, new TableInfo(tableId, table.schemaName(), table.tableName(), ImmutableList.copyOf(columns), table.truncated(), table.dataFragments(), table.comment()));
    }

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        checkSchemaExists(viewName.getSchemaName());
        if (tableIds.containsKey(viewName) && !replace) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }

        if (replace) {
            views.put(viewName, definition);
        }
        else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
        tableIds.put(viewName, nextTableId.getAndIncrement());
    }

    @Override
    public synchronized void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
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
    public synchronized void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
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

    @Override
    public synchronized void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        checkSchemaExists(newViewName.getSchemaName());

        if (!tableIds.containsKey(viewName)) {
            throw new TrinoException(NOT_FOUND, "View not found: " + viewName);
        }

        if (tableIds.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + newViewName);
        }

        if (views.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + newViewName);
        }

        tableIds.put(newViewName, tableIds.remove(viewName));
        views.put(newViewName, views.remove(viewName));
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
        tableIds.remove(viewName);
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return views.keySet().stream()
                .filter(viewName -> schemaName.map(viewName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(Maps.filterKeys(views, prefix::matches));
    }

    @Override
    public synchronized Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(views.get(viewName));
    }

    @GuardedBy("this")
    private void updateRowsOnHosts(long tableId, Collection<Slice> fragments)
    {
        TableInfo info = tables.get(tableId);
        checkState(info != null, "Uninitialized tableId %s", tableId);

        Map<HostAddress, MemoryDataFragment> dataFragments = new HashMap<>(info.dataFragments());
        for (Slice fragment : fragments) {
            MemoryDataFragment memoryDataFragment = MemoryDataFragment.fromSlice(fragment);
            dataFragments.merge(memoryDataFragment.hostAddress(), memoryDataFragment, MemoryDataFragment::merge);
        }

        tables.put(tableId, new TableInfo(tableId, info.schemaName(), info.tableName(), info.columns(), info.truncated(), dataFragments, info.comment()));
    }

    public synchronized List<MemoryDataFragment> getDataFragments(long tableId)
    {
        return ImmutableList.copyOf(tables.get(tableId).dataFragments().values());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        List<MemoryDataFragment> dataFragments = getDataFragments(((MemoryTableHandle) tableHandle).id());
        long rows = dataFragments.stream()
                .mapToLong(MemoryDataFragment::rows)
                .sum();
        return TableStatistics.builder()
                .setRowCount(Estimate.of(rows))
                .build();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        if (table.limit().isPresent() && table.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new MemoryTableHandle(table.id(), OptionalLong.of(limit), OptionalDouble.empty()),
                true,
                true));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        if ((table.sampleRatio().isPresent() && table.sampleRatio().getAsDouble() == sampleRatio) || sampleType != SYSTEM || table.limit().isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new SampleApplicationResult<>(
                new MemoryTableHandle(table.id(), table.limit(), OptionalDouble.of(table.sampleRatio().orElse(1) * sampleRatio)),
                true));
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return true;
    }

    @Override
    public synchronized void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        MemoryTableHandle table = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.get(table.id());
        checkArgument(info != null, "Table not found");
        tables.put(table.id(), new TableInfo(table.id(), info.schemaName(), info.tableName(), info.columns(), info.truncated(), info.dataFragments(), comment));
    }

    @Override
    public synchronized void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Optional<String> comment)
    {
        MemoryTableHandle table = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.get(table.id());
        checkArgument(info != null, "Table not found");
        tables.put(
                table.id(),
                new TableInfo(
                        table.id(),
                        info.schemaName(),
                        info.tableName(),
                        info.columns().stream()
                                .map(tableColumn -> Objects.equals(tableColumn.handle(), columnHandle) ? new ColumnInfo(tableColumn.handle(), tableColumn.name(), tableColumn.getMetadata().getType(), tableColumn.nullable(), comment) : tableColumn)
                                .collect(toImmutableList()),
                        info.truncated(),
                        info.dataFragments(),
                        info.comment()));
    }

    @Override
    public synchronized Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return functions.entrySet().stream()
                .filter(entry -> entry.getKey().getSchemaName().equals(schemaName))
                .flatMap(entry -> entry.getValue().values().stream())
                .toList();
    }

    @Override
    public synchronized Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return functions.getOrDefault(name, Map.of()).values();
    }

    @Override
    public synchronized boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return functions.getOrDefault(name, Map.of()).containsKey(signatureToken);
    }

    @Override
    public synchronized void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        Map<String, LanguageFunction> map = functions.computeIfAbsent(name, _ -> new HashMap<>());
        if (!replace && map.containsKey(function.signatureToken())) {
            throw new TrinoException(ALREADY_EXISTS, "Function already exists");
        }
        map.put(function.signatureToken(), function);
    }

    @Override
    public synchronized void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        Map<String, LanguageFunction> map = functions.get(name);
        if ((map == null) || !map.containsKey(signatureToken)) {
            throw new TrinoException(NOT_FOUND, "Function not found");
        }
        map.remove(signatureToken);
        if (map.isEmpty()) {
            functions.remove(name);
        }
    }
}
