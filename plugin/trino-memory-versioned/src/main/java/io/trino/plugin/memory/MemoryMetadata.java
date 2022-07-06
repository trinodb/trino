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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.trino.plugin.base.CatalogName;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTableVersioningLayout;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    private static final Type VERSION_TYPE = new ArrayType(BIGINT);
    private static final String DELETE_TABLE_SUFFIX = "$delete";

    private final CatalogName catalogName;
    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    private final Map<Long, TableInfo> tables = new HashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
    private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();

    @Inject
    public MemoryMetadata(CatalogName catalogName, NodeManager nodeManager, TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManage is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
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
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new TrinoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        // DropSchemaTask has the same logic, but needs to check in connector side considering concurrent operations
        if (!isSchemaEmpty(schemaName)) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    private boolean isSchemaEmpty(String schemaName)
    {
        if (tables.values().stream()
                .anyMatch(table -> table.getSchemaName().equals(schemaName))) {
            return false;
        }

        if (views.keySet().stream()
                .anyMatch(view -> view.getSchemaName().equals(schemaName))) {
            return false;
        }

        return true;
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        String tableName = schemaTableName.getTableName();
        boolean deletedRows = tableName.endsWith(DELETE_TABLE_SUFFIX);
        if (deletedRows) {
            schemaTableName = new SchemaTableName(
                    schemaTableName.getSchemaName(),
                    tableName.substring(0, tableName.length() - DELETE_TABLE_SUFFIX.length()));
        }
        Long id = tableIds.get(schemaTableName);
        if (id == null) {
            return null;
        }

        TableInfo info = tables.get(id);
        if (info == null) {
            checkState(materializedViews.containsKey(schemaTableName));
            // this is MV
            return null;
        }

        if (info.getKeyColumnIndex().isEmpty()) {
            if (deletedRows) {
                throw new TrinoException(NOT_SUPPORTED, "Delete table only available for versioned tables");
            }

            if (startVersion.isPresent() || endVersion.isPresent()) {
                throw new TrinoException(NOT_SUPPORTED, "Table is not versioned");
            }
        }

        Optional<Set<Long>> startVersions = startVersion
                .map(this::getVersions);
        Optional<Set<Long>> endVersions = endVersion
                .map(this::getVersions);

        // choose transactions that were committed between start and end version
        Set<Long> rangeVersions = new HashSet<>(endVersions.orElse(info.getCommittedVersions()));
        startVersions.stream()
                .peek(versions -> checkState(rangeVersions.containsAll(versions)))
                .forEach(rangeVersions::removeAll);

        return new MemoryTableHandle(
                id,
                schemaTableName,
                info.getKeyColumnIndex().map(ignored -> rangeVersions),
                Optional.empty(),
                deletedRows,
                info.getMaterializedViewId());
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.get(handle.getId());
        if (handle.isDeletedRows()) {
            return new ConnectorTableMetadata(
                    new SchemaTableName(info.getSchemaName(), info.getTableName() + DELETE_TABLE_SUFFIX),
                    ImmutableList.of(info.getColumns().get(info.getKeyColumnIndex().orElseThrow()).getMetadata()));
        }
        return tables.get(handle.getId()).getMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        views.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .forEach(builder::add);

        tables.values().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .map(TableInfo::getSchemaTableName)
                .forEach(builder::add);

        return builder.build();
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.get(handle.getId());
        if (handle.isDeletedRows()) {
            ColumnInfo columnInfo = info.getColumns().get(info.getKeyColumnIndex().orElseThrow());
            return ImmutableMap.of(columnInfo.getName(), columnInfo.getHandle());
        }
        return info.getColumns().stream()
                .collect(toImmutableMap(ColumnInfo::getName, ColumnInfo::getHandle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.get(handle.getId());
        if (handle.isDeletedRows()) {
            return info.getColumns().get(info.getKeyColumnIndex().orElseThrow()).getMetadata();
        }
        return info
                .getColumn((MemoryColumnHandle) columnHandle)
                .getMetadata();
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .collect(toImmutableMap(TableInfo::getSchemaTableName, handle -> handle.getMetadata().getColumns()));
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = tables.remove(handle.getId());
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
        long tableId = handle.getId();

        TableInfo oldInfo = tables.get(tableId);
        tables.put(tableId, new TableInfo(tableId, newTableName.getSchemaName(), newTableName.getTableName(), oldInfo.getColumns(), oldInfo.getKeyColumnIndex(), oldInfo.getHostAddress(), oldInfo.getMaterializedViewId()));

        tableIds.remove(oldInfo.getSchemaTableName());
        tableIds.put(newTableName, tableId);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized MemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long tableId = nextTableId.getAndIncrement();

        ImmutableList.Builder<ColumnInfo> columnsBuilder = ImmutableList.builder();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            if (column.getComment() != null) {
                throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
            }
            columnsBuilder.add(new ColumnInfo(new MemoryColumnHandle(i, false), column.getName(), column.getType()));
        }

        List<ColumnInfo> columns = columnsBuilder.build();
        Optional<Integer> keyColumnIndex = MemoryTableProperties.getKeyColumn(tableMetadata.getProperties())
                .map(keyColumnName -> columns.stream()
                        .filter(column -> column.getName().equals(keyColumnName))
                        .map(columns::indexOf)
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(INVALID_TABLE_PROPERTY, format("Key column '%s' not present", keyColumnName))));
        keyColumnIndex.ifPresent(index -> {
            Type type = columns.get(index).getMetadata().getType();
            if (!type.equals(BIGINT) && !type.equals(INTEGER)) {
                throw new TrinoException(TYPE_MISMATCH, "Only BIGINT and INTEGER key columns are supported");
            }
        });

        tableIds.put(tableMetadata.getTable(), tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columns,
                keyColumnIndex,
                layout.flatMap((Function<ConnectorTableLayout, Optional<?>>) ConnectorTableLayout::getPartitioning)
                        .map(p -> (MemoryPartitioningHandle) p)
                        .map(MemoryPartitioningHandle::getHostAddress)
                        .orElseGet(this::getWorkerAddress),
                Optional.empty()));

        return new MemoryOutputTableHandle(tableId, ImmutableSet.copyOf(tableIds.values()), getNextVersion(tableId), keyColumnIndex);
    }

    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

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
        MemoryOutputTableHandle handle = (MemoryOutputTableHandle) tableHandle;
        commitVersion(handle.getTable(), handle.getVersion());
        return Optional.empty();
    }

    @Override
    public synchronized MemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo info = requireNonNull(tables.get(memoryTableHandle.getId()), "tableInfo is null");
        return new MemoryInsertTableHandle(memoryTableHandle.getId(), ImmutableSet.copyOf(tableIds.values()), getNextVersion(memoryTableHandle.getId()), info.getKeyColumnIndex(), Optional.empty());
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryInsertTableHandle handle = (MemoryInsertTableHandle) insertHandle;
        commitVersion(handle.getTable(), handle.getVersion());
        return Optional.empty();
    }

    private synchronized Optional<Long> getNextVersion(long tableId)
    {
        TableInfo info = requireNonNull(tables.get(tableId), "tables is null");
        return info.getKeyColumnIndex().map(ignored -> info.getNextVersion());
    }

    @Override
    public synchronized ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo info = requireNonNull(tables.get(memoryTableHandle.getId()), "tableInfo is null");
        if (info.getKeyColumnIndex().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This table does not support deletes");
        }
        return new MemoryTableHandle(
                memoryTableHandle.getId(),
                memoryTableHandle.getTableName(),
                memoryTableHandle.getVersions(),
                Optional.of(info.getNextVersion()),
                false,
                memoryTableHandle.getMaterializedViewId());
    }

    @Override
    public synchronized void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        commitVersion(handle.getId(), handle.getUpdateVersion());
    }

    @Override
    public synchronized ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo info = requireNonNull(tables.get(memoryTableHandle.getId()), "tableInfo is null");
        if (info.getKeyColumnIndex().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This table does not support deletes");
        }
        return info.getKeyColumnIndex()
                .map(index -> new MemoryColumnHandle(index, true))
                .orElseThrow();
    }

    private synchronized void commitVersion(long tableId, Optional<Long> version)
    {
        if (version.isEmpty()) {
            return;
        }

        TableInfo info = requireNonNull(tables.get(tableId), "tables is null");
        info.commitVersion(version.get());
    }

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
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

    @Override
    public synchronized void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        checkSchemaExists(viewName.getSchemaName());
        if (tableIds.containsKey(viewName) && !replace) {
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
        }

        checkArgument(definition.getSourceTableVersions().isEmpty());
        Optional<ConnectorMaterializedViewDefinition.VersioningLayout> layoutOptional = definition.getVersioningLayout()
                .filter(ConnectorMaterializedViewDefinition.VersioningLayout::isUnique)
                .filter(l -> l.getVersioningColumns().size() == 1)
                .filter(l -> {
                    TypeId type = getOnlyElement(l.getVersioningColumns()).getType();
                    return type.equals(BIGINT.getTypeId()) || type.equals(INTEGER.getTypeId());
                });

        ImmutableList.Builder<ColumnInfo> columnsBuilder = ImmutableList.builder();
        for (int i = 0; i < definition.getColumns().size(); ++i) {
            ConnectorMaterializedViewDefinition.Column column = definition.getColumns().get(i);
            columnsBuilder.add(new ColumnInfo(new MemoryColumnHandle(i, false), column.getName(), typeManager.getType(column.getType())));
        }

        Optional<Integer> keyColumnIndex = Optional.empty();
        if (layoutOptional.isPresent()) {
            List<String> storageColumnNames = definition.getColumns().stream()
                    .map(ConnectorMaterializedViewDefinition.Column::getName)
                    .collect(toImmutableList());
            ConnectorMaterializedViewDefinition.Column keyColumn = getOnlyElement(layoutOptional.get().getVersioningColumns());
            if (storageColumnNames.stream().anyMatch(keyColumn.getName()::equalsIgnoreCase)) {
                keyColumnIndex = Optional.of(Iterables.indexOf(storageColumnNames, keyColumn.getName()::equalsIgnoreCase));
            }
            else {
                keyColumnIndex = Optional.of(storageColumnNames.size());
                columnsBuilder.add(new ColumnInfo(new MemoryColumnHandle(storageColumnNames.size(), false), keyColumn.getName(), typeManager.getType(keyColumn.getType())));
            }
        }

        SchemaTableName storageTableName = new SchemaTableName(viewName.getSchemaName(), viewName.getTableName() + "_storage");
        if (tableIds.containsKey(storageTableName)) {
            throw new TrinoException(ALREADY_EXISTS, "Storage table already exists");
        }

        long materializedViewId = nextTableId.getAndIncrement();
        long storageTableId = nextTableId.getAndIncrement();
        tableIds.put(storageTableName, storageTableId);
        tables.put(
                storageTableId,
                new TableInfo(
                        storageTableId,
                        storageTableName.getSchemaName(),
                        storageTableName.getTableName(),
                        columnsBuilder.build(),
                        keyColumnIndex,
                        getWorkerAddress(),
                        Optional.of(materializedViewId)));

        ConnectorMaterializedViewDefinition newDefinition = new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName.getSchemaName(), storageTableName.getTableName())),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                definition.getComment(),
                definition.getOwner(),
                definition.getProperties(),
                layoutOptional,
                definition.getSourceTableVersions());

        if (replace) {
            materializedViews.put(viewName, newDefinition);
        }
        else if (materializedViews.putIfAbsent(viewName, newDefinition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
        }
        tableIds.put(viewName, materializedViewId);
    }

    @Override
    public synchronized void renameMaterializedView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        checkSchemaExists(newViewName.getSchemaName());

        if (!tableIds.containsKey(viewName)) {
            throw new TrinoException(NOT_FOUND, "Materialized view not found: " + viewName);
        }

        if (tableIds.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + newViewName);
        }

        if (materializedViews.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + newViewName);
        }

        tableIds.put(newViewName, tableIds.remove(viewName));
        materializedViews.put(newViewName, materializedViews.remove(viewName));
    }

    @Override
    public synchronized void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        if (materializedViews.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
        materializedViews.remove(viewName);
    }

    @Override
    public synchronized List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return materializedViews.keySet().stream()
                .filter(viewName -> schemaName.map(viewName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return materializedViews.entrySet().stream()
                .filter(entry -> prefix.matches(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, entry -> addComments(entry.getValue())));
    }

    @Override
    public synchronized Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(materializedViews.get(viewName)).map(this::addComments);
    }

    private ConnectorMaterializedViewDefinition addComments(ConnectorMaterializedViewDefinition definition)
    {
        Optional<Map<CatalogSchemaTableName, ConnectorTableVersion>> tableVersions = definition.getSourceTableVersions()
                .map(versions -> versions.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> {
                            ConnectorTableVersion version = entry.getValue();
                            // convert structured version for better readability
                            if (version.getVersionType().equals(VERSION_TYPE)) {
                                return new ConnectorTableVersion(version.getPointerType(), version.getVersionType(), getVersions(version));
                            }

                            return version;
                        })));
        return new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                definition.getStorageTable(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                Optional.of(format("LAYOUT: %s, TABLE VERSIONS: %s", definition.getVersioningLayout(), tableVersions)),
                definition.getOwner(),
                definition.getProperties(),
                definition.getVersioningLayout(),
                definition.getSourceTableVersions());
    }

    @Override
    public Optional<MaterializedViewFreshness> getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name, boolean refresh)
    {
        return Optional.empty();
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public synchronized ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo info = requireNonNull(tables.get(memoryTableHandle.getId()), "tableInfo is null");
        long tableId;
        Optional<Long> oldTableId;
        if (info.getKeyColumnIndex().isPresent()) {
            tableId = memoryTableHandle.getId();
            oldTableId = Optional.empty();
        }
        else {
            // use new table id for full MV refresh
            tableId = nextTableId.getAndIncrement();
            oldTableId = Optional.of(memoryTableHandle.getId());
        }
        return new MemoryInsertTableHandle(
                tableId,
                ImmutableSet.<Long>builder()
                        .addAll(tableIds.values())
                        .add(tableId)
                        .build(),
                getNextVersion(memoryTableHandle.getId()),
                info.getKeyColumnIndex(),
                oldTableId);
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            Map<CatalogSchemaTableName, ConnectorTableVersion> sourceTableVersions)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryInsertTableHandle handle = (MemoryInsertTableHandle) insertHandle;
        commitVersion(handle.getTable(), handle.getVersion());

        // TODO: use reverse lookup
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        long materializedViewId = memoryTableHandle.getMaterializedViewId().orElseThrow();
        SchemaTableName viewName = tableIds.entrySet().stream()
                .filter(entry -> entry.getValue() == materializedViewId)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow();
        ConnectorMaterializedViewDefinition oldDefinition = materializedViews.get(viewName);
        ConnectorMaterializedViewDefinition newDefinition = new ConnectorMaterializedViewDefinition(
                oldDefinition.getOriginalSql(),
                oldDefinition.getStorageTable(),
                oldDefinition.getCatalog(),
                oldDefinition.getSchema(),
                oldDefinition.getColumns(),
                oldDefinition.getComment(),
                oldDefinition.getOwner(),
                oldDefinition.getProperties(),
                oldDefinition.getVersioningLayout(),
                Optional.of(sourceTableVersions));
        materializedViews.put(viewName, newDefinition);

        // switch to new table data for full refresh MV
        if (handle.getOldTableId().isPresent()) {
            tableIds.put(memoryTableHandle.getTableName(), handle.getTable());
            tables.put(handle.getTable(), tables.remove(handle.getOldTableId().get()));
        }

        return Optional.empty();
    }

    @Override
    public synchronized Optional<ConnectorTableVersioningLayout> getTableVersioningLayout(ConnectorSession session, ConnectorTableHandle handle)
    {
        MemoryTableHandle tableHandle = (MemoryTableHandle) handle;
        TableInfo info = requireNonNull(tables.get(tableHandle.getId()), "tableInfo is null");
        return info.getKeyColumnIndex()
                .map(index -> new ConnectorTableVersioningLayout(tableHandle, ImmutableSet.of(info.getColumns().get(index).getHandle()), true));
    }

    @Override
    public synchronized Optional<ConnectorTableVersion> getCurrentTableVersion(ConnectorSession session, ConnectorTableHandle handle)
    {
        MemoryTableHandle tableHandle = (MemoryTableHandle) handle;
        return tableHandle.getVersions().map(versions -> new ConnectorTableVersion(
                TARGET_ID,
                VERSION_TYPE,
                new LongArrayBlock(versions.size(), Optional.empty(), Longs.toArray(versions))));
    }

    @Override
    public Optional<ConnectorTableHandle> getInsertedOrUpdatedRows(ConnectorSession session, ConnectorTableHandle handle, ConnectorTableVersion fromVersionExclusive)
    {
        return getDataFrom(handle, fromVersionExclusive, false);
    }

    @Override
    public synchronized Optional<ConnectorTableHandle> getDeletedRows(ConnectorSession session, ConnectorTableHandle handle, ConnectorTableVersion fromVersionExclusive)
    {
        return getDataFrom(handle, fromVersionExclusive, true);
    }

    private synchronized Optional<ConnectorTableHandle> getDataFrom(ConnectorTableHandle handle, ConnectorTableVersion fromVersionExclusive, boolean deletedRows)
    {
        MemoryTableHandle tableHandle = (MemoryTableHandle) handle;
        TableInfo info = requireNonNull(tables.get(tableHandle.getId()), "tableInfo is null");
        return info.getKeyColumnIndex()
                .map(index -> {
                    Set<Long> rangeVersions = new HashSet<>(tableHandle.getVersions().orElseThrow());
                    Set<Long> fromVersions = getVersions(fromVersionExclusive);
                    checkState(rangeVersions.containsAll(fromVersions));
                    rangeVersions.removeAll(fromVersions);
                    return new MemoryTableHandle(
                            tableHandle.getId(),
                            tableHandle.getTableName(),
                            Optional.of(rangeVersions),
                            Optional.empty(),
                            deletedRows,
                            tableHandle.getMaterializedViewId());
                });
    }

    private Set<Long> getVersions(ConnectorTableVersion version)
    {
        if (!version.getPointerType().equals(TARGET_ID)) {
            throw new TrinoException(NOT_SUPPORTED, "Only 'VERSION' type is supported");
        }
        if (!version.getVersionType().equals(VERSION_TYPE)) {
            throw new TrinoException(NOT_SUPPORTED, "Table version must be of ARRAY[BIGINT] type");
        }
        checkArgument(version.getVersion() instanceof LongArrayBlock);
        Block block = (Block) version.getVersion();
        ImmutableSet.Builder<Long> versions = ImmutableSet.builder();
        for (int position = 0; position < block.getPositionCount(); ++position) {
            checkArgument(!block.isNull(position));
            versions.add(block.getLong(position, 0));
        }
        return versions.build();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.of(new ConnectorTableLayout(
                new MemoryPartitioningHandle(getWorkerAddress()),
                ImmutableList.of()));
    }

    private HostAddress getWorkerAddress()
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");
        return nodes.iterator().next().getHostAndPort();
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.of(new ConnectorTableLayout(
                new MemoryPartitioningHandle(getTableHostAddress(((MemoryTableHandle) tableHandle).getId())),
                ImmutableList.of()));
    }

    public HostAddress getTableHostAddress(long tableId)
    {
        return tables.get(tableId).getHostAddress();
    }
}
