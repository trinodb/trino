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
import io.airlift.slice.Slice;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.SampleType.SYSTEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MemoryMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    private final Map<Long, TableInfo> tables = new HashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

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
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new TrinoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        boolean tablesExist = tables.values().stream()
                .anyMatch(table -> table.getSchemaName().equals(schemaName));

        if (tablesExist) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Long id = tableIds.get(schemaTableName);
        if (id == null) {
            return null;
        }

        return new MemoryTableHandle(id);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
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
        return tables.get(handle.getId())
                .getColumns().stream()
                .collect(toImmutableMap(ColumnInfo::getName, ColumnInfo::getHandle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return tables.get(handle.getId())
                .getColumn(columnHandle)
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
        tables.put(tableId, new TableInfo(tableId, newTableName.getSchemaName(), newTableName.getTableName(), oldInfo.getColumns(), oldInfo.getDataFragments()));

        tableIds.remove(oldInfo.getSchemaTableName());
        tableIds.put(newTableName, tableId);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized MemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout)
    {
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long tableId = nextTableId.getAndIncrement();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            columns.add(new ColumnInfo(new MemoryColumnHandle(i), column.getName(), column.getType()));
        }

        tableIds.put(tableMetadata.getTable(), tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columns.build(),
                new HashMap<>()));

        return new MemoryOutputTableHandle(tableId, ImmutableSet.copyOf(tableIds.values()));
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
        MemoryOutputTableHandle memoryOutputHandle = (MemoryOutputTableHandle) tableHandle;

        updateRowsOnHosts(memoryOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized MemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        return new MemoryInsertTableHandle(memoryTableHandle.getId(), ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryInsertTableHandle memoryInsertHandle = (MemoryInsertTableHandle) insertHandle;

        updateRowsOnHosts(memoryInsertHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        checkSchemaExists(viewName.getSchemaName());
        if (tableIds.containsKey(viewName)) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            views.put(viewName, definition);
        }
        else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public synchronized void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        checkSchemaExists(newViewName.getSchemaName());
        if (tableIds.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + newViewName);
        }

        if (views.containsKey(newViewName)) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + newViewName);
        }

        views.put(newViewName, views.remove(viewName));
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
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

    private void updateRowsOnHosts(long tableId, Collection<Slice> fragments)
    {
        TableInfo info = tables.get(tableId);
        checkState(
                info != null,
                "Uninitialized tableId [%s.%s]",
                info.getSchemaName(),
                info.getTableName());

        Map<HostAddress, MemoryDataFragment> dataFragments = new HashMap<>(info.getDataFragments());
        for (Slice fragment : fragments) {
            MemoryDataFragment memoryDataFragment = MemoryDataFragment.fromSlice(fragment);
            dataFragments.merge(memoryDataFragment.getHostAddress(), memoryDataFragment, MemoryDataFragment::merge);
        }

        tables.put(tableId, new TableInfo(tableId, info.getSchemaName(), info.getTableName(), info.getColumns(), dataFragments));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    public List<MemoryDataFragment> getDataFragments(long tableId)
    {
        return ImmutableList.copyOf(tables.get(tableId).getDataFragments().values());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        List<MemoryDataFragment> dataFragments = getDataFragments(((MemoryTableHandle) tableHandle).getId());
        long rows = dataFragments.stream()
                .mapToLong(MemoryDataFragment::getRows)
                .sum();
        return TableStatistics.builder()
                .setRowCount(Estimate.of(rows))
                .build();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new MemoryTableHandle(table.getId(), OptionalLong.of(limit), OptionalDouble.empty()),
                true,
                true));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        if ((table.getSampleRatio().isPresent() && table.getSampleRatio().getAsDouble() == sampleRatio) || sampleType != SYSTEM || table.getLimit().isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new SampleApplicationResult<>(
                new MemoryTableHandle(table.getId(), table.getLimit(), OptionalDouble.of(table.getSampleRatio().orElse(1) * sampleRatio)),
                true));
    }
}
