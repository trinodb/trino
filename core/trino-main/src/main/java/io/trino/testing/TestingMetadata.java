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
package io.trino.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static java.util.Collections.synchronizedSet;
import static java.util.Objects.requireNonNull;

public class TestingMetadata
        implements ConnectorMetadata
{
    public static final Duration STALE_MV_STALENESS = Duration.ofHours(7);

    private final ConcurrentMap<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
    private final ConcurrentMap<SchemaTableName, ConnectorViewDefinition> views = new ConcurrentHashMap<>();
    private final ConcurrentMap<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new ConcurrentHashMap<>();
    private final Set<SchemaTableName> freshMaterializedViews = synchronizedSet(new HashSet<>());

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Set<String> schemaNames = new HashSet<>();

        for (SchemaTableName schemaTableName : tables.keySet()) {
            schemaNames.add(schemaTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return new TestingTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        ConnectorTableMetadata tableMetadata = tables.get(tableName);
        checkArgument(tableMetadata != null, "Table '%s' does not exist", tableName);
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TestingColumnHandle(columnMetadata.getName(), index, columnMetadata.getType()));
            index++;
        }
        return builder.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType()));
            }
            tableColumns.put(tableName, columns.build());
        }
        return tableColumns.buildOrThrow();
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        return new ConnectorAnalyzeMetadata(tableHandle, TableStatisticsMetadata.empty());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        int columnIndex = ((TestingColumnHandle) columnHandle).getOrdinalPosition();
        return tables.get(tableName).getColumns().get(columnIndex);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tables.keySet()) {
            if (schemaName.map(tableName.getSchemaName()::equals).orElse(true)) {
                builder.add(tableName);
            }
        }
        return builder.build();
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        // TODO: use locking to do this properly
        ConnectorTableMetadata table = getTableMetadata(session, tableHandle);
        if (tables.putIfAbsent(newTableName, table) != null) {
            throw new IllegalArgumentException("Target table already exists: " + newTableName);
        }
        tables.remove(table.getTable(), table);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorTableMetadata existingTable = tables.putIfAbsent(tableMetadata.getTable(), tableMetadata);
        if (existingTable != null && !ignoreExisting) {
            throw new IllegalArgumentException("Target table already exists: " + tableMetadata.getTable());
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        tables.remove(getTableName(tableHandle));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        if (replace) {
            views.put(viewName, definition);
        }
        else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName viewName : views.keySet()) {
            if (schemaName.map(viewName.getSchemaName()::equals).orElse(true)) {
                builder.add(viewName);
            }
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(Maps.filterKeys(views, prefix::matches));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(views.get(viewName));
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        if (replace) {
            materializedViews.put(viewName, definition);
        }
        else if (materializedViews.putIfAbsent(viewName, definition) != null) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
        }
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO: use locking to do this properly
        ConnectorMaterializedViewDefinition materializedView = getMaterializedView(session, source).orElseThrow();
        if (materializedViews.putIfAbsent(target, materializedView) != null) {
            throw new IllegalArgumentException("Target materialized view already exists: " + target);
        }
        materializedViews.remove(source, materializedView);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(materializedViews.get(viewName));
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        if (materializedViews.remove(viewName) == null) {
            throw new MaterializedViewNotFoundException(viewName);
        }
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        boolean fresh = freshMaterializedViews.contains(name);
        return new MaterializedViewFreshness(
                fresh ? FRESH : STALE,
                fresh ? Optional.empty() : Optional.of(Instant.now().minus(STALE_MV_STALENESS)));
    }

    public void markMaterializedViewIsFresh(SchemaTableName name)
    {
        freshMaterializedViews.add(name);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        return TestingHandle.INSTANCE;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        createTable(session, tableMetadata, false);
        return TestingHandle.INSTANCE;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return TestingHandle.INSTANCE;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        SchemaTableName tableName = getTableName(tableHandle);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.addAll(tableMetadata.getColumns());
        columns.add(column);
        tables.put(tableName, new ConnectorTableMetadata(tableName, columns.build(), tableMetadata.getProperties(), tableMetadata.getComment()));
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        SchemaTableName tableName = getTableName(tableHandle);
        List<ColumnMetadata> columns = new ArrayList<>(tableMetadata.getColumns());
        ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, column);
        columns.set(columns.indexOf(columnMetadata), ColumnMetadata.builderFrom(columnMetadata).setType(type).build());
        tables.put(tableName, new ConnectorTableMetadata(tableName, ImmutableList.copyOf(columns), tableMetadata.getProperties(), tableMetadata.getComment()));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        SchemaTableName tableName = getTableName(tableHandle);
        ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, source);
        List<ColumnMetadata> columns = new ArrayList<>(tableMetadata.getColumns());
        columns.set(columns.indexOf(columnMetadata), ColumnMetadata.builderFrom(columnMetadata).setName(target).build());
        tables.put(tableName, new ConnectorTableMetadata(tableName, ImmutableList.copyOf(columns), tableMetadata.getProperties(), tableMetadata.getComment()));
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption) {}

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption) {}

    public void clear()
    {
        views.clear();
        tables.clear();
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof TestingTableHandle, "tableHandle is not an instance of TestingTableHandle");
        TestingTableHandle testingTableHandle = (TestingTableHandle) tableHandle;
        return testingTableHandle.getTableName();
    }

    public static class TestingTableHandle
            implements ConnectorTableHandle
    {
        private final SchemaTableName tableName;

        public TestingTableHandle()
        {
            this(new SchemaTableName("test-schema", "test-table"));
        }

        @JsonCreator
        public TestingTableHandle(@JsonProperty("tableName") SchemaTableName schemaTableName)
        {
            this.tableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @JsonProperty
        public SchemaTableName getTableName()
        {
            return tableName;
        }

        @Override
        public String toString()
        {
            return tableName.toString();
        }
    }

    public static class TestingColumnHandle
            implements ColumnHandle
    {
        private final String name;
        private final OptionalInt ordinalPosition;
        private final Optional<Type> type;

        public TestingColumnHandle(String name)
        {
            this(name, OptionalInt.empty(), Optional.empty());
        }

        public TestingColumnHandle(String name, int ordinalPosition, Type type)
        {
            this(name, OptionalInt.of(ordinalPosition), Optional.of(type));
        }

        @JsonCreator
        public TestingColumnHandle(
                @JsonProperty("name") String name,
                @JsonProperty("ordinalPosition") OptionalInt ordinalPosition,
                @JsonProperty("type") Optional<Type> type)
        {
            this.name = requireNonNull(name, "name is null");
            this.ordinalPosition = requireNonNull(ordinalPosition, "ordinalPosition is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        public int getOrdinalPosition()
        {
            return ordinalPosition.orElseThrow(() -> new UnsupportedOperationException("Testing handle was created without ordinal position"));
        }

        public Type getType()
        {
            return type.orElseThrow(() -> new UnsupportedOperationException("Testing handle was created without type"));
        }

        @JsonProperty("ordinalPosition")
        public OptionalInt getJsonOrdinalPosition()
        {
            return ordinalPosition;
        }

        @JsonProperty("type")
        public Optional<Type> getJsonType()
        {
            return type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingColumnHandle that = (TestingColumnHandle) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(ordinalPosition, that.ordinalPosition) &&
                    Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, ordinalPosition, type);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .addValue(ordinalPosition)
                    .add("name", name)
                    .add("type", type)
                    .toString();
        }
    }
}
