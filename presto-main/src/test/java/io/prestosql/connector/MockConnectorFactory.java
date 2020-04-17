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
package io.prestosql.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchHandleResolver;
import io.prestosql.plugin.tpch.TpchRecordSetProvider;
import io.prestosql.plugin.tpch.TpchSplitManager;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class MockConnectorFactory
        implements ConnectorFactory
{
    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
    private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
    private final ApplyProjection applyProjection;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout;
    private final Supplier<Iterable<EventListener>> eventListeners;

    private MockConnectorFactory(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            ApplyProjection applyProjection,
            BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout,
            BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout,
            Supplier<Iterable<EventListener>> eventListeners)
    {
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
        this.getColumns = getColumns;
        this.applyProjection = applyProjection;
        this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
        this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
        this.eventListeners = requireNonNull(eventListeners, "eventListeners is null");
    }

    @Override
    public String getName()
    {
        return "mock";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new TpchHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new MockConnector(context, listSchemaNames, listTables, getViews, getTableHandle, getColumns, applyProjection, getInsertLayout, getNewTableLayout, eventListeners);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @FunctionalInterface
    public interface ApplyProjection
    {
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments);
    }

    public static class MockConnector
            implements Connector
    {
        private final ConnectorContext context;
        private final Function<ConnectorSession, List<String>> listSchemaNames;
        private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
        private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
        private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
        private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
        private final ApplyProjection applyProjection;
        private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout;
        private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout;
        private final Supplier<Iterable<EventListener>> eventListeners;

        private MockConnector(
                ConnectorContext context,
                Function<ConnectorSession, List<String>> listSchemaNames,
                BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
                BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
                BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
                Function<SchemaTableName, List<ColumnMetadata>> getColumns,
                ApplyProjection applyProjection,
                BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout,
                BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout,
                Supplier<Iterable<EventListener>> eventListeners)
        {
            this.context = requireNonNull(context, "context is null");
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            this.listTables = requireNonNull(listTables, "listTables is null");
            this.getViews = requireNonNull(getViews, "getViews is null");
            this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
            this.getColumns = requireNonNull(getColumns, "getColumns is null");
            this.applyProjection = requireNonNull(applyProjection, "applyProjection is null");
            this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
            this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
            this.eventListeners = requireNonNull(eventListeners, "eventListeners is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return new ConnectorTransactionHandle() {};
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
        {
            return new MockConnectorMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TpchSplitManager(context.getNodeManager(), 1);
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider()
        {
            return new TpchRecordSetProvider();
        }

        @Override
        public Iterable<EventListener> getEventListeners()
        {
            return eventListeners.get();
        }

        private class MockConnectorMetadata
                implements ConnectorMetadata
        {
            @Override
            public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
            {
                return applyProjection.apply(session, handle, projections, assignments);
            }

            @Override
            public List<String> listSchemaNames(ConnectorSession session)
            {
                return listSchemaNames.apply(session);
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
            {
                return getTableHandle.apply(session, tableName);
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
                return new ConnectorTableMetadata(table.getTableName(), getColumns.apply(table.getTableName()));
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
            {
                if (schemaName.isPresent()) {
                    return listTables.apply(session, schemaName.get());
                }
                ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
                for (String schema : listSchemaNames(session)) {
                    tableNames.addAll(listTables.apply(session, schema));
                }
                return tableNames.build();
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
                return getColumns.apply(table.getTableName()).stream()
                        .collect(toImmutableMap(ColumnMetadata::getName, column -> new TpchColumnHandle(column.getName(), column.getType())));
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
            {
                TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
                return new ColumnMetadata(tpchColumnHandle.getColumnName(), tpchColumnHandle.getType());
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return listTables(session, prefix.getSchema()).stream()
                        .filter(prefix::matches)
                        .collect(toImmutableMap(table -> table, getColumns));
            }

            @Override
            public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
            {
                return getViews.apply(session, schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new));
            }

            @Override
            public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
            {
                return Optional.ofNullable(getViews.apply(session, viewName.toSchemaTablePrefix()).get(viewName));
            }

            @Override
            public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                return new MockConnectorInsertTableHandle();
            }

            @Override
            public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
                return getInsertLayout.apply(session, table.getTableName());
            }

            @Override
            public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
            {
                return new MockConnectorOutputTableHandle();
            }

            @Override
            public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
            {
                return getNewTableLayout.apply(session, tableMetadata);
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
        }
    }

    public static class MockConnectorTableHandle
            implements ConnectorTableHandle
    {
        private final SchemaTableName tableName;

        @JsonCreator
        public MockConnectorTableHandle(@JsonProperty SchemaTableName tableName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        @JsonProperty
        public SchemaTableName getTableName()
        {
            return tableName;
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
            MockConnectorTableHandle other = (MockConnectorTableHandle) o;
            return Objects.equals(tableName, other.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName);
        }
    }

    private static class MockConnectorInsertTableHandle
            implements ConnectorInsertTableHandle
    {
    }

    private static class MockConnectorOutputTableHandle
            implements ConnectorOutputTableHandle
    {
    }

    public static final class Builder
    {
        private Function<ConnectorSession, List<String>> listSchemaNames = defaultListSchemaNames();
        private BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = defaultListTables();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = defaultGetViews();
        private BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle = defaultGetTableHandle();
        private Function<SchemaTableName, List<ColumnMetadata>> getColumns = defaultGetColumns();
        private ApplyProjection applyProjection = (session, handle, projections, assignments) -> Optional.empty();
        private BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout = defaultGetInsertLayout();
        private BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout = defaultGetNewTableLayout();
        private Supplier<Iterable<EventListener>> eventListeners = ImmutableList::of;

        public Builder withListSchemaNames(Function<ConnectorSession, List<String>> listSchemaNames)
        {
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            return this;
        }

        public Builder withListTables(BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables)
        {
            this.listTables = requireNonNull(listTables, "listTables is null");
            return this;
        }

        public Builder withGetViews(BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews)
        {
            this.getViews = requireNonNull(getViews, "getViews is null");
            return this;
        }

        public Builder withGetTableHandle(BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle)
        {
            this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
            return this;
        }

        public Builder withGetColumns(Function<SchemaTableName, List<ColumnMetadata>> getColumns)
        {
            this.getColumns = requireNonNull(getColumns, "getColumns is null");
            return this;
        }

        public Builder withApplyProjection(ApplyProjection applyProjection)
        {
            this.applyProjection = applyProjection;
            return this;
        }

        public Builder withGetInsertLayout(BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout)
        {
            this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
            return this;
        }

        public Builder withGetNewTableLayout(BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout)
        {
            this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
            return this;
        }

        public Builder withEventListener(EventListener listener)
        {
            requireNonNull(listener, "listener is null");

            withEventListener(() -> listener);
            return this;
        }

        public Builder withEventListener(Supplier<EventListener> listenerFactory)
        {
            requireNonNull(listenerFactory, "listenerFactory is null");

            this.eventListeners = () -> ImmutableList.of(listenerFactory.get());
            return this;
        }

        public MockConnectorFactory build()
        {
            return new MockConnectorFactory(listSchemaNames, listTables, getViews, getTableHandle, getColumns, applyProjection, getInsertLayout, getNewTableLayout, eventListeners);
        }

        public static Function<ConnectorSession, List<String>> defaultListSchemaNames()
        {
            return (session) -> ImmutableList.of();
        }

        public static BiFunction<ConnectorSession, String, List<SchemaTableName>> defaultListTables()
        {
            return (session, schemaName) -> ImmutableList.of();
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> defaultGetViews()
        {
            return (session, schemaTablePrefix) -> ImmutableMap.of();
        }

        public static BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> defaultGetTableHandle()
        {
            return (session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName);
        }

        public static BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> defaultGetInsertLayout()
        {
            return (session, tableHandle) -> Optional.empty();
        }

        public static BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> defaultGetNewTableLayout()
        {
            return (session, tableHandle) -> Optional.empty();
        }

        public static Function<SchemaTableName, List<ColumnMetadata>> defaultGetColumns()
        {
            return table -> IntStream.range(0, 100)
                    .boxed()
                    .map(i -> new ColumnMetadata("column_" + i, createUnboundedVarcharType()))
                    .collect(toImmutableList());
        }
    }
}
