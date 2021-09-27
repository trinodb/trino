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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.MockConnector.MockConnectorSplit.MOCK_CONNECTOR_SPLIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MockConnector
        implements Connector
{
    private static final String DELETE_ROW_ID = "delete_row_id";
    private static final String UPDATE_ROW_ID = "update_row_id";

    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
    private final Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>>> streamTableColumns;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews;
    private final BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector;
    private final BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView;
    private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
    private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
    private final MockConnectorFactory.ApplyProjection applyProjection;
    private final MockConnectorFactory.ApplyAggregation applyAggregation;
    private final MockConnectorFactory.ApplyJoin applyJoin;
    private final MockConnectorFactory.ApplyTopN applyTopN;
    private final MockConnectorFactory.ApplyFilter applyFilter;
    private final MockConnectorFactory.ApplyTableScanRedirect applyTableScanRedirect;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties;
    private final Supplier<Iterable<EventListener>> eventListeners;
    private final MockConnectorFactory.ListRoleGrants roleGrants;
    private final Optional<MockConnectorAccessControl> accessControl;
    private final Function<SchemaTableName, List<List<?>>> data;
    private final Set<Procedure> procedures;

    MockConnector(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>>> streamTableColumns,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews,
            BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector,
            BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            MockConnectorFactory.ApplyProjection applyProjection,
            MockConnectorFactory.ApplyAggregation applyAggregation,
            MockConnectorFactory.ApplyJoin applyJoin,
            MockConnectorFactory.ApplyTopN applyTopN,
            MockConnectorFactory.ApplyFilter applyFilter,
            MockConnectorFactory.ApplyTableScanRedirect applyTableScanRedirect,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable,
            BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout,
            BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout,
            BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties,
            Supplier<Iterable<EventListener>> eventListeners,
            MockConnectorFactory.ListRoleGrants roleGrants,
            Optional<MockConnectorAccessControl> accessControl,
            Function<SchemaTableName, List<List<?>>> data,
            Set<Procedure> procedures)
    {
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.streamTableColumns = requireNonNull(streamTableColumns, "streamTableColumns is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getMaterializedViews = requireNonNull(getMaterializedViews, "getMaterializedViews is null");
        this.delegateMaterializedViewRefreshToConnector = requireNonNull(delegateMaterializedViewRefreshToConnector, "delegateMaterializedViewRefreshToConnector is null");
        this.refreshMaterializedView = requireNonNull(refreshMaterializedView, "refreshMaterializedView is null");
        this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
        this.getColumns = requireNonNull(getColumns, "getColumns is null");
        this.applyProjection = requireNonNull(applyProjection, "applyProjection is null");
        this.applyAggregation = requireNonNull(applyAggregation, "applyAggregation is null");
        this.applyJoin = requireNonNull(applyJoin, "applyJoin is null");
        this.applyTopN = requireNonNull(applyTopN, "applyTopN is null");
        this.applyFilter = requireNonNull(applyFilter, "applyFilter is null");
        this.applyTableScanRedirect = requireNonNull(applyTableScanRedirect, "applyTableScanRedirection is null");
        this.redirectTable = requireNonNull(redirectTable, "redirectTable is null");
        this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
        this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
        this.getTableProperties = requireNonNull(getTableProperties, "getTableProperties is null");
        this.eventListeners = requireNonNull(eventListeners, "eventListeners is null");
        this.roleGrants = requireNonNull(roleGrants, "roleGrants is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.data = requireNonNull(data, "data is null");
        this.procedures = requireNonNull(procedures, "procedures is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return MockConnectorTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        return new MockConnectorMetadata();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new MockConnectorPageSourceProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return new MockPageSinkProvider();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ConnectorSplitManager()
        {
            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, DynamicFilter dynamicFilter)
            {
                return new FixedSplitSource(ImmutableList.of(MOCK_CONNECTOR_SPLIT));
            }
        };
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return eventListeners.get();
    }

    @Override
    public MockConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(() -> new UnsupportedOperationException("Access control for mock connector is not set"));
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    private class MockConnectorMetadata
            implements ConnectorMetadata
    {
        @Override
        public boolean schemaExists(ConnectorSession session, String schemaName)
        {
            return listSchemaNames.apply(session).contains(schemaName);
        }

        @Override
        public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<ConnectorExpression> projections,
                Map<String, ColumnHandle> assignments)
        {
            return applyProjection.apply(session, handle, projections, assignments);
        }

        @Override
        public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<AggregateFunction> aggregates,
                Map<String, ColumnHandle> assignments,
                List<List<ColumnHandle>> groupingSets)
        {
            return applyAggregation.apply(session, handle, aggregates, assignments, groupingSets);
        }

        @Override
        public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
                ConnectorSession session,
                JoinType joinType,
                ConnectorTableHandle left,
                ConnectorTableHandle right,
                List<JoinCondition> joinConditions,
                Map<String, ColumnHandle> leftAssignments,
                Map<String, ColumnHandle> rightAssignments,
                JoinStatistics statistics)
        {
            return applyJoin.apply(session, joinType, left, right, joinConditions, leftAssignments, rightAssignments);
        }

        @Override
        public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
                ConnectorSession session,
                ConnectorTableHandle handle,
                long topNCount,
                List<SortItem> sortItems,
                Map<String, ColumnHandle> assignments)
        {
            return applyTopN.apply(session, handle, topNCount, sortItems, assignments);
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
        {
            return applyFilter.apply(session, handle, constraint);
        }

        @Override
        public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return applyTableScanRedirect.apply(session, tableHandle);
        }

        @Override
        public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName schemaTableName)
        {
            return redirectTable.apply(session, schemaTableName);
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return listSchemaNames.apply(session);
        }

        @Override
        public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner) {}

        @Override
        public void renameSchema(ConnectorSession session, String source, String target) {}

        @Override
        public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal) {}

        @Override
        public void dropSchema(ConnectorSession session, String schemaName) {}

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
                    .collect(toImmutableMap(ColumnMetadata::getName, column -> new MockConnectorColumnHandle(column.getName(), column.getType())));
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            MockConnectorColumnHandle mockColumnHandle = (MockConnectorColumnHandle) columnHandle;
            return new ColumnMetadata(mockColumnHandle.getName(), mockColumnHandle.getType());
        }

        @Override
        public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            if (streamTableColumns.isPresent()) {
                return streamTableColumns.get().apply(session, prefix);
            }

            return listTables(session, prefix.getSchema()).stream()
                    .filter(prefix::matches)
                    .map(name -> TableColumnsMetadata.forTable(name, getColumns.apply(name)));
        }

        @Override
        public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {}

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {}

        @Override
        public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {}

        @Override
        public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment) {}

        @Override
        public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment) {}

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {}

        @Override
        public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal) {}

        @Override
        public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target) {}

        @Override
        public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {}

        @Override
        public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace) {}

        @Override
        public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target) {}

        @Override
        public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal) {}

        @Override
        public void dropView(ConnectorSession session, SchemaTableName viewName) {}

        @Override
        public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting) {}

        @Override
        public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
        {
            return Optional.ofNullable(getMaterializedViews.apply(session, viewName.toSchemaTablePrefix()).get(viewName));
        }

        @Override
        public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName viewName)
        {
            ConnectorMaterializedViewDefinition view = getMaterializedViews.apply(session, viewName.toSchemaTablePrefix()).get(viewName);
            checkArgument(view != null, "Materialized view %s does not exist", viewName);
            return new MaterializedViewFreshness(view.getStorageTable().isPresent());
        }

        @Override
        public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
        {
            return delegateMaterializedViewRefreshToConnector.apply(session, viewName);
        }

        @Override
        public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
        {
            return refreshMaterializedView.apply(session, viewName);
        }

        @Override
        public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles)
        {
            return new MockConnectorInsertTableHandle(((MockConnectorTableHandle) tableHandle).getTableName());
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                ConnectorInsertTableHandle insertHandle,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics,
                List<ConnectorTableHandle> sourceTableHandles)
        {
            return Optional.empty();
        }

        @Override
        public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName) {}

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
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
        {
            return new MockConnectorInsertTableHandle(((MockConnectorTableHandle) tableHandle).getTableName());
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return Optional.empty();
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
            return new MockConnectorOutputTableHandle(tableMetadata.getTable());
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            return getNewTableLayout.apply(session, tableMetadata);
        }

        @Override
        public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
        {
            return tableHandle;
        }

        @Override
        public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {}

        @Override
        public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
        {
            return new MockConnectorColumnHandle(UPDATE_ROW_ID, BIGINT);
        }

        @Override
        public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return tableHandle;
        }

        @Override
        public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return new MockConnectorColumnHandle(DELETE_ROW_ID, BIGINT);
        }

        @Override
        public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {}

        @Override
        public boolean usesLegacyTableLayouts()
        {
            return false;
        }

        @Override
        public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
        {
            return getTableProperties.apply(session, table);
        }

        @Override
        public Set<String> listRoles(ConnectorSession session)
        {
            return roleGrants.apply(session, Optional.empty(), Optional.empty(), OptionalLong.empty()).stream().map(grant -> grant.getRoleName()).collect(toImmutableSet());
        }

        @Override
        public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
        {
            return roleGrants.apply(session, Optional.empty(), Optional.empty(), OptionalLong.empty())
                    .stream()
                    .filter(grant -> grant.getGrantee().equals(principal))
                    .collect(toImmutableSet());
        }

        @Override
        public Set<RoleGrant> listAllRoleGrants(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
        {
            return roleGrants.apply(session, roles, grantees, limit);
        }

        @Override
        public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
        {
            return listRoleGrants(session, principal);
        }

        @Override
        public Set<String> listEnabledRoles(ConnectorSession session)
        {
            return listRoles(session);
        }

        @Override
        public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            getAccessControl().grantSchemaPrivileges(schemaName, privileges, grantee, grantOption);
        }

        @Override
        public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
        {
            getAccessControl().revokeSchemaPrivileges(schemaName, privileges, revokee, grantOption);
        }

        @Override
        public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            getAccessControl().grantTablePrivileges(tableName, privileges, grantee, grantOption);
        }

        @Override
        public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
        {
            getAccessControl().revokeTablePrivileges(tableName, privileges, revokee, grantOption);
        }
    }

    private static class MockPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            return new MockPageSink();
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            return new MockPageSink();
        }
    }

    private static class MockPageSink
            implements ConnectorPageSink
    {
        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort() {}
    }

    private class MockConnectorPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
        {
            MockConnectorTableHandle handle = (MockConnectorTableHandle) table;
            SchemaTableName tableName = handle.getTableName();
            List<MockConnectorColumnHandle> projection = columns.stream()
                    .map(MockConnectorColumnHandle.class::cast)
                    .collect(toImmutableList());
            List<Type> types = columns.stream()
                    .map(MockConnectorColumnHandle.class::cast)
                    .map(MockConnectorColumnHandle::getType)
                    .collect(toImmutableList());
            Map<String, Integer> columnIndexes = getColumnIndexes(tableName);
            List<List<?>> records = data.apply(tableName).stream()
                    .map(record -> {
                        ImmutableList.Builder<Object> projectedRow = ImmutableList.builder();
                        for (MockConnectorColumnHandle column : projection) {
                            String columnName = column.getName();
                            if (columnName.equals(DELETE_ROW_ID) || columnName.equals(UPDATE_ROW_ID)) {
                                projectedRow.add(0);
                                continue;
                            }
                            Integer index = columnIndexes.get(columnName);
                            requireNonNull(index, "index is null");
                            projectedRow.add(record.get(index));
                        }
                        return projectedRow.build();
                    })
                    .collect(toImmutableList());
            return new MockConnectorPageSource(new RecordPageSource(new InMemoryRecordSet(types, records)));
        }

        private Map<String, Integer> getColumnIndexes(SchemaTableName tableName)
        {
            ImmutableMap.Builder<String, Integer> columnIndexes = ImmutableMap.builder();
            List<ColumnMetadata> columnMetadata = getColumns.apply(tableName);
            for (int index = 0; index < columnMetadata.size(); index++) {
                columnIndexes.put(columnMetadata.get(index).getName(), index);
            }
            return columnIndexes.build();
        }
    }

    public enum MockConnectorSplit
            implements ConnectorSplit
    {
        MOCK_CONNECTOR_SPLIT;

        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return "mock connector split";
        }
    }
}
