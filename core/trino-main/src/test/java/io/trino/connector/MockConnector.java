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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.connector.MockConnectorFactory.ApplyAggregation;
import io.trino.connector.MockConnectorFactory.ApplyFilter;
import io.trino.connector.MockConnectorFactory.ApplyJoin;
import io.trino.connector.MockConnectorFactory.ApplyProjection;
import io.trino.connector.MockConnectorFactory.ApplyTableFunction;
import io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import io.trino.connector.MockConnectorFactory.ApplyTopN;
import io.trino.connector.MockConnectorFactory.ListRoleGrants;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
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
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.MockConnector.MockConnectorSplit.MOCK_CONNECTOR_SPLIT;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MockConnector
        implements Connector
{
    private static final String DELETE_ROW_ID = "delete_row_id";
    private static final String UPDATE_ROW_ID = "update_row_id";
    private static final String MERGE_ROW_ID = "merge_row_id";

    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<String>> listTables;
    private final Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>>> streamTableColumns;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews;
    private final BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector;
    private final BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView;
    private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
    private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
    private final Function<SchemaTableName, TableStatistics> getTableStatistics;
    private final Function<SchemaTableName, List<String>> checkConstraints;
    private final MockConnectorFactory.ApplyProjection applyProjection;
    private final MockConnectorFactory.ApplyAggregation applyAggregation;
    private final MockConnectorFactory.ApplyJoin applyJoin;
    private final MockConnectorFactory.ApplyTopN applyTopN;
    private final MockConnectorFactory.ApplyFilter applyFilter;
    private final MockConnectorFactory.ApplyTableFunction applyTableFunction;
    private final MockConnectorFactory.ApplyTableScanRedirect applyTableScanRedirect;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> getNewTableLayout;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges;
    private final Supplier<Iterable<EventListener>> eventListeners;
    private final MockConnectorFactory.ListRoleGrants roleGrants;
    private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Function<SchemaTableName, List<List<?>>> data;
    private final Function<SchemaTableName, Metrics> metrics;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final Optional<FunctionProvider> functionProvider;
    private final boolean supportsReportingWrittenBytes;
    private final boolean allowMissingColumnsOnInsert;
    private final Supplier<List<PropertyMetadata<?>>> analyzeProperties;
    private final Supplier<List<PropertyMetadata<?>>> schemaProperties;
    private final Supplier<List<PropertyMetadata<?>>> tableProperties;
    private final Supplier<List<PropertyMetadata<?>>> columnProperties;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Map<SchemaFunctionName, Function<ConnectorTableFunctionHandle, ConnectorSplitSource>> tableFunctionSplitsSources;
    private final OptionalInt maxWriterTasks;
    private final BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute;

    MockConnector(
            List<PropertyMetadata<?>> sessionProperties,
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<String>> listTables,
            Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>>> streamTableColumns,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews,
            BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector,
            BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            Function<SchemaTableName, TableStatistics> getTableStatistics,
            Function<SchemaTableName, List<String>> checkConstraints,
            ApplyProjection applyProjection,
            ApplyAggregation applyAggregation,
            ApplyJoin applyJoin,
            ApplyTopN applyTopN,
            ApplyFilter applyFilter,
            ApplyTableFunction applyTableFunction,
            ApplyTableScanRedirect applyTableScanRedirect,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable,
            BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> getInsertLayout,
            BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> getNewTableLayout,
            BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties,
            BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges,
            Supplier<Iterable<EventListener>> eventListeners,
            ListRoleGrants roleGrants,
            Optional<ConnectorNodePartitioningProvider> partitioningProvider,
            Optional<ConnectorAccessControl> accessControl,
            Function<SchemaTableName, List<List<?>>> data,
            Function<SchemaTableName, Metrics> metrics,
            Set<Procedure> procedures,
            Set<TableProcedureMetadata> tableProcedures,
            Set<ConnectorTableFunction> tableFunctions,
            Optional<FunctionProvider> functionProvider,
            boolean allowMissingColumnsOnInsert,
            Supplier<List<PropertyMetadata<?>>> analyzeProperties,
            Supplier<List<PropertyMetadata<?>>> schemaProperties,
            Supplier<List<PropertyMetadata<?>>> tableProperties,
            Supplier<List<PropertyMetadata<?>>> columnProperties,
            boolean supportsReportingWrittenBytes,
            Map<SchemaFunctionName, Function<ConnectorTableFunctionHandle, ConnectorSplitSource>> tableFunctionSplitsSources,
            OptionalInt maxWriterTasks,
            BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute)
    {
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.streamTableColumns = requireNonNull(streamTableColumns, "streamTableColumns is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getMaterializedViewProperties = requireNonNull(getMaterializedViewProperties, "getMaterializedViewProperties is null");
        this.getMaterializedViews = requireNonNull(getMaterializedViews, "getMaterializedViews is null");
        this.delegateMaterializedViewRefreshToConnector = requireNonNull(delegateMaterializedViewRefreshToConnector, "delegateMaterializedViewRefreshToConnector is null");
        this.refreshMaterializedView = requireNonNull(refreshMaterializedView, "refreshMaterializedView is null");
        this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
        this.getColumns = requireNonNull(getColumns, "getColumns is null");
        this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
        this.checkConstraints = requireNonNull(checkConstraints, "checkConstraints is null");
        this.applyProjection = requireNonNull(applyProjection, "applyProjection is null");
        this.applyAggregation = requireNonNull(applyAggregation, "applyAggregation is null");
        this.applyJoin = requireNonNull(applyJoin, "applyJoin is null");
        this.applyTopN = requireNonNull(applyTopN, "applyTopN is null");
        this.applyFilter = requireNonNull(applyFilter, "applyFilter is null");
        this.applyTableFunction = requireNonNull(applyTableFunction, "applyTableFunction is null");
        this.applyTableScanRedirect = requireNonNull(applyTableScanRedirect, "applyTableScanRedirection is null");
        this.redirectTable = requireNonNull(redirectTable, "redirectTable is null");
        this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
        this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
        this.getTableProperties = requireNonNull(getTableProperties, "getTableProperties is null");
        this.listTablePrivileges = requireNonNull(listTablePrivileges, "listTablePrivileges is null");
        this.eventListeners = requireNonNull(eventListeners, "eventListeners is null");
        this.roleGrants = requireNonNull(roleGrants, "roleGrants is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.data = requireNonNull(data, "data is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.procedures = requireNonNull(procedures, "procedures is null");
        this.tableProcedures = requireNonNull(tableProcedures, "tableProcedures is null");
        this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.supportsReportingWrittenBytes = supportsReportingWrittenBytes;
        this.allowMissingColumnsOnInsert = allowMissingColumnsOnInsert;
        this.analyzeProperties = requireNonNull(analyzeProperties, "analyzeProperties is null");
        this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.columnProperties = requireNonNull(columnProperties, "columnProperties is null");
        this.tableFunctionSplitsSources = ImmutableMap.copyOf(tableFunctionSplitsSources);
        this.maxWriterTasks = requireNonNull(maxWriterTasks, "maxWriterTasks is null");
        this.getLayoutForTableExecute = requireNonNull(getLayoutForTableExecute, "getLayoutForTableExecute is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return MockConnectorTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
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
            public ConnectorSplitSource getSplits(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    DynamicFilter dynamicFilter,
                    Constraint constraint)
            {
                return new FixedSplitSource(MOCK_CONNECTOR_SPLIT);
            }

            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaFunctionName name, ConnectorTableFunctionHandle functionHandle)
            {
                Function<ConnectorTableFunctionHandle, ConnectorSplitSource> splitSourceProvider = tableFunctionSplitsSources.get(name);
                requireNonNull(splitSourceProvider, "missing ConnectorSplitSource for table function " + name);
                return splitSourceProvider.apply(functionHandle);
            }
        };
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return eventListeners.get();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return functionProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties.get();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties.get();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.get();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return getMaterializedViewProperties.get();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties.get();
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
        public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
        {
            return applyTableFunction.apply(session, handle);
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
        public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal) {}

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
            return new ConnectorTableMetadata(
                    table.getTableName(),
                    getColumns.apply(table.getTableName()),
                    ImmutableMap.of(),
                    Optional.empty(),
                    checkConstraints.apply(table.getTableName()));
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
            return getTableStatistics.apply(table.getTableName());
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
        {
            if (schemaName.isPresent()) {
                String schema = schemaName.get();
                return listTables.apply(session, schema).stream()
                        .map(tableName -> new SchemaTableName(schema, tableName))
                        .collect(toImmutableList());
            }
            ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
            for (String schema : listSchemaNames(session)) {
                tableNames.addAll(listTables.apply(session, schema).stream()
                        .map(tableName -> new SchemaTableName(schema, tableName))
                        .collect(toImmutableList()));
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
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
        }

        @Override
        public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            if (streamTableColumns.isPresent()) {
                return streamTableColumns.get().apply(session, prefix);
            }

            return listTables(session, prefix.getSchema()).stream()
                    .filter(prefix::matches)
                    .map(name -> TableColumnsMetadata.forTable(name, getColumns.apply(name)))
                    .iterator();
        }

        @Override
        public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {}

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {}

        @Override
        public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {}

        @Override
        public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties) {}

        @Override
        public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment) {}

        @Override
        public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment) {}

        @Override
        public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment) {}

        @Override
        public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment) {}

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {}

        @Override
        public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type) {}

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
            return new MaterializedViewFreshness(view.getStorageTable().isPresent() ? FRESH : STALE);
        }

        @Override
        public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target) {}

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
        public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
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
        public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties) {}

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
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
        {
            return new MockConnectorInsertTableHandle(((MockConnectorTableHandle) tableHandle).getTableName());
        }

        @Override
        public boolean supportsMissingColumnsOnInsert()
        {
            return allowMissingColumnsOnInsert;
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
            return getInsertLayout.apply(session, table.getTableName());
        }

        @Override
        public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
        {
            return getLayoutForTableExecute.apply(session, tableExecuteHandle);
        }

        @Override
        public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
        {
            return new MockConnectorOutputTableHandle(tableMetadata.getTable());
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            return getNewTableLayout.apply(session, tableMetadata);
        }

        @Override
        public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return DELETE_ROW_AND_INSERT_ROW;
        }

        @Override
        public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return new MockConnectorColumnHandle(MERGE_ROW_ID, BIGINT);
        }

        @Override
        public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
        {
            return new MockConnectorMergeTableHandle((MockConnectorTableHandle) tableHandle);
        }

        @Override
        public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {}

        @Override
        public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
        {
            return getTableProperties.apply(session, table);
        }

        @Override
        public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
        {
            MockConnectorTableHandle connectorTableHandle = (MockConnectorTableHandle) tableHandle;
            return Optional.of(new MockConnectorTableExecuteHandle(0, connectorTableHandle.getTableName()));
        }

        @Override
        public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle) {}

        @Override
        public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState) {}

        @Override
        public Set<String> listRoles(ConnectorSession session)
        {
            return roleGrants.apply(session, Optional.empty(), Optional.empty(), OptionalLong.empty()).stream().map(RoleGrant::getRoleName).collect(toImmutableSet());
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
        public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
        {
            return listTablePrivileges.apply(session, prefix);
        }

        @Override
        public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            getMockAccessControl().grantSchemaPrivileges(schemaName, privileges, grantee, grantOption);
        }

        @Override
        public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
        {
            getMockAccessControl().revokeSchemaPrivileges(schemaName, privileges, revokee, grantOption);
        }

        @Override
        public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
        {
            getMockAccessControl().grantTablePrivileges(tableName, privileges, grantee, grantOption);
        }

        @Override
        public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
        {
            getMockAccessControl().revokeTablePrivileges(tableName, privileges, revokee, grantOption);
        }

        @Override
        public boolean supportsReportingWrittenBytes(ConnectorSession session, SchemaTableName schemaTableName, Map<String, Object> tableProperties)
        {
            return supportsReportingWrittenBytes;
        }

        @Override
        public boolean supportsReportingWrittenBytes(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return supportsReportingWrittenBytes;
        }

        @Override
        public OptionalInt getMaxWriterTasks(ConnectorSession session)
        {
            return maxWriterTasks;
        }

        @Override
        public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
        {
            return new BeginTableExecuteResult<>(tableExecuteHandle, updatedSourceTableHandle);
        }

        private MockConnectorAccessControl getMockAccessControl()
        {
            return (MockConnectorAccessControl) getAccessControl();
        }
    }

    private static class MockPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
        {
            return new MockPageSink();
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
        {
            return new MockPageSink();
        }

        @Override
        public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
        {
            return new MockPageSink();
        }
    }

    private static class MockPageSink
            implements ConnectorPageSink, ConnectorMergeSink
    {
        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            return NOT_BLOCKED;
        }

        @Override
        public void storeMergedRows(Page page) {}

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
                            if (columnName.equals(DELETE_ROW_ID) || columnName.equals(UPDATE_ROW_ID) || columnName.equals(MERGE_ROW_ID)) {
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
            return new MockConnectorPageSource(new RecordPageSource(new InMemoryRecordSet(types, records)), metrics.apply(tableName));
        }

        private Map<String, Integer> getColumnIndexes(SchemaTableName tableName)
        {
            ImmutableMap.Builder<String, Integer> columnIndexes = ImmutableMap.builder();
            List<ColumnMetadata> columnMetadata = getColumns.apply(tableName);
            for (int index = 0; index < columnMetadata.size(); index++) {
                columnIndexes.put(columnMetadata.get(index).getName(), index);
            }
            return columnIndexes.buildOrThrow();
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

        @Override
        public long getRetainedSizeInBytes()
        {
            return 0;
        }
    }

    public static class MockConnectorTableExecuteHandle
            implements ConnectorTableExecuteHandle
    {
        private final int someFieldForSerializer;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public MockConnectorTableExecuteHandle(int someFieldForSerializer, SchemaTableName schemaTableName)
        {
            this.someFieldForSerializer = someFieldForSerializer;
            this.schemaTableName = schemaTableName;
        }

        @JsonProperty
        public int getSomeFieldForSerializer()
        {
            return someFieldForSerializer;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }
    }
}
