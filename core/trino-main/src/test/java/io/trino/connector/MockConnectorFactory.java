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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

import java.util.ArrayList;
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
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.metrics.Metrics.EMPTY;
import static io.trino.spi.statistics.TableStatistics.empty;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class MockConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final List<PropertyMetadata<?>> sessionProperty;
    private final Function<ConnectorMetadata, ConnectorMetadata> metadataWrapper;
    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<String>> listTables;
    private final Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>>> streamTableColumns;
    private final Optional<MockConnectorFactory.StreamRelationColumns> streamRelationColumns;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final Supplier<List<PropertyMetadata<?>>> getViewProperties;
    private final Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews;
    private final BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector;
    private final BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView;
    private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
    private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
    private final Function<SchemaTableName, Optional<String>> getComment;
    private final Function<SchemaTableName, TableStatistics> getTableStatistics;
    private final Function<SchemaTableName, List<String>> checkConstraints;
    private final ApplyProjection applyProjection;
    private final ApplyAggregation applyAggregation;
    private final ApplyJoin applyJoin;
    private final ApplyTopN applyTopN;
    private final ApplyFilter applyFilter;
    private final ApplyTableFunction applyTableFunction;
    private final ApplyTableScanRedirect applyTableScanRedirect;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> getNewTableLayout;
    private final BiFunction<ConnectorSession, Type, Optional<Type>> getSupportedType;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges;
    private final Supplier<Iterable<EventListener>> eventListeners;
    private final Collection<FunctionMetadata> functions;
    private final Function<SchemaTableName, List<List<?>>> data;
    private final Function<SchemaTableName, Metrics> metrics;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final Optional<FunctionProvider> functionProvider;
    private final boolean allowMissingColumnsOnInsert;
    private final Supplier<List<PropertyMetadata<?>>> analyzeProperties;
    private final Supplier<List<PropertyMetadata<?>>> schemaProperties;
    private final Supplier<List<PropertyMetadata<?>>> tableProperties;
    private final Supplier<List<PropertyMetadata<?>>> columnProperties;
    private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
    private final Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources;

    // access control
    private final ListRoleGrants roleGrants;
    private final Optional<ConnectorAccessControl> accessControl;
    private final OptionalInt maxWriterTasks;
    private final Function<ConnectorTableHandle, Optional<CacheTableId>> getCacheTableId;
    private final Function<ColumnHandle, Optional<CacheColumnId>> getCacheColumnId;
    private final Function<ConnectorTableHandle, ConnectorTableHandle> getCanonicalTableHandle;
    private final BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute;

    private final WriterScalingOptions writerScalingOptions;
    private final Supplier<Set<ConnectorCapabilities>> capabilities;
    private final boolean allowSplittingReadIntoMultipleSubQueries;

    private MockConnectorFactory(
            String name,
            List<PropertyMetadata<?>> sessionProperty,
            Function<ConnectorMetadata, ConnectorMetadata> metadataWrapper,
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<String>> listTables,
            Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>>> streamTableColumns,
            Optional<StreamRelationColumns> streamRelationColumns,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            Supplier<List<PropertyMetadata<?>>> getViewProperties,
            Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews,
            BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector,
            BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            Function<SchemaTableName, Optional<String>> getComment,
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
            BiFunction<ConnectorSession, Type, Optional<Type>> getSupportedType,
            BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties,
            BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges,
            Supplier<Iterable<EventListener>> eventListeners,
            Collection<FunctionMetadata> functions,
            Function<SchemaTableName, List<List<?>>> data,
            Function<SchemaTableName, Metrics> metrics,
            Set<Procedure> procedures,
            Set<TableProcedureMetadata> tableProcedures,
            Set<ConnectorTableFunction> tableFunctions,
            Optional<FunctionProvider> functionProvider,
            Supplier<List<PropertyMetadata<?>>> analyzeProperties,
            Supplier<List<PropertyMetadata<?>>> schemaProperties,
            Supplier<List<PropertyMetadata<?>>> tableProperties,
            Supplier<List<PropertyMetadata<?>>> columnProperties,
            Optional<ConnectorNodePartitioningProvider> partitioningProvider,
            ListRoleGrants roleGrants,
            Optional<ConnectorAccessControl> accessControl,
            boolean allowMissingColumnsOnInsert,
            Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources,
            OptionalInt maxWriterTasks,
            Function<ConnectorTableHandle, Optional<CacheTableId>> getCacheTableId,
            Function<ColumnHandle, Optional<CacheColumnId>> getCacheColumnId,
            Function<ConnectorTableHandle, ConnectorTableHandle> getCanonicalTableHandle,
            BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute,
            WriterScalingOptions writerScalingOptions,
            Supplier<Set<ConnectorCapabilities>> capabilities,
            boolean allowSplittingReadIntoMultipleSubQueries)
    {
        this.name = requireNonNull(name, "name is null");
        this.sessionProperty = ImmutableList.copyOf(requireNonNull(sessionProperty, "sessionProperty is null"));
        this.metadataWrapper = requireNonNull(metadataWrapper, "metadataWrapper is null");
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.streamTableColumns = requireNonNull(streamTableColumns, "streamTableColumns is null");
        this.streamRelationColumns = requireNonNull(streamRelationColumns, "streamRelationColumns is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getViewProperties = requireNonNull(getViewProperties, "getViewProperties is null");
        this.getMaterializedViewProperties = requireNonNull(getMaterializedViewProperties, "getMaterializedViewProperties is null");
        this.getMaterializedViews = requireNonNull(getMaterializedViews, "getMaterializedViews is null");
        this.delegateMaterializedViewRefreshToConnector = requireNonNull(delegateMaterializedViewRefreshToConnector, "delegateMaterializedViewRefreshToConnector is null");
        this.refreshMaterializedView = requireNonNull(refreshMaterializedView, "refreshMaterializedView is null");
        this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
        this.getColumns = requireNonNull(getColumns, "getColumns is null");
        this.getComment = requireNonNull(getComment, "getComment is null");
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
        this.getSupportedType = requireNonNull(getSupportedType, "getSupportedType is null");
        this.getTableProperties = requireNonNull(getTableProperties, "getTableProperties is null");
        this.listTablePrivileges = requireNonNull(listTablePrivileges, "listTablePrivileges is null");
        this.eventListeners = requireNonNull(eventListeners, "eventListeners is null");
        this.functions = ImmutableList.copyOf(functions);
        this.analyzeProperties = requireNonNull(analyzeProperties, "analyzeProperties is null");
        this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.columnProperties = requireNonNull(columnProperties, "columnProperties is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
        this.roleGrants = requireNonNull(roleGrants, "roleGrants is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.data = requireNonNull(data, "data is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.procedures = requireNonNull(procedures, "procedures is null");
        this.tableProcedures = requireNonNull(tableProcedures, "tableProcedures is null");
        this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.allowMissingColumnsOnInsert = allowMissingColumnsOnInsert;
        this.tableFunctionSplitsSources = requireNonNull(tableFunctionSplitsSources, "tableFunctionSplitsSources is null");
        this.maxWriterTasks = maxWriterTasks;
        this.getCacheTableId = requireNonNull(getCacheTableId, "getCacheTableId is null");
        this.getCacheColumnId = requireNonNull(getCacheColumnId, "getCacheColumnId is null");
        this.getCanonicalTableHandle = requireNonNull(getCanonicalTableHandle, "getCacheColumnId is null");
        this.getLayoutForTableExecute = requireNonNull(getLayoutForTableExecute, "getLayoutForTableExecute is null");
        this.writerScalingOptions = requireNonNull(writerScalingOptions, "writerScalingOptions is null");
        this.capabilities = requireNonNull(capabilities, "capabilities is null");
        this.allowSplittingReadIntoMultipleSubQueries = allowSplittingReadIntoMultipleSubQueries;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new MockConnector(
                metadataWrapper,
                sessionProperty,
                listSchemaNames,
                listTables,
                streamTableColumns,
                streamRelationColumns,
                getViews,
                getViewProperties,
                getMaterializedViewProperties,
                getMaterializedViews,
                delegateMaterializedViewRefreshToConnector,
                refreshMaterializedView,
                getTableHandle,
                getColumns,
                getComment,
                getTableStatistics,
                checkConstraints,
                applyProjection,
                applyAggregation,
                applyJoin,
                applyTopN,
                applyFilter,
                applyTableFunction,
                applyTableScanRedirect,
                redirectTable,
                getInsertLayout,
                getNewTableLayout,
                getSupportedType,
                getTableProperties,
                listTablePrivileges,
                eventListeners,
                functions,
                roleGrants,
                partitioningProvider,
                accessControl,
                data,
                metrics,
                procedures,
                tableProcedures,
                tableFunctions,
                functionProvider,
                allowMissingColumnsOnInsert,
                analyzeProperties,
                schemaProperties,
                tableProperties,
                columnProperties,
                tableFunctionSplitsSources,
                maxWriterTasks,
                getCacheTableId,
                getCacheColumnId,
                getCanonicalTableHandle,
                getLayoutForTableExecute,
                writerScalingOptions,
                capabilities,
                allowSplittingReadIntoMultipleSubQueries);
    }

    public static MockConnectorFactory create()
    {
        return builder().build();
    }

    public static MockConnectorFactory create(String name)
    {
        return builder().withName(name).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @FunctionalInterface
    public interface StreamRelationColumns
    {
        Iterator<RelationColumnsMetadata> apply(
                ConnectorSession session,
                Optional<String> schemaName,
                UnaryOperator<Set<SchemaTableName>> tablesFilter);
    }

    @FunctionalInterface
    public interface ApplyProjection
    {
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> apply(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<ConnectorExpression> projections,
                Map<String, ColumnHandle> assignments);
    }

    @FunctionalInterface
    public interface ApplyAggregation
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> apply(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<AggregateFunction> aggregates,
                Map<String, ColumnHandle> assignments,
                List<List<ColumnHandle>> groupingSets);
    }

    @FunctionalInterface
    public interface ApplyJoin
    {
        Optional<JoinApplicationResult<ConnectorTableHandle>> apply(
                ConnectorSession session,
                JoinType joinType,
                ConnectorTableHandle left,
                ConnectorTableHandle right,
                List<JoinCondition> joinConditions,
                Map<String, ColumnHandle> leftAssignments,
                Map<String, ColumnHandle> rightAssignments);
    }

    @FunctionalInterface
    public interface ApplyTopN
    {
        Optional<TopNApplicationResult<ConnectorTableHandle>> apply(
                ConnectorSession session,
                ConnectorTableHandle handle,
                long topNCount,
                List<SortItem> sortItems,
                Map<String, ColumnHandle> assignments);
    }

    @FunctionalInterface
    public interface ApplyFilter
    {
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint);
    }

    @FunctionalInterface
    public interface ApplyTableScanRedirect
    {
        Optional<TableScanRedirectApplicationResult> apply(ConnectorSession session, ConnectorTableHandle handle);
    }

    @FunctionalInterface
    public interface ApplyTableFunction
    {
        Optional<TableFunctionApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, ConnectorTableFunctionHandle handle);
    }

    @FunctionalInterface
    public interface ListRoleGrants
    {
        Set<RoleGrant> apply(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit);
    }

    public static final class Builder
    {
        private String name = "mock";
        private final List<PropertyMetadata<?>> sessionProperties = new ArrayList<>();
        private Function<ConnectorMetadata, ConnectorMetadata> metadataWrapper = identity();
        private Function<ConnectorSession, List<String>> listSchemaNames = defaultListSchemaNames();
        private BiFunction<ConnectorSession, String, List<String>> listTables = defaultListTables();
        private Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>>> streamTableColumns = Optional.empty();
        private Optional<MockConnectorFactory.StreamRelationColumns> streamRelationColumns = Optional.empty();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = defaultGetViews();
        private Supplier<List<PropertyMetadata<?>>> getViewProperties = defaultGetViewProperties();
        private Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties = defaultGetMaterializedViewProperties();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews = defaultGetMaterializedViews();
        private BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector = (session, viewName) -> false;
        private BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView = (session, viewName) -> CompletableFuture.completedFuture(null);
        private BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle = defaultGetTableHandle();
        private Function<SchemaTableName, List<ColumnMetadata>> getColumns = defaultGetColumns();
        private Function<SchemaTableName, Optional<String>> getComment = schemaTableName -> Optional.empty();
        private Function<SchemaTableName, TableStatistics> getTableStatistics = schemaTableName -> empty();
        private Function<SchemaTableName, List<String>> checkConstraints = schemaTableName -> ImmutableList.of();
        private ApplyProjection applyProjection = (session, handle, projections, assignments) -> Optional.empty();
        private ApplyAggregation applyAggregation = (session, handle, aggregates, assignments, groupingSets) -> Optional.empty();
        private ApplyJoin applyJoin = (session, joinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.empty();
        private BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> getInsertLayout = defaultGetInsertLayout();
        private BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> getNewTableLayout = defaultGetNewTableLayout();
        private BiFunction<ConnectorSession, Type, Optional<Type>> getSupportedType = (session, type) -> Optional.empty();
        private BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties = defaultGetTableProperties();
        private BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges = defaultListTablePrivileges();
        private Supplier<Iterable<EventListener>> eventListeners = ImmutableList::of;
        private Collection<FunctionMetadata> functions = ImmutableList.of();
        private ApplyTopN applyTopN = (session, handle, topNCount, sortItems, assignments) -> Optional.empty();
        private ApplyFilter applyFilter = (session, handle, constraint) -> Optional.empty();
        private ApplyTableFunction applyTableFunction = (session, handle) -> Optional.empty();
        private ApplyTableScanRedirect applyTableScanRedirect = (session, handle) -> Optional.empty();
        private BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable = (session, tableName) -> Optional.empty();
        private Function<SchemaTableName, List<List<?>>> data = schemaTableName -> ImmutableList.of();
        private Function<SchemaTableName, Metrics> metrics = schemaTableName -> EMPTY;
        private Set<Procedure> procedures = ImmutableSet.of();
        private Set<TableProcedureMetadata> tableProcedures = ImmutableSet.of();
        private Set<ConnectorTableFunction> tableFunctions = ImmutableSet.of();
        private Optional<FunctionProvider> functionProvider = Optional.empty();
        private Supplier<List<PropertyMetadata<?>>> analyzeProperties = ImmutableList::of;
        private Supplier<List<PropertyMetadata<?>>> schemaProperties = ImmutableList::of;
        private Supplier<List<PropertyMetadata<?>>> tableProperties = ImmutableList::of;
        private Supplier<List<PropertyMetadata<?>>> columnProperties = ImmutableList::of;
        private Optional<ConnectorNodePartitioningProvider> partitioningProvider = Optional.empty();
        private Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources = handle -> null;

        // access control
        private boolean provideAccessControl;
        private ListRoleGrants roleGrants = defaultRoleAuthorizations();
        private Grants<String> schemaGrants = new AllowAllGrants<>();
        private Grants<SchemaTableName> tableGrants = new AllowAllGrants<>();
        private Function<SchemaTableName, ViewExpression> rowFilter = tableName -> null;
        private BiFunction<SchemaTableName, String, ViewExpression> columnMask = (tableName, columnName) -> null;
        private boolean allowMissingColumnsOnInsert;
        private OptionalInt maxWriterTasks = OptionalInt.empty();
        private Function<ConnectorTableHandle, Optional<CacheTableId>> getCacheTableId = handle -> Optional.empty();
        private Function<ColumnHandle, Optional<CacheColumnId>> getCacheColumnId = handle -> Optional.empty();
        private Function<ConnectorTableHandle, ConnectorTableHandle> getCanonicalTableHandle = Function.identity();
        private BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute = (session, handle) -> Optional.empty();
        private WriterScalingOptions writerScalingOptions = WriterScalingOptions.DISABLED;
        private Supplier<Set<ConnectorCapabilities>> capabilities = ImmutableSet::of;
        private boolean allowSplittingReadIntoMultipleSubQueries;

        private Builder() {}

        public Builder withName(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder withSessionProperty(PropertyMetadata<?> sessionProperty)
        {
            sessionProperties.add(sessionProperty);
            return this;
        }

        public Builder withSessionProperties(Iterable<PropertyMetadata<?>> sessionProperties)
        {
            for (PropertyMetadata<?> sessionProperty : sessionProperties) {
                withSessionProperty(sessionProperty);
            }
            return this;
        }

        public Builder withMetadataWrapper(Function<ConnectorMetadata, ConnectorMetadata> metadataWrapper)
        {
            this.metadataWrapper = requireNonNull(metadataWrapper, "metadataWrapper is null");
            return this;
        }

        public Builder withListSchemaNames(Function<ConnectorSession, List<String>> listSchemaNames)
        {
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            return this;
        }

        public Builder withListTables(BiFunction<ConnectorSession, String, List<String>> listTables)
        {
            this.listTables = requireNonNull(listTables, "listTables is null");
            return this;
        }

        public Builder withStreamTableColumns(BiFunction<ConnectorSession, SchemaTablePrefix, Iterator<TableColumnsMetadata>> streamTableColumns)
        {
            this.streamTableColumns = Optional.of(requireNonNull(streamTableColumns, "streamTableColumns is null"));
            return this;
        }

        public Builder withStreamRelationColumns(StreamRelationColumns streamRelationColumns)
        {
            this.streamRelationColumns = Optional.of(streamRelationColumns);
            return this;
        }

        public Builder withGetViews(BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews)
        {
            this.getViews = requireNonNull(getViews, "getViews is null");
            return this;
        }

        public Builder withGetViewProperties(Supplier<List<PropertyMetadata<?>>> getViewProperties)
        {
            this.getViewProperties = requireNonNull(getViewProperties, "getViewProperties is null");
            return this;
        }

        public Builder withGetMaterializedViewProperties(Supplier<List<PropertyMetadata<?>>> getMaterializedViewProperties)
        {
            this.getMaterializedViewProperties = requireNonNull(getMaterializedViewProperties, "getMaterializedViewProperties is null");
            return this;
        }

        public Builder withGetMaterializedViews(BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews)
        {
            this.getMaterializedViews = requireNonNull(getMaterializedViews, "getMaterializedViews is null");
            return this;
        }

        public Builder withDelegateMaterializedViewRefreshToConnector(BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector)
        {
            this.delegateMaterializedViewRefreshToConnector = requireNonNull(delegateMaterializedViewRefreshToConnector, "delegateMaterializedViewRefreshToConnector is null");
            return this;
        }

        public Builder withRefreshMaterializedView(BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView)
        {
            this.refreshMaterializedView = requireNonNull(refreshMaterializedView, "refreshMaterializedView is null");
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

        public Builder withGetComment(Function<SchemaTableName, Optional<String>> getComment)
        {
            this.getComment = requireNonNull(getComment, "getComment is null");
            return this;
        }

        public Builder withGetTableStatistics(Function<SchemaTableName, TableStatistics> getTableStatistics)
        {
            this.getTableStatistics = requireNonNull(getTableStatistics, "getColumns is null");
            return this;
        }

        public Builder withCheckConstraints(Function<SchemaTableName, List<String>> checkConstraints)
        {
            this.checkConstraints = requireNonNull(checkConstraints, "checkConstraints is null");
            return this;
        }

        public Builder withApplyProjection(ApplyProjection applyProjection)
        {
            this.applyProjection = applyProjection;
            return this;
        }

        public Builder withApplyAggregation(ApplyAggregation applyAggregation)
        {
            this.applyAggregation = applyAggregation;
            return this;
        }

        public Builder withApplyJoin(ApplyJoin applyJoin)
        {
            this.applyJoin = applyJoin;
            return this;
        }

        public Builder withApplyTopN(ApplyTopN applyTopN)
        {
            this.applyTopN = applyTopN;
            return this;
        }

        public Builder withApplyFilter(ApplyFilter applyFilter)
        {
            this.applyFilter = applyFilter;
            return this;
        }

        public Builder withApplyTableFunction(ApplyTableFunction applyTableFunction)
        {
            this.applyTableFunction = applyTableFunction;
            return this;
        }

        public Builder withApplyTableScanRedirect(ApplyTableScanRedirect applyTableScanRedirect)
        {
            this.applyTableScanRedirect = applyTableScanRedirect;
            return this;
        }

        public Builder withRedirectTable(BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable)
        {
            this.redirectTable = requireNonNull(redirectTable, "redirectTable is null");
            return this;
        }

        public Builder withGetInsertLayout(BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> getInsertLayout)
        {
            this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
            return this;
        }

        public Builder withGetNewTableLayout(BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> getNewTableLayout)
        {
            this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
            return this;
        }

        public Builder withGetSupportedType(BiFunction<ConnectorSession, Type, Optional<Type>> getSupportedType)
        {
            this.getSupportedType = requireNonNull(getSupportedType, "getSupportedType is null");
            return this;
        }

        public Builder withGetLayoutForTableExecute(BiFunction<ConnectorSession, ConnectorTableExecuteHandle, Optional<ConnectorTableLayout>> getLayoutForTableExecute)
        {
            this.getLayoutForTableExecute = requireNonNull(getLayoutForTableExecute, "getLayoutForTableExecute is null");
            return this;
        }

        public Builder withGetTableProperties(BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties)
        {
            this.getTableProperties = requireNonNull(getTableProperties, "getTableProperties is null");
            return this;
        }

        public Builder withListTablePrivileges(BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> listTablePrivileges)
        {
            provideAccessControl = true;
            this.listTablePrivileges = requireNonNull(listTablePrivileges, "listTablePrivileges is null");
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

        public Builder withFunctions(Collection<FunctionMetadata> functions)
        {
            requireNonNull(functions, "functions is null");

            this.functions = ImmutableList.copyOf(functions);
            return this;
        }

        public Builder withData(Function<SchemaTableName, List<List<?>>> data)
        {
            this.data = requireNonNull(data, "data is null");
            return this;
        }

        public Builder withMetrics(Function<SchemaTableName, Metrics> metrics)
        {
            this.metrics = requireNonNull(metrics, "metrics is null");
            return this;
        }

        public Builder withProcedures(Iterable<Procedure> procedures)
        {
            this.procedures = ImmutableSet.copyOf(procedures);
            return this;
        }

        public Builder withTableProcedures(Iterable<TableProcedureMetadata> tableProcedures)
        {
            this.tableProcedures = ImmutableSet.copyOf(tableProcedures);
            return this;
        }

        public Builder withTableFunctions(Iterable<ConnectorTableFunction> tableFunctions)
        {
            this.tableFunctions = ImmutableSet.copyOf(tableFunctions);
            return this;
        }

        public Builder withFunctionProvider(Optional<FunctionProvider> functionProvider)
        {
            this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
            return this;
        }

        public Builder withAnalyzeProperties(Supplier<List<PropertyMetadata<?>>> analyzeProperties)
        {
            this.analyzeProperties = requireNonNull(analyzeProperties, "analyzeProperties is null");
            return this;
        }

        public Builder withSchemaProperties(Supplier<List<PropertyMetadata<?>>> schemaProperties)
        {
            this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
            return this;
        }

        public Builder withTableProperties(Supplier<List<PropertyMetadata<?>>> tableProperties)
        {
            this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
            return this;
        }

        public Builder withColumnProperties(Supplier<List<PropertyMetadata<?>>> columnProperties)
        {
            this.columnProperties = requireNonNull(columnProperties, "columnProperties is null");
            return this;
        }

        public Builder withPartitionProvider(ConnectorNodePartitioningProvider partitioningProvider)
        {
            this.partitioningProvider = Optional.of(partitioningProvider);
            return this;
        }

        public Builder withTableFunctionSplitSources(Function<ConnectorTableFunctionHandle, ConnectorSplitSource> sourceProvider)
        {
            tableFunctionSplitsSources = requireNonNull(sourceProvider, "sourceProvider is null");
            return this;
        }

        public Builder withListRoleGrants(ListRoleGrants roleGrants)
        {
            provideAccessControl = true;
            this.roleGrants = requireNonNull(roleGrants, "roleGrants is null");
            return this;
        }

        public Builder withSchemaGrants(Grants<String> schemaGrants)
        {
            provideAccessControl = true;
            this.schemaGrants = schemaGrants;
            return this;
        }

        public Builder withTableGrants(Grants<SchemaTableName> tableGrants)
        {
            provideAccessControl = true;
            this.tableGrants = tableGrants;
            return this;
        }

        public Builder withRowFilter(Function<SchemaTableName, ViewExpression> rowFilter)
        {
            provideAccessControl = true;
            this.rowFilter = rowFilter;
            return this;
        }

        public Builder withColumnMask(BiFunction<SchemaTableName, String, ViewExpression> columnMask)
        {
            provideAccessControl = true;
            this.columnMask = columnMask;
            return this;
        }

        public Builder withMaxWriterTasks(OptionalInt maxWriterTasks)
        {
            this.maxWriterTasks = maxWriterTasks;
            return this;
        }

        public Builder withGetCacheTableId(Function<ConnectorTableHandle, Optional<CacheTableId>> getCacheTableId)
        {
            this.getCacheTableId = requireNonNull(getCacheTableId, "getCacheTableId is null");
            return this;
        }

        public Builder withGetCacheColumnId(Function<ColumnHandle, Optional<CacheColumnId>> getCacheColumnId)
        {
            this.getCacheColumnId = requireNonNull(getCacheColumnId, "getCacheColumnId is null");
            return this;
        }

        public Builder withGetCanonicalTableHandle(Function<ConnectorTableHandle, ConnectorTableHandle> getCanonicalTableHandle)
        {
            this.getCanonicalTableHandle = requireNonNull(getCanonicalTableHandle, "getCanonicalTableHandle is null");
            return this;
        }

        public Builder withAllowMissingColumnsOnInsert(boolean allowMissingColumnsOnInsert)
        {
            this.allowMissingColumnsOnInsert = allowMissingColumnsOnInsert;
            return this;
        }

        public Builder withWriterScalingOptions(WriterScalingOptions writerScalingOptions)
        {
            this.writerScalingOptions = writerScalingOptions;
            return this;
        }

        public Builder withCapabilities(Supplier<Set<ConnectorCapabilities>> capabilities)
        {
            this.capabilities = capabilities;
            return this;
        }

        public Builder withAllowSplittingReadIntoMultipleSubQueries(boolean allowSplittingReadIntoMultipleSubQueries)
        {
            this.allowSplittingReadIntoMultipleSubQueries = allowSplittingReadIntoMultipleSubQueries;
            return this;
        }

        public MockConnectorFactory build()
        {
            Optional<ConnectorAccessControl> accessControl = Optional.empty();
            if (provideAccessControl) {
                accessControl = Optional.of(new MockConnectorAccessControl(schemaGrants, tableGrants, rowFilter, columnMask));
            }
            return new MockConnectorFactory(
                    name,
                    sessionProperties,
                    metadataWrapper,
                    listSchemaNames,
                    listTables,
                    streamTableColumns,
                    streamRelationColumns,
                    getViews,
                    getViewProperties,
                    getMaterializedViewProperties,
                    getMaterializedViews,
                    delegateMaterializedViewRefreshToConnector,
                    refreshMaterializedView,
                    getTableHandle,
                    getColumns,
                    getComment,
                    getTableStatistics,
                    checkConstraints,
                    applyProjection,
                    applyAggregation,
                    applyJoin,
                    applyTopN,
                    applyFilter,
                    applyTableFunction,
                    applyTableScanRedirect,
                    redirectTable,
                    getInsertLayout,
                    getNewTableLayout,
                    getSupportedType,
                    getTableProperties,
                    listTablePrivileges,
                    eventListeners,
                    functions,
                    data,
                    metrics,
                    procedures,
                    tableProcedures,
                    tableFunctions,
                    functionProvider,
                    analyzeProperties,
                    schemaProperties,
                    tableProperties,
                    columnProperties,
                    partitioningProvider,
                    roleGrants,
                    accessControl,
                    allowMissingColumnsOnInsert,
                    tableFunctionSplitsSources,
                    maxWriterTasks,
                    getCacheTableId,
                    getCacheColumnId,
                    getCanonicalTableHandle,
                    getLayoutForTableExecute,
                    writerScalingOptions,
                    capabilities,
                    allowSplittingReadIntoMultipleSubQueries);
        }

        public static Function<ConnectorSession, List<String>> defaultListSchemaNames()
        {
            return session -> ImmutableList.of();
        }

        public static ListRoleGrants defaultRoleAuthorizations()
        {
            return (session, roles, grantees, limit) -> ImmutableSet.of();
        }

        public static BiFunction<ConnectorSession, String, List<String>> defaultListTables()
        {
            return (session, schemaName) -> ImmutableList.of();
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> defaultGetViews()
        {
            return (session, schemaTablePrefix) -> ImmutableMap.of();
        }

        public static Supplier<List<PropertyMetadata<?>>> defaultGetViewProperties()
        {
            return ImmutableList::of;
        }

        public static Supplier<List<PropertyMetadata<?>>> defaultGetMaterializedViewProperties()
        {
            return ImmutableList::of;
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> defaultGetMaterializedViews()
        {
            return (session, schemaTablePrefix) -> ImmutableMap.of();
        }

        public static BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> defaultGetTableHandle()
        {
            return (session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName);
        }

        public static BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorTableLayout>> defaultGetInsertLayout()
        {
            return (session, schemaTableName) -> Optional.empty();
        }

        public static BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorTableLayout>> defaultGetNewTableLayout()
        {
            return (session, tableMetadata) -> Optional.empty();
        }

        public static BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> defaultGetTableProperties()
        {
            return (session, tableHandle) -> new ConnectorTableProperties();
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, List<GrantInfo>> defaultListTablePrivileges()
        {
            return (session, tableHandle) -> ImmutableList.of();
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
