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
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.ViewExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class MockConnectorFactory
        implements ConnectorFactory
{
    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
    Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>>> streamTableColumns;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews;
    private final BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector;
    private final BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView;
    private final BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle;
    private final Function<SchemaTableName, List<ColumnMetadata>> getColumns;
    private final ApplyProjection applyProjection;
    private final ApplyAggregation applyAggregation;
    private final ApplyJoin applyJoin;
    private final ApplyTopN applyTopN;
    private final ApplyFilter applyFilter;
    private final ApplyTableScanRedirect applyTableScanRedirect;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties;
    private final Supplier<Iterable<EventListener>> eventListeners;
    private final ListRoleGrants roleGrants;
    private final MockConnectorAccessControl accessControl;

    private MockConnectorFactory(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>>> streamTableColumns,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews,
            BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector,
            BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            ApplyProjection applyProjection,
            ApplyAggregation applyAggregation,
            ApplyJoin applyJoin,
            ApplyTopN applyTopN,
            ApplyFilter applyFilter,
            ApplyTableScanRedirect applyTableScanRedirect,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable,
            BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout,
            BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout,
            BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties,
            Supplier<Iterable<EventListener>> eventListeners,
            ListRoleGrants roleGrants,
            MockConnectorAccessControl accessControl)
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
    }

    @Override
    public String getName()
    {
        return "mock";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MockConnectorHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new MockConnector(
                listSchemaNames,
                listTables,
                streamTableColumns,
                getViews,
                getMaterializedViews,
                delegateMaterializedViewRefreshToConnector,
                refreshMaterializedView,
                getTableHandle,
                getColumns,
                applyProjection,
                applyAggregation,
                applyJoin,
                applyTopN,
                applyFilter,
                applyTableScanRedirect,
                redirectTable,
                getInsertLayout,
                getNewTableLayout,
                getTableProperties,
                eventListeners,
                roleGrants,
                accessControl);
    }

    public static Builder builder()
    {
        return new Builder();
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
    public interface ListRoleGrants
    {
        Set<RoleGrant> apply(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit);
    }

    public static final class Builder
    {
        private Function<ConnectorSession, List<String>> listSchemaNames = defaultListSchemaNames();
        private BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = defaultListTables();
        private Optional<BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>>> streamTableColumns = Optional.empty();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = defaultGetViews();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> getMaterializedViews = defaultGetMaterializedViews();
        private BiFunction<ConnectorSession, SchemaTableName, Boolean> delegateMaterializedViewRefreshToConnector = (session, viewName) -> false;
        private BiFunction<ConnectorSession, SchemaTableName, CompletableFuture<?>> refreshMaterializedView = (session, viewName) -> CompletableFuture.completedFuture(null);
        private BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle = defaultGetTableHandle();
        private Function<SchemaTableName, List<ColumnMetadata>> getColumns = defaultGetColumns();
        private ApplyProjection applyProjection = (session, handle, projections, assignments) -> Optional.empty();
        private ApplyAggregation applyAggregation = (session, handle, aggregates, assignments, groupingSets) -> Optional.empty();
        private ApplyJoin applyJoin = (session, joinType, left, right, joinConditions, leftAssignments, rightAssignments) -> Optional.empty();
        private BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout = defaultGetInsertLayout();
        private BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout = defaultGetNewTableLayout();
        private BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties = defaultGetTableProperties();
        private Supplier<Iterable<EventListener>> eventListeners = ImmutableList::of;
        private ListRoleGrants roleGrants = defaultRoleAuthorizations();
        private ApplyTopN applyTopN = (session, handle, topNCount, sortItems, assignments) -> Optional.empty();
        private Grants<String> schemaGrants = new AllowAllGrants<>();
        private Grants<SchemaTableName> tableGrants = new AllowAllGrants<>();
        private ApplyFilter applyFilter = (session, handle, constraint) -> Optional.empty();
        private ApplyTableScanRedirect applyTableScanRedirect = (session, handle) -> Optional.empty();
        private Function<SchemaTableName, ViewExpression> rowFilter = (tableName) -> null;
        private BiFunction<SchemaTableName, String, ViewExpression> columnMask = (tableName, columnName) -> null;
        private BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> redirectTable = (session, tableName) -> Optional.empty();

        public Builder withListSchemaNames(Function<ConnectorSession, List<String>> listSchemaNames)
        {
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            return this;
        }

        public Builder withListRoleGrants(ListRoleGrants roleGrants)
        {
            this.roleGrants = requireNonNull(roleGrants, "roleGrants is null");
            return this;
        }

        public Builder withListTables(BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables)
        {
            this.listTables = requireNonNull(listTables, "listTables is null");
            return this;
        }

        public Builder withStreamTableColumns(BiFunction<ConnectorSession, SchemaTablePrefix, Stream<TableColumnsMetadata>> streamTableColumns)
        {
            this.streamTableColumns = Optional.of(requireNonNull(streamTableColumns, "streamTableColumns is null"));
            return this;
        }

        public Builder withGetViews(BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews)
        {
            this.getViews = requireNonNull(getViews, "getViews is null");
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

        public Builder withGetTableProperties(BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> getTableProperties)
        {
            this.getTableProperties = requireNonNull(getTableProperties, "getTableProperties is null");
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

        public Builder withSchemaGrants(Grants<String> schemaGrants)
        {
            this.schemaGrants = schemaGrants;
            return this;
        }

        public Builder withTableGrants(Grants<SchemaTableName> tableGrants)
        {
            this.tableGrants = tableGrants;
            return this;
        }

        public Builder withRowFilter(Function<SchemaTableName, ViewExpression> rowFilter)
        {
            this.rowFilter = rowFilter;
            return this;
        }

        public Builder withColumnMask(BiFunction<SchemaTableName, String, ViewExpression> columnMask)
        {
            this.columnMask = columnMask;
            return this;
        }

        public MockConnectorFactory build()
        {
            return new MockConnectorFactory(
                    listSchemaNames,
                    listTables,
                    streamTableColumns,
                    getViews,
                    getMaterializedViews,
                    delegateMaterializedViewRefreshToConnector,
                    refreshMaterializedView,
                    getTableHandle,
                    getColumns,
                    applyProjection,
                    applyAggregation,
                    applyJoin,
                    applyTopN,
                    applyFilter,
                    applyTableScanRedirect,
                    redirectTable,
                    getInsertLayout,
                    getNewTableLayout,
                    getTableProperties,
                    eventListeners,
                    roleGrants,
                    new MockConnectorAccessControl(schemaGrants, tableGrants, rowFilter, columnMask));
        }

        public static Function<ConnectorSession, List<String>> defaultListSchemaNames()
        {
            return (session) -> ImmutableList.of();
        }

        public static ListRoleGrants defaultRoleAuthorizations()
        {
            return (session, roles, grantees, limit) -> ImmutableSet.of();
        }

        public static BiFunction<ConnectorSession, String, List<SchemaTableName>> defaultListTables()
        {
            return (session, schemaName) -> ImmutableList.of();
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> defaultGetViews()
        {
            return (session, schemaTablePrefix) -> ImmutableMap.of();
        }

        public static BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorMaterializedViewDefinition>> defaultGetMaterializedViews()
        {
            return (session, schemaTablePrefix) -> ImmutableMap.of();
        }

        public static BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> defaultGetTableHandle()
        {
            return (session, schemaTableName) -> new MockConnectorTableHandle(schemaTableName);
        }

        public static BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> defaultGetInsertLayout()
        {
            return (session, schemaTableName) -> Optional.empty();
        }

        public static BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> defaultGetNewTableLayout()
        {
            return (session, tableMetadata) -> Optional.empty();
        }

        public static BiFunction<ConnectorSession, ConnectorTableHandle, ConnectorTableProperties> defaultGetTableProperties()
        {
            return (session, tableHandle) -> new ConnectorTableProperties();
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
