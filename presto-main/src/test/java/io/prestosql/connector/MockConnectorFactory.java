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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.AggregationApplicationResult;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SortItem;
import io.prestosql.spi.connector.TopNApplicationResult;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.security.RoleGrant;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final ApplyAggregation applyAggregation;
    private final ApplyTopN applyTopN;
    private final BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout;
    private final BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout;
    private final Supplier<Iterable<EventListener>> eventListeners;
    private final ListRoleGrants roleGrants;
    private final MockConnectorAccessControl accessControl;

    private MockConnectorFactory(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle,
            Function<SchemaTableName, List<ColumnMetadata>> getColumns,
            ApplyProjection applyProjection,
            ApplyAggregation applyAggregation,
            ApplyTopN applyTopN,
            BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout,
            BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout,
            Supplier<Iterable<EventListener>> eventListeners,
            ListRoleGrants roleGrants,
            MockConnectorAccessControl accessControl)
    {
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getTableHandle = requireNonNull(getTableHandle, "getTableHandle is null");
        this.getColumns = requireNonNull(getColumns, "getColumns is null");
        this.applyProjection = requireNonNull(applyProjection, "applyProjection is null");
        this.applyAggregation = requireNonNull(applyAggregation, "applyAggregation is null");
        this.applyTopN = requireNonNull(applyTopN, "applyTopN is null");
        this.getInsertLayout = requireNonNull(getInsertLayout, "getInsertLayout is null");
        this.getNewTableLayout = requireNonNull(getNewTableLayout, "getNewTableLayout is null");
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
                getViews,
                getTableHandle,
                getColumns,
                applyProjection,
                applyAggregation,
                applyTopN,
                getInsertLayout,
                getNewTableLayout,
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
    public interface ListRoleGrants
    {
        Set<RoleGrant> apply(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit);
    }

    public static final class Builder
    {
        private Function<ConnectorSession, List<String>> listSchemaNames = defaultListSchemaNames();
        private BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = defaultListTables();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = defaultGetViews();
        private BiFunction<ConnectorSession, SchemaTableName, ConnectorTableHandle> getTableHandle = defaultGetTableHandle();
        private Function<SchemaTableName, List<ColumnMetadata>> getColumns = defaultGetColumns();
        private ApplyProjection applyProjection = (session, handle, projections, assignments) -> Optional.empty();
        private ApplyAggregation applyAggregation = (session, handle, aggregates, assignments, groupingSets) -> Optional.empty();
        private BiFunction<ConnectorSession, SchemaTableName, Optional<ConnectorNewTableLayout>> getInsertLayout = defaultGetInsertLayout();
        private BiFunction<ConnectorSession, ConnectorTableMetadata, Optional<ConnectorNewTableLayout>> getNewTableLayout = defaultGetNewTableLayout();
        private Supplier<Iterable<EventListener>> eventListeners = ImmutableList::of;
        private ListRoleGrants roleGrants = defaultRoleAuthorizations();
        private ApplyTopN applyTopN = (session, handle, topNCount, sortItems, assignments) -> Optional.empty();
        private Grants<String> schemaGrants = new AllowAllGrants<>();
        private Grants<SchemaTableName> tableGrants = new AllowAllGrants<>();

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

        public Builder withApplyAggregation(ApplyAggregation applyAggregation)
        {
            this.applyAggregation = applyAggregation;
            return this;
        }

        public Builder withApplyTopN(ApplyTopN applyTopN)
        {
            this.applyTopN = applyTopN;
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

        public MockConnectorFactory build()
        {
            return new MockConnectorFactory(
                    listSchemaNames,
                    listTables,
                    getViews,
                    getTableHandle,
                    getColumns,
                    applyProjection,
                    applyAggregation,
                    applyTopN,
                    getInsertLayout,
                    getNewTableLayout,
                    eventListeners,
                    roleGrants,
                    new MockConnectorAccessControl(schemaGrants, tableGrants));
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
