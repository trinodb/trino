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
package io.trino.plugin.varada.dispatcher;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.expression.rewrite.ExpressionService;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorResolvedIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory.STATS_DISPATCHER_KEY;
import static io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory.createFixedStatKey;
import static java.util.Objects.requireNonNull;

public class DispatcherMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(DispatcherMetadata.class);

    private final ConnectorMetadata proxiedConnectorMetadata;
    private final ExpressionService expressionService;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;
    private final GlobalConfiguration globalConfiguration;

    public DispatcherMetadata(ConnectorMetadata proxiedConnectorMetadata,
            ExpressionService expressionService,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            GlobalConfiguration globalConfiguration)
    {
        this.proxiedConnectorMetadata = requireNonNull(proxiedConnectorMetadata);
        this.expressionService = requireNonNull(expressionService);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
        this.globalConfiguration = requireNonNull(globalConfiguration);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return convertTableHandle(
                session,
                proxiedConnectorMetadata.getTableHandle(session, tableName, startVersion, endVersion));
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return proxiedConnectorMetadata.getSystemTable(session, tableName);
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return convertTableHandle(
                session,
                dispatcherTableHandle,
                proxiedConnectorMetadata.makeCompatiblePartitioning(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        partitioningHandle));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        return proxiedConnectorMetadata.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableName(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableSchema(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableMetadata(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getInfo(((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getColumnHandles(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return proxiedConnectorMetadata.getColumnMetadata(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                columnHandle);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getTableStatistics(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        proxiedConnectorMetadata.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        proxiedConnectorMetadata.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        proxiedConnectorMetadata.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setSchemaAuthorization(session, source, principal);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        proxiedConnectorMetadata.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        proxiedConnectorMetadata.dropTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        proxiedConnectorMetadata.truncateTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        proxiedConnectorMetadata.renameTable(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        proxiedConnectorMetadata.setTableProperties(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                properties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        proxiedConnectorMetadata.setTableComment(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        proxiedConnectorMetadata.setColumnComment(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column,
                comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        proxiedConnectorMetadata.addColumn(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        proxiedConnectorMetadata.setColumnType(session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column,
                type);
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        ConnectorTableHandle proxyConnectorTableHandle = ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle();
        proxiedConnectorMetadata.setFieldType(session, proxyConnectorTableHandle, fieldPath, type);
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        ConnectorTableHandle proxyConnectorTableHandle = ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle();
        proxiedConnectorMetadata.renameField(session, proxyConnectorTableHandle, fieldPath, target);
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return proxiedConnectorMetadata.getNewTableWriterScalingOptions(session, tableName, tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return proxiedConnectorMetadata.getInsertWriterScalingOptions(session, dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setTableAuthorization(session, tableName, principal);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        proxiedConnectorMetadata.renameColumn(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                source,
                target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        proxiedConnectorMetadata.dropColumn(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                column);
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        DispatcherTableHandle dispatcherMergeTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.dropField(session, dispatcherMergeTableHandle.getProxyConnectorTableHandle(), column, fieldPath);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return proxiedConnectorMetadata.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getInsertLayout(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return proxiedConnectorMetadata.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        ConnectorAnalyzeMetadata proxiedResult = proxiedConnectorMetadata.getStatisticsCollectionMetadata(
                session,
                dispatcherTableHandle.getProxyConnectorTableHandle(),
                analyzeProperties);

        DispatcherTableHandle dispatcherTableHandleResult = convertTableHandle(
                session,
                dispatcherTableHandle,
                proxiedResult.getTableHandle());

        return new ConnectorAnalyzeMetadata(dispatcherTableHandleResult, proxiedResult.getStatisticsMetadata());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return convertTableHandle(
                session,
                dispatcherTableHandle,
                proxiedConnectorMetadata.beginStatisticsCollection(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle()));
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        proxiedConnectorMetadata.finishStatisticsCollection(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                computedStatistics);
    }

    @SuppressWarnings("deprecation")
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        return proxiedConnectorMetadata.beginCreateTable(session, tableMetadata, layout, retryMode);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        return proxiedConnectorMetadata.beginCreateTable(session, tableMetadata, layout, retryMode, replace);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        proxiedConnectorMetadata.createTable(session, tableMetadata, saveMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return proxiedConnectorMetadata.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        proxiedConnectorMetadata.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        proxiedConnectorMetadata.cleanupQuery(session);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return proxiedConnectorMetadata.beginInsert(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                columns,
                retryMode);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return proxiedConnectorMetadata.supportsMissingColumnsOnInsert();
    }

    @SuppressWarnings("deprecation")
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return proxiedConnectorMetadata.finishInsert(session, insertHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return proxiedConnectorMetadata.finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.refreshMaterializedView(session, viewName);
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            RetryMode retryMode)
    {
        return proxiedConnectorMetadata.beginRefreshMaterializedView(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                sourceTableHandles.stream()
                        .map(connectorTableHandle -> {
                            if (connectorTableHandle instanceof DispatcherTableHandle dispatcherTableHandle) {
                                return dispatcherTableHandle.getProxyConnectorTableHandle();
                            }
                            return connectorTableHandle;
                        })
                        .toList(),
                retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session,
                                                                           ConnectorTableHandle tableHandle,
                                                                           ConnectorInsertTableHandle insertHandle,
                                                                           Collection<Slice> fragments,
                                                                           Collection<ComputedStatistics> computedStatistics,
                                                                           List<ConnectorTableHandle> sourceTableHandles,
                                                                           List<String> sourceTableFunctions)
    {
        return proxiedConnectorMetadata.finishRefreshMaterializedView(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                insertHandle,
                fragments,
                computedStatistics,
                sourceTableHandles.stream()
                        .map(connectorTableHandle -> {
                            if (connectorTableHandle instanceof DispatcherTableHandle dispatcherTableHandle) {
                                return dispatcherTableHandle.getProxyConnectorTableHandle();
                            }
                            return connectorTableHandle;
                        })
                        .collect(Collectors.toList()),
                sourceTableFunctions);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        proxiedConnectorMetadata.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        proxiedConnectorMetadata.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        proxiedConnectorMetadata.setViewAuthorization(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        proxiedConnectorMetadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.getView(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.getSchemaOwner(session, schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.applyDelete(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle())
                .map(ret ->
                        convertTableHandle(
                                session,
                                dispatcherTableHandle,
                                ret));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return proxiedConnectorMetadata.executeDelete(
                session,
                ((DispatcherTableHandle) handle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return proxiedConnectorMetadata.resolveIndex(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                indexableColumns,
                outputColumns,
                tupleDomain);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return proxiedConnectorMetadata.roleExists(session, role);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        proxiedConnectorMetadata.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listRoles(session);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        proxiedConnectorMetadata.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return proxiedConnectorMetadata.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        proxiedConnectorMetadata.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return proxiedConnectorMetadata.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return proxiedConnectorMetadata.listEnabledRoles(session);
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        proxiedConnectorMetadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        proxiedConnectorMetadata.denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        proxiedConnectorMetadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return proxiedConnectorMetadata.listTablePrivileges(session, prefix);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return proxiedConnectorMetadata.getTableProperties(
                session,
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle());
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return proxiedConnectorMetadata.streamRelationComments(session, schemaName, relationFilter);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;

        if (dispatcherTableHandle.getLimit().equals(OptionalLong.of(limit))) {
            return Optional.empty();
        }

        Optional<LimitApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyLimit(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        limit);

        boolean isLimitGuaranteed = false;
        if (resultOpt.isPresent()) {
            LimitApplicationResult<ConnectorTableHandle> result = resultOpt.get();
            dispatcherTableHandle = createTableHandleBuilder(
                    session,
                    Optional.of(dispatcherTableHandle),
                    result.getHandle())
                    .limit(limit)
                    .build();
            isLimitGuaranteed = result.isLimitGuaranteed();
        }
        else {
            dispatcherTableHandle = createTableHandleBuilder(
                    session,
                    Optional.of(dispatcherTableHandle),
                    dispatcherTableHandle.getProxyConnectorTableHandle())
                    .limit(limit)
                    .build();
        }
        return Optional.of(new LimitApplicationResult<>(dispatcherTableHandle, isLimitGuaranteed, false));
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        proxiedConnectorMetadata.setMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return proxiedConnectorMetadata.streamRelationColumns(session, schemaName, relationFilter);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint constraint)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;

        if (logger.isDebugEnabled()) {
            logger.debug("ApplyFilter - Input tupleDomain: %s, expression: %s",
                    constraint.getSummary().toString(session),
                    constraint.getExpression().toString());
        }
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyFilter(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        constraint);

        // TODO: VDB-5850 The new varadaExpressions should be merged with the existing ones at the table handle
        // TODO: and we should check that no changes were made to the varadaExpressions before returning Optional.empty().
        Map<String, Long> customStats = new HashMap<>();

        Optional<WarpExpression> warpExpression = dispatcherTableHandle.getWarpExpression();
        if (warpExpression.isEmpty()) {
            warpExpression = expressionService.convertToWarpExpression(session,
                constraint.getExpression(),
                constraint.getAssignments(),
                customStats);
        }
        // Build and return result
        if (resultOpt.isEmpty()) {
            if (dispatcherTableHandle.getWarpExpression().equals(warpExpression)) {
                return createSubsumedPredicatesAlternative(session, dispatcherTableHandle, constraint.getExpression())
                        .flatMap(alternative -> Optional.of(new ConstraintApplicationResult<>(true, List.of(alternative))));
            }
            return createConstraintApplicationResult(
                    session,
                    constraint,
                    dispatcherTableHandle,
                    constraint.getSummary(),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    warpExpression,
                    customStats);
        }

        TupleDomain<ColumnHandle> newRemainingFilter = resultOpt.get().getAlternatives().get(0).remainingFilter()
                .transformKeys(ColumnHandle.class::cast);
        if (logger.isDebugEnabled()) {
            logger.debug("Will return to Presto the following remaining Filter: %s", newRemainingFilter.toString(session));
        }

        return createConstraintApplicationResult(
                session,
                constraint,
                dispatcherTableHandle,
                newRemainingFilter,
                resultOpt.get().getAlternatives().get(0).handle(),
                warpExpression,
                customStats);
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> createConstraintApplicationResult(
            ConnectorSession session,
            Constraint constraint,
            DispatcherTableHandle table,
            TupleDomain<ColumnHandle> newRemainingFilter,
            ConnectorTableHandle proxiedConnectorTableHandle,
            Optional<WarpExpression> warpExpression,
            Map<String, Long> customStatsMap)
    {
        Map<String, Long> allStatsMap = table.getCustomStats().stream()
                .collect(Collectors.toMap(CustomStat::statName, CustomStat::statValue, (a, b) -> a, HashMap::new));
        customStatsMap.forEach((key, value) -> allStatsMap.merge(createFixedStatKey(STATS_DISPATCHER_KEY, key), value, Long::sum));

        List<CustomStat> customStats = allStatsMap.entrySet().stream()
                .map(entry -> new CustomStat(entry.getKey(), entry.getValue()))
                .toList();

        TupleDomain<ColumnHandle> fullPredicate = table.getFullPredicate().intersect(newRemainingFilter);
        DispatcherTableHandle dispatcherTableHandle = createTableHandleBuilder(session, Optional.of(table), proxiedConnectorTableHandle)
                .warpExpression(warpExpression)
                .customStats(customStats)
                .fullPredicate(fullPredicate)
                .subsumedPredicates(false)
                .build();

        List<ConstraintApplicationResult.Alternative<ConnectorTableHandle>> alternatives = new ArrayList<>(2);
        alternatives.add(new ConstraintApplicationResult.Alternative<>(dispatcherTableHandle, newRemainingFilter, Optional.empty(), false));

        createSubsumedPredicatesAlternative(session, dispatcherTableHandle, constraint.getExpression())
                .ifPresent(alternatives::add);

        return Optional.of(new ConstraintApplicationResult<>(false, alternatives));
    }

    private Optional<ConstraintApplicationResult.Alternative<ConnectorTableHandle>> createSubsumedPredicatesAlternative(
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorExpression remainingExpression)
    {
        if (!dispatcherTableHandle.getFullPredicate().isAll()) {
            // TODO: Support expressions (we currently don't have a way to know if the expression is fully subsumed or not)
            DispatcherTableHandle table = createTableHandleBuilder(session, Optional.of(dispatcherTableHandle), dispatcherTableHandle.getProxyConnectorTableHandle())
                    .subsumedPredicates(true)
                    .build();
            return Optional.of(new ConstraintApplicationResult.Alternative<>(
                    table,
                    TupleDomain.all(), // in this alternative, TupleDomain is fully subsumed
                    Optional.of(remainingExpression), // currently, Trino has to filter expressions after WarpSpeed
                    false));
        }
        return Optional.empty();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyProjection(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        projections,
                        assignments);
        return resultOpt.map(result -> new ProjectionApplicationResult<>(
                convertTableHandle(session, dispatcherTableHandle, result.getHandle()),
                result.getProjections(),
                result.getAssignments(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(
            ConnectorSession session,
            ConnectorTableHandle handle,
            SampleType sampleType,
            double sampleRatio)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.applySample(session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        sampleType,
                        sampleRatio)
                .map(result -> new SampleApplicationResult<>(
                        convertTableHandle(session, dispatcherTableHandle, result.getHandle()),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        Optional<AggregationApplicationResult<ConnectorTableHandle>> resultOpt =
                proxiedConnectorMetadata.applyAggregation(
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        aggregates,
                        assignments,
                        groupingSets);
        return resultOpt.map(result -> new AggregationApplicationResult<>(
                convertTableHandle(session, dispatcherTableHandle, result.getHandle()),
                result.getProjections(),
                result.getAssignments(),
                result.getGroupingColumnMapping(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        proxiedConnectorMetadata.validateScan(
                session,
                ((DispatcherTableHandle) handle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        return proxiedConnectorMetadata.getTableHandleForExecute(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                procedureName,
                executeProperties,
                retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return proxiedConnectorMetadata.getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) updatedSourceTableHandle;
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecuteResult =
                proxiedConnectorMetadata.beginTableExecute(
                        session,
                        tableExecuteHandle,
                        dispatcherTableHandle.getProxyConnectorTableHandle());
        return new BeginTableExecuteResult<>(
                beginTableExecuteResult.getTableExecuteHandle(),
                convertTableHandle(
                        session,
                        dispatcherTableHandle,
                        beginTableExecuteResult.getSourceHandle()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        proxiedConnectorMetadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        proxiedConnectorMetadata.executeTableExecute(session, tableExecuteHandle);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> properties, boolean replace, boolean ignoreExisting)
    {
        proxiedConnectorMetadata.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return proxiedConnectorMetadata.getMaterializedViewProperties(session, viewName, materializedViewDefinition);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        proxiedConnectorMetadata.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return proxiedConnectorMetadata.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return proxiedConnectorMetadata.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        proxiedConnectorMetadata.renameMaterializedView(session, source, target);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.applyTableScanRedirect(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return proxiedConnectorMetadata.redirectTable(session, tableName);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        Optional<TableFunctionApplicationResult<ConnectorTableHandle>> proxiedResult = proxiedConnectorMetadata.applyTableFunction(session, handle);

        return proxiedResult.map(result ->
                new TableFunctionApplicationResult<>(
                        convertTableHandle(session, result.getTableHandle()),
                        result.getColumnHandles()));
    }

    @Override
    public OptionalInt getMaxWriterTasks(ConnectorSession session)
    {
        return proxiedConnectorMetadata.getMaxWriterTasks(session);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            RetryMode retryMode)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        ConnectorMergeTableHandle proxyConnectorMergeTableHandle = proxiedConnectorMetadata.beginMerge(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(),
                retryMode);
        return new DispatcherMergeTableHandle(
                dispatcherTableHandle,
                proxyConnectorMergeTableHandle);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getMergeRowIdColumnHandle(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getUpdateLayout(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return proxiedConnectorMetadata.getRowChangeParadigm(
                session,
                ((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle());
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        DispatcherMergeTableHandle dispatcherMergeTableHandle = (DispatcherMergeTableHandle) tableHandle;
        proxiedConnectorMetadata.finishMerge(
                session,
                dispatcherMergeTableHandle.getProxyConnectorMergeTableHandle(),
                fragments,
                computedStatistics);
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.addField(session, dispatcherTableHandle.getProxyConnectorTableHandle(), parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.listFunctions(session, schemaName);
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return proxiedConnectorMetadata.getFunctions(session, name);
    }

    @Override
    public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        return proxiedConnectorMetadata.applyUpdate(session, dispatcherTableHandle.getProxyConnectorTableHandle(), assignments);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) handle;
        return proxiedConnectorMetadata.executeUpdate(session, dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return proxiedConnectorMetadata.getFunctionMetadata(session, functionId);
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return proxiedConnectorMetadata.getAggregationFunctionMetadata(session, functionId);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return proxiedConnectorMetadata.getFunctionDependencies(session, functionId, boundSignature);
    }

    @Override
    public boolean isColumnarTableScan(ConnectorSession session, ConnectorTableHandle proxiedConnectorTableHandle)
    {
//        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) proxiedConnectorTableHandle;
//        return proxiedConnectorMetadata.isColumnarTableScan(session, dispatcherTableHandle.getProxyConnectorTableHandle());

        // @see * 9911e9420a lukasz-stec:  (tag: 425-galaxy-1-u46-g9911e9420a) Fire MultipleDistinctAggregationsToSubqueries automatically
        // TODO uncomment after adding test that validate that multiple count(distinct col) on the same table
        // SIC-1451
        return false;
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> map, Type type)
    {
        return proxiedConnectorMetadata.getSupportedType(session, map, type);
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return proxiedConnectorMetadata.listLanguageFunctions(session, schemaName);
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return proxiedConnectorMetadata.getLanguageFunctions(session, name);
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return proxiedConnectorMetadata.languageFunctionExists(session, name, signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        proxiedConnectorMetadata.createLanguageFunction(session, name, function, replace);
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        ConnectorMetadata.super.dropLanguageFunction(session, name, signatureToken);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        return proxiedConnectorMetadata.getRelationTypes(session, schemaName);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        proxiedConnectorMetadata.dropNotNullConstraint(session, dispatcherTableHandle.getProxyConnectorTableHandle(), column);
    }

    @Override
    public String getCatalogIdentity(ConnectorSession session)
    {
        return proxiedConnectorMetadata.getCatalogIdentity(session);
    }

    private DispatcherTableHandle convertTableHandle(ConnectorSession session, ConnectorTableHandle proxiedConnectorTableHandle)
    {
        return convertTableHandle(session, null, proxiedConnectorTableHandle);
    }

    private DispatcherTableHandle convertTableHandle(
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorTableHandle proxiedConnectorTableHandle)
    {
        if (proxiedConnectorTableHandle == null) {
            return null;
        }
        return createTableHandleBuilder(session, Optional.ofNullable(dispatcherTableHandle), proxiedConnectorTableHandle)
                .build();
    }

    private DispatcherTableHandleBuilderProvider.Builder createTableHandleBuilder(
            ConnectorSession session,
            Optional<DispatcherTableHandle> optionalDispatcherTableHandle,
            ConnectorTableHandle proxiedConnectorTableHandle)
    {
        int predicateThreashold = VaradaSessionProperties.getPredicateSimplifyThreshold(session, globalConfiguration);

        return optionalDispatcherTableHandle.map(dispatcherTableHandle ->
                        dispatcherTableHandleBuilderProvider.builder(dispatcherTableHandle, predicateThreashold)
                                .proxiedConnectorTableHandle(proxiedConnectorTableHandle))
                .orElse(dispatcherTableHandleBuilderProvider.builder(predicateThreashold, proxiedConnectorTableHandle));
    }
}
