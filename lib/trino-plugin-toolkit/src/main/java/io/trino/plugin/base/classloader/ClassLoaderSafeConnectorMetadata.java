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
package io.trino.plugin.base.classloader;

import io.airlift.slice.Slice;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorResolvedIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static io.trino.spi.classloader.ThreadContextClassLoader.withClassLoader;
import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorMetadata
        implements ConnectorMetadata
{
    private final ConnectorMetadata delegate;
    private final ClassLoader classLoader;

    @Inject
    public ClassLoaderSafeConnectorMetadata(@ForClassLoaderSafe ConnectorMetadata delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        return withClassLoader(classLoader, () -> delegate.getTableLayouts(session, table, constraint, desiredColumns));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return withClassLoader(classLoader, () -> delegate.getTableLayout(session, handle));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        return withClassLoader(classLoader, () -> delegate.getCommonPartitioningHandle(session, left, right));
    }

    @Override
    public ConnectorTableLayoutHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        return withClassLoader(classLoader, () -> delegate.makeCompatiblePartitioning(session, tableLayoutHandle, partitioningHandle));
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        return withClassLoader(classLoader, () -> delegate.makeCompatiblePartitioning(session, tableHandle, partitioningHandle));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return withClassLoader(classLoader, () -> delegate.getNewTableLayout(session, tableMetadata));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.getInsertLayout(session, tableHandle));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return withClassLoader(classLoader, () -> delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return withClassLoader(classLoader, () -> delegate.getStatisticsCollectionMetadata(session, tableMetadata));
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.beginStatisticsCollection(session, tableHandle));
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        withClassLoader(classLoader, () -> delegate.finishStatisticsCollection(session, tableHandle, computedStatistics));
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.schemaExists(session, schemaName));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return withClassLoader(classLoader, () -> delegate.listSchemaNames(session));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return withClassLoader(classLoader, () -> delegate.getTableHandle(session, tableName));
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return withClassLoader(classLoader, () -> delegate.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties));
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return withClassLoader(classLoader, () -> delegate.getSystemTable(session, tableName));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        return withClassLoader(classLoader, () -> delegate.getTableSchema(session, table));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return withClassLoader(classLoader, () -> delegate.getTableMetadata(session, table));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle table)
    {
        return withClassLoader(classLoader, () -> delegate.getInfo(table));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return withClassLoader(classLoader, () -> delegate.getInfo(table));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.listTables(session, schemaName));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.getColumnHandles(session, tableHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return withClassLoader(classLoader, () -> delegate.getColumnMetadata(session, tableHandle, columnHandle));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return withClassLoader(classLoader, () -> delegate.listTableColumns(session, prefix));
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return withClassLoader(classLoader, () -> delegate.streamTableColumns(session, prefix));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        return withClassLoader(classLoader, () -> delegate.getTableStatistics(session, tableHandle, constraint));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        withClassLoader(classLoader, () -> delegate.addColumn(session, tableHandle, column));
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName table, TrinoPrincipal principal)
    {
        withClassLoader(classLoader, () -> delegate.setTableAuthorization(session, table, principal));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        withClassLoader(classLoader, () -> delegate.createSchema(session, schemaName, properties, owner));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        withClassLoader(classLoader, () -> delegate.dropSchema(session, schemaName));
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        withClassLoader(classLoader, () -> delegate.renameSchema(session, source, target));
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        withClassLoader(classLoader, () -> delegate.setSchemaAuthorization(session, source, principal));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        withClassLoader(classLoader, () -> delegate.createTable(session, tableMetadata, ignoreExisting));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        withClassLoader(classLoader, () -> delegate.dropTable(session, tableHandle));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        withClassLoader(classLoader, () -> delegate.renameColumn(session, tableHandle, source, target));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        withClassLoader(classLoader, () -> delegate.dropColumn(session, tableHandle, column));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        withClassLoader(classLoader, () -> delegate.renameTable(session, tableHandle, newTableName));
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        withClassLoader(classLoader, () -> delegate.setTableComment(session, tableHandle, comment));
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        withClassLoader(classLoader, () -> delegate.setColumnComment(session, tableHandle, column, comment));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return withClassLoader(classLoader, () -> delegate.beginCreateTable(session, tableMetadata, layout));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return withClassLoader(classLoader, () -> delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics));
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        withClassLoader(classLoader, () -> delegate.beginQuery(session));
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        withClassLoader(classLoader, () -> delegate.cleanupQuery(session));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.beginInsert(session, tableHandle));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        return withClassLoader(classLoader, () -> delegate.beginInsert(session, tableHandle, columns));
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return withClassLoader(classLoader, () -> delegate.supportsMissingColumnsOnInsert());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return withClassLoader(classLoader, () -> delegate.finishInsert(session, insertHandle, fragments, computedStatistics));
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return withClassLoader(classLoader, () -> delegate.delegateMaterializedViewRefreshToConnector(session, viewName));
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return withClassLoader(classLoader, () -> delegate.refreshMaterializedView(session, viewName));
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles)
    {
        return withClassLoader(classLoader, () -> delegate.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles));
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
        return withClassLoader(classLoader, () -> delegate.finishRefreshMaterializedView(session, tableHandle, insertHandle, fragments, computedStatistics, sourceTableHandles));
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.getDeleteRowIdColumnHandle(session, tableHandle));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        withClassLoader(classLoader, () -> delegate.createView(session, viewName, definition, replace));
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        withClassLoader(classLoader, () -> delegate.renameView(session, source, target));
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        withClassLoader(classLoader, () -> delegate.setViewAuthorization(session, viewName, principal));
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        withClassLoader(classLoader, () -> delegate.dropView(session, viewName));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.listViews(session, schemaName));
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.getViews(session, schemaName));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return withClassLoader(classLoader, () -> delegate.getView(session, viewName));
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.getSchemaProperties(session, schemaName));
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.getSchemaOwner(session, schemaName));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return withClassLoader(classLoader, () -> delegate.getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns));
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.beginDelete(session, tableHandle));
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        withClassLoader(classLoader, () -> delegate.finishDelete(session, tableHandle, fragments));
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return withClassLoader(classLoader, () -> delegate.supportsMetadataDelete(session, tableHandle, tableLayoutHandle));
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return withClassLoader(classLoader, () -> delegate.metadataDelete(session, tableHandle, tableLayoutHandle));
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return withClassLoader(classLoader, () -> delegate.applyDelete(session, handle));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return withClassLoader(classLoader, () -> delegate.executeDelete(session, handle));
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return withClassLoader(classLoader, () -> delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain));
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        withClassLoader(classLoader, () -> delegate.createRole(session, role, grantor));
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        withClassLoader(classLoader, () -> delegate.dropRole(session, role));
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return withClassLoader(classLoader, () -> delegate.listRoles(session));
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        return withClassLoader(classLoader, () -> delegate.listAllRoleGrants(session, roles, grantees, limit));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return withClassLoader(classLoader, () -> delegate.listRoleGrants(session, principal));
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        withClassLoader(classLoader, () -> delegate.grantRoles(connectorSession, roles, grantees, adminOption, grantor));
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        withClassLoader(classLoader, () -> delegate.revokeRoles(connectorSession, roles, grantees, adminOption, grantor));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return withClassLoader(classLoader, () -> delegate.listApplicableRoles(session, principal));
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return withClassLoader(classLoader, () -> delegate.listEnabledRoles(session));
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        withClassLoader(classLoader, () -> delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption));
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        withClassLoader(classLoader, () -> delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption));
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        withClassLoader(classLoader, () -> delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption));
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        withClassLoader(classLoader, () -> delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption));
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return withClassLoader(classLoader, () -> delegate.listTablePrivileges(session, prefix));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return withClassLoader(classLoader, () -> delegate.usesLegacyTableLayouts());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return withClassLoader(classLoader, () -> delegate.getTableProperties(session, table));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        return withClassLoader(classLoader, () -> delegate.applyLimit(session, table, limit));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        return withClassLoader(classLoader, () -> delegate.applyFilter(session, table, constraint));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return withClassLoader(classLoader, () -> delegate.applyProjection(session, table, projections, assignments));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle table, SampleType sampleType, double sampleRatio)
    {
        return withClassLoader(classLoader, () -> delegate.applySample(session, table, sampleType, sampleRatio));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        return withClassLoader(classLoader, () -> delegate.applyAggregation(session, table, aggregates, assignments, groupingSets));
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
        return withClassLoader(classLoader, () -> delegate.applyJoin(session, joinType, left, right, joinConditions, leftAssignments, rightAssignments, statistics));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        return withClassLoader(classLoader, () -> delegate.applyTopN(session, table, topNCount, sortItems, assignments));
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        withClassLoader(classLoader, () -> delegate.validateScan(session, handle));
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        withClassLoader(classLoader, () -> delegate.createMaterializedView(session, viewName, definition, replace, ignoreExisting));
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        withClassLoader(classLoader, () -> delegate.dropMaterializedView(session, viewName));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.listMaterializedViews(session, schemaName));
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return withClassLoader(classLoader, () -> delegate.getMaterializedViews(session, schemaName));
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return withClassLoader(classLoader, () -> delegate.getMaterializedView(session, viewName));
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return withClassLoader(classLoader, () -> delegate.getMaterializedViewFreshness(session, name));
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return withClassLoader(classLoader, () -> delegate.applyTableScanRedirect(session, tableHandle));
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return withClassLoader(classLoader, () -> delegate.beginUpdate(session, tableHandle, updatedColumns));
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        withClassLoader(classLoader, () -> delegate.finishUpdate(session, tableHandle, fragments));
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return withClassLoader(classLoader, () -> delegate.redirectTable(session, tableName));
    }
}
