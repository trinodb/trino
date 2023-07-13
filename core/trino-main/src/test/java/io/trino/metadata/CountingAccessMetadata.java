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
package io.trino.metadata;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

public class CountingAccessMetadata
        implements Metadata
{
    public enum Method
    {
        GET_TABLE_STATISTICS,
    }

    private final Metadata delegate;
    private final ConcurrentHashMultiset<Method> methodInvocations = ConcurrentHashMultiset.create();

    public CountingAccessMetadata(Metadata delegate)
    {
        this.delegate = delegate;
    }

    public Multiset<Method> getMethodInvocations()
    {
        return ImmutableMultiset.copyOf(methodInvocations);
    }

    public void resetCounters()
    {
        methodInvocations.clear();
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
    {
        return delegate.getConnectorCapabilities(session, catalogHandle);
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        return delegate.catalogExists(session, catalogName);
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        return delegate.schemaExists(session, schema);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        return delegate.listSchemaNames(session, catalogName);
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        return delegate.getTableHandle(session, tableName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedureName, Map<String, Object> executeProperties)
    {
        return delegate.getTableHandleForExecute(session, tableHandle, procedureName, executeProperties);
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        return delegate.getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle handle, TableHandle updatedSourceTableHandle)
    {
        return delegate.beginTableExecute(session, handle, updatedSourceTableHandle);
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        delegate.finishTableExecute(session, handle, fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle handle)
    {
        delegate.executeTableExecute(session, handle);
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        return delegate.getTableProperties(session, handle);
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle)
    {
        return delegate.makeCompatiblePartitioning(session, table, partitioningHandle);
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        return delegate.getCommonPartitioning(session, left, right);
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        return delegate.getInfo(session, handle);
    }

    @Override
    public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
    {
        return delegate.getTableName(session, tableHandle);
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        return delegate.getTableSchema(session, tableHandle);
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        return delegate.getTableMetadata(session, tableHandle);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        methodInvocations.add(Method.GET_TABLE_STATISTICS);
        return delegate.getTableStatistics(session, tableHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTables(session, prefix);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        return delegate.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTableColumns(session, prefix);
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        delegate.createSchema(session, schema, properties, principal);
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        delegate.dropSchema(session, schema);
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        delegate.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        delegate.setSchemaAuthorization(session, source, principal);
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        delegate.createTable(session, catalogName, tableMetadata, ignoreExisting);
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName)
    {
        delegate.renameTable(session, tableHandle, currentTableName, newTableName);
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        delegate.setTableProperties(session, tableHandle, properties);
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        delegate.setTableComment(session, tableHandle, comment);
    }

    @Override
    public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
    {
        delegate.setViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        delegate.setViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        delegate.setColumnComment(session, tableHandle, column, comment);
    }

    @Override
    public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
    {
        delegate.setColumnType(session, tableHandle, column, type);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
    {
        delegate.renameColumn(session, tableHandle, table, source, target);
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
    {
        delegate.addColumn(session, tableHandle, table, column);
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        delegate.setTableAuthorization(session, table, principal);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
    {
        delegate.dropColumn(session, tableHandle, table, column);
    }

    @Override
    public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        delegate.dropField(session, tableHandle, column, fieldPath);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
    {
        delegate.dropTable(session, tableHandle, tableName);
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        delegate.truncateTable(session, tableHandle);
    }

    @Override
    public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getNewTableLayout(session, catalogName, tableMetadata);
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout)
    {
        return delegate.beginCreateTable(session, catalogName, tableMetadata, layout);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle target)
    {
        return delegate.getInsertLayout(session, target);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getStatisticsCollectionMetadataForWrite(session, catalogHandle, tableMetadata);
    }

    @Override
    public AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        return delegate.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        return delegate.beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public void cleanupQuery(Session session)
    {
        delegate.cleanupQuery(session);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        return delegate.beginInsert(session, tableHandle, columns);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        return delegate.supportsMissingColumnsOnInsert(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishInsert(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        return delegate.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return delegate.refreshMaterializedView(session, viewName);
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles)
    {
        return delegate.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(Session session, TableHandle tableHandle, InsertTableHandle insertTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<TableHandle> sourceTableHandles)
    {
        return delegate.finishRefreshMaterializedView(session, tableHandle, insertTableHandle, fragments, computedStatistics, sourceTableHandles);
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle)
    {
        return delegate.applyDelete(session, tableHandle);
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle tableHandle)
    {
        return delegate.executeDelete(session, tableHandle);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        return delegate.getRowChangeParadigm(session, tableHandle);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        return delegate.getMergeRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
    {
        return delegate.getUpdateLayout(session, tableHandle);
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle)
    {
        return delegate.beginMerge(session, tableHandle);
    }

    @Override
    public void finishMerge(Session session, MergeHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishMerge(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
    {
        return delegate.getCatalogHandle(session, catalogName);
    }

    @Override
    public List<CatalogInfo> listCatalogs(Session session)
    {
        return delegate.listCatalogs(session);
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listViews(session, prefix);
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.getViews(session, prefix);
    }

    @Override
    public boolean isView(Session session, QualifiedObjectName viewName)
    {
        return delegate.isView(session, viewName);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        return delegate.getView(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        return delegate.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
    {
        return delegate.getSchemaOwner(session, schemaName);
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
    {
        delegate.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        delegate.renameView(session, existingViewName, newViewName);
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        delegate.setViewAuthorization(session, view, principal);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropView(session, viewName);
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        return delegate.applyLimit(session, table, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        return delegate.applyFilter(session, table, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return delegate.applyProjection(session, table, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        return delegate.applySample(session, table, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<TableHandle>> applyAggregation(Session session, TableHandle table, List<AggregateFunction> aggregations, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return delegate.applyAggregation(session, table, aggregations, assignments, groupingSets);
    }

    @Override
    public Optional<JoinApplicationResult<TableHandle>> applyJoin(Session session, JoinType joinType, TableHandle left, TableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        return delegate.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
    }

    @Override
    public Optional<TopNApplicationResult<TableHandle>> applyTopN(Session session, TableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return delegate.applyTopN(session, handle, topNCount, sortItems, assignments);
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
    {
        return delegate.applyTableFunction(session, handle);
    }

    @Override
    public void validateScan(Session session, TableHandle table)
    {
        delegate.validateScan(session, table);
    }

    @Override
    public boolean isCatalogManagedSecurity(Session session, String catalog)
    {
        return delegate.isCatalogManagedSecurity(session, catalog);
    }

    @Override
    public boolean roleExists(Session session, String role, Optional<String> catalog)
    {
        return delegate.roleExists(session, role, catalog);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        delegate.createRole(session, role, grantor, catalog);
    }

    @Override
    public void dropRole(Session session, String role, Optional<String> catalog)
    {
        delegate.dropRole(session, role, catalog);
    }

    @Override
    public Set<String> listRoles(Session session, Optional<String> catalog)
    {
        return delegate.listRoles(session, catalog);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal)
    {
        return delegate.listRoleGrants(session, catalog, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        delegate.grantRoles(session, roles, grantees, adminOption, grantor, catalog);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        delegate.revokeRoles(session, roles, grantees, adminOption, grantor, catalog);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog)
    {
        return delegate.listApplicableRoles(session, principal, catalog);
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return delegate.listEnabledRoles(identity);
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        return delegate.listEnabledRoles(session, catalog);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate.denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate.denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listTablePrivileges(session, prefix);
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(Session session)
    {
        return delegate.listFunctions(session);
    }

    @Override
    public ResolvedFunction decodeFunction(QualifiedName name)
    {
        return delegate.decodeFunction(name);
    }

    @Override
    public ResolvedFunction resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return delegate.resolveFunction(session, name, parameterTypes);
    }

    @Override
    public ResolvedFunction resolveOperator(Session session, OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return delegate.resolveOperator(session, operatorType, argumentTypes);
    }

    @Override
    public ResolvedFunction getCoercion(Session session, Type fromType, Type toType)
    {
        return delegate.getCoercion(session, fromType, toType);
    }

    @Override
    public ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType)
    {
        return delegate.getCoercion(session, operatorType, fromType, toType);
    }

    @Override
    public ResolvedFunction getCoercion(Session session, QualifiedName name, Type fromType, Type toType)
    {
        return delegate.getCoercion(session, name, fromType, toType);
    }

    @Override
    public boolean isAggregationFunction(Session session, QualifiedName name)
    {
        return delegate.isAggregationFunction(session, name);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        return delegate.getFunctionMetadata(session, resolvedFunction);
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        return delegate.getAggregationFunctionMetadata(session, resolvedFunction);
    }

    @Override
    public void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        delegate.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.listMaterializedViews(session, prefix);
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        return delegate.getMaterializedViews(session, prefix);
    }

    @Override
    public boolean isMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return delegate.isMaterializedView(session, viewName);
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return delegate.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name)
    {
        return delegate.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        delegate.renameMaterializedView(session, existingViewName, newViewName);
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        delegate.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        return delegate.applyTableScanRedirect(session, tableHandle);
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName)
    {
        return delegate.getRedirectionAwareTableHandle(session, tableName);
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        return delegate.getRedirectionAwareTableHandle(session, tableName, startVersion, endVersion);
    }

    @Override
    public boolean supportsReportingWrittenBytes(Session session, TableHandle tableHandle)
    {
        return false;
    }

    @Override
    public boolean supportsReportingWrittenBytes(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
    {
        return false;
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        return delegate.getTableHandle(session, tableName, startVersion, endVersion);
    }

    @Override
    public OptionalInt getMaxWriterTasks(Session session, String catalogName)
    {
        return delegate.getMaxWriterTasks(session, catalogName);
    }
}
