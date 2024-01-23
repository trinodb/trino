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
package io.trino.connector.alternatives;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.BigintIn;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.FilterDefinition;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.IntegerIn;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.IsNull;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.Ranges;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle.VarcharIn;
import io.trino.spi.Experimental;
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
import io.trino.spi.connector.ConstraintApplicationResult.Alternative;
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
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

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
import java.util.function.UnaryOperator;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class MockPlanAlternativeMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MockPlanAlternativeMetadata.class);
    private final ConnectorMetadata delegate;

    public MockPlanAlternativeMetadata(ConnectorMetadata delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return delegate.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return delegate.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return delegate.getTableHandle(session, tableName, startVersion, endVersion);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        return delegate.getTableHandleForExecute(session, getDelegate(tableHandle), procedureName, executeProperties, retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return delegate.getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> delegateResult = delegate.beginTableExecute(session, tableExecuteHandle, getDelegate(updatedSourceTableHandle));
        return new BeginTableExecuteResult<>(
                delegateResult.getTableExecuteHandle(),
                withDelegate(updatedSourceTableHandle, delegateResult.getSourceHandle()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        delegate.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        delegate.executeTableExecute(session, tableExecuteHandle);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        ConnectorTableHandle delegateTable = delegate.makeCompatiblePartitioning(session, getDelegate(tableHandle), partitioningHandle);
        return withDelegate(tableHandle, delegateTable);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        return delegate.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return delegate.getTableName(session, getDelegate(table));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getTableSchema(session, getDelegate(tableHandle));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getTableMetadata(session, getDelegate(tableHandle));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        return delegate.getInfo(getDelegate(tableHandle));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.listTables(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.getRelationTypes(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getColumnHandles(session, getDelegate(tableHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate.getColumnMetadata(session, getDelegate(tableHandle), columnHandle);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return delegate.streamRelationColumns(session, schemaName, relationFilter);
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return delegate.streamRelationComments(session, schemaName, relationFilter);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getTableStatistics(session, getDelegate(tableHandle));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        delegate.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        delegate.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        delegate.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        delegate.setSchemaAuthorization(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        delegate.createTable(session, tableMetadata, saveMode);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        delegate.dropTable(session, getDelegate(tableHandle));
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        delegate.truncateTable(session, getDelegate(tableHandle));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        delegate.renameTable(session, getDelegate(tableHandle), newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        delegate.setTableProperties(session, getDelegate(tableHandle), properties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        delegate.setTableComment(session, getDelegate(tableHandle), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        delegate.setViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        delegate.setViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        delegate.setMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        delegate.setColumnComment(session, getDelegate(tableHandle), column, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        delegate.addColumn(session, getDelegate(tableHandle), column);
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        delegate.addField(session, getDelegate(tableHandle), parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    @Experimental(eta = "2023-04-01")
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        delegate.setColumnType(session, getDelegate(tableHandle), column, type);
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        delegate.setFieldType(session, getDelegate(tableHandle), fieldPath, type);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        delegate.dropNotNullConstraint(session, getDelegate(tableHandle), column);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        delegate.setTableAuthorization(session, tableName, principal);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        delegate.renameColumn(session, getDelegate(tableHandle), source, target);
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        delegate.renameField(session, getDelegate(tableHandle), fieldPath, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        delegate.dropColumn(session, getDelegate(tableHandle), column);
    }

    @Override
    @Experimental(eta = "2023-05-01")
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        delegate.dropField(session, getDelegate(tableHandle), column, fieldPath);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        return delegate.getSupportedType(session, tableProperties, type);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getInsertLayout(session, getDelegate(tableHandle));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        ConnectorTableHandle delegateHandle = getDelegate(tableHandle);
        ConnectorAnalyzeMetadata delegateResult = delegate.getStatisticsCollectionMetadata(
                session,
                delegateHandle,
                analyzeProperties);

        return new ConnectorAnalyzeMetadata(
                withDelegate(tableHandle, delegateResult.getTableHandle()),
                delegateResult.getStatisticsMetadata());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableHandle delegateHandle = delegate.beginStatisticsCollection(session, getDelegate(tableHandle));
        return withDelegate(tableHandle, delegateHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishStatisticsCollection(session, getDelegate(tableHandle), computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        return delegate.beginCreateTable(session, tableMetadata, layout, retryMode, replace);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        delegate.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        delegate.cleanupQuery(session);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return delegate.beginInsert(session, getDelegate(tableHandle), columns, retryMode);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return delegate.supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return delegate.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return delegate.refreshMaterializedView(session, viewName);
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        return delegate.beginRefreshMaterializedView(session, getDelegate(tableHandle), sourceTableHandles, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<ConnectorTableHandle> sourceTableHandles, List<String> sourceTableFunctions)
    {
        return delegate.finishRefreshMaterializedView(session, getDelegate(tableHandle), insertHandle, fragments, computedStatistics, sourceTableHandles, sourceTableFunctions);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getRowChangeParadigm(session, getDelegate(tableHandle));
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getMergeRowIdColumnHandle(session, getDelegate(tableHandle));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getUpdateLayout(session, getDelegate(tableHandle));
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        return delegate.beginMerge(session, getDelegate(tableHandle), retryMode);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishMerge(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        delegate.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        delegate.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        delegate.setViewAuthorization(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        delegate.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return delegate.getView(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return delegate.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return delegate.getSchemaOwner(session, schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        Optional<ConnectorTableHandle> delegateResult = delegate.applyDelete(session, getDelegate(handle));
        return delegateResult.map(delegateHandle -> withDelegate(handle, delegateHandle));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.executeDelete(session, getDelegate(handle));
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate.resolveIndex(session, getDelegate(tableHandle), indexableColumns, outputColumns, tupleDomain);
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return delegate.listFunctions(session, schemaName);
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return delegate.getFunctions(session, name);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return delegate.getFunctionMetadata(session, functionId);
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return delegate.getAggregationFunctionMetadata(session, functionId);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return delegate.getFunctionDependencies(session, functionId, boundSignature);
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return delegate.listLanguageFunctions(session, schemaName);
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return delegate.getLanguageFunctions(session, name);
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return delegate.languageFunctionExists(session, name, signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        delegate.createLanguageFunction(session, name, function, replace);
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        delegate.dropLanguageFunction(session, name, signatureToken);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return delegate.roleExists(session, role);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        delegate.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        delegate.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return delegate.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return delegate.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        delegate.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        delegate.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return delegate.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return delegate.listEnabledRoles(session);
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate.denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        delegate.denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return delegate.listTablePrivileges(session, prefix);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return delegate.getTableProperties(session, getDelegate(table));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        return delegate.applyLimit(session, getDelegate(handle), limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> delegateResult = delegate.applyFilter(session, getDelegate(handle), constraint);

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.getDomains().isPresent()) {
            Map<ColumnHandle, Domain> domains = summary.getDomains().get();

            boolean retainOriginalPlan = delegateResult.isEmpty() || delegateResult.get().isRetainOriginalPlan();
            List<Alternative<ConnectorTableHandle>> alternatives = new ArrayList<>();
            if (!retainOriginalPlan) {
                alternatives.add(delegateResult.get().getAlternatives().get(0));
            }
            ConnectorTableHandle delegateHandle = alternatives.isEmpty() ? handle : alternatives.get(0).handle();

            domains.forEach((columnHandle, domain) -> {
                if (domain.getValues() instanceof EquatableValueSet) {
                    // Give up since getRanges() is not supported
                    return;
                }
                log.debug("domain : column %s, domain type: %s, isNullAllowed: %s, rangeCount %d, isSingleValue: %s,  isDiscreteSet: %s, getDiscreteSet: %s",
                        columnHandle,
                        domain.getType(),
                        domain.isNullAllowed(),
                        domain.getValues().getRanges().getRangeCount(),
                        domain.getValues().isSingleValue(),
                        domain.getValues().isDiscreteSet(),
                        domain.getValues().isDiscreteSet() ? domain.getValues().getDiscreteSet() : ImmutableList.of());
                if (!domain.isNullAllowed() && domain.getValues().isDiscreteSet()
                        && (domain.getType() instanceof VarcharType || domain.getType().equals(BIGINT) || domain.getType().equals(INTEGER))) {
                    FilterDefinition filterDefinition;
                    if (domain.getType() instanceof VarcharType) {
                        filterDefinition = new VarcharIn(domain.getType(), domain.getValues().getDiscreteSet());
                    }
                    else if (domain.getType().equals(BIGINT)) {
                        filterDefinition = new BigintIn(domain.getValues().getDiscreteSet());
                    }
                    else if (domain.getType().equals(INTEGER)) {
                        filterDefinition = new IntegerIn(domain.getValues().getDiscreteSet());
                    }
                    else {
                        throw new IllegalArgumentException(domain.getType() + " not supported");
                    }
                    alternatives.add(new Alternative<>(
                            new MockPlanAlternativeTableHandle(delegateHandle, columnHandle, filterDefinition),
                            TupleDomain.withColumnDomains(Maps.filterKeys(domains, key -> !key.equals(columnHandle))),
                            Optional.of(constraint.getExpression()),
                            false));
                }
                else if (domain.isOnlyNull()) {
                    alternatives.add(new Alternative<>(
                            new MockPlanAlternativeTableHandle(delegateHandle, columnHandle, new IsNull()),
                            TupleDomain.withColumnDomains(Maps.filterKeys(domains, key -> !key.equals(columnHandle))),
                            Optional.of(constraint.getExpression()),
                            false));
                }
                else if (!domain.getValues().isAll() && !domain.getValues().isNone()
                        && domain.getValues().getRanges().getRangeCount() > 0) {
                    alternatives.add(new Alternative<>(
                            new MockPlanAlternativeTableHandle(
                                    delegateHandle,
                                    columnHandle,
                                    new Ranges(SortedRangeSet.copyOf(domain.getType(), domain.getValues().getRanges().getOrderedRanges()), domain.isNullAllowed())),
                            TupleDomain.withColumnDomains(Maps.filterKeys(domains, key -> !key.equals(columnHandle))),
                            Optional.of(constraint.getExpression()),
                            false));
                }
            });
            int numberOfAlternatives = (retainOriginalPlan ? 1 : 0) + alternatives.size();
            if (numberOfAlternatives > 1) {
                log.debug("Created #%d alternatives for table %s, constraint %s, retainOriginalPlan %b, alternatives %s", numberOfAlternatives, handle, constraint, retainOriginalPlan, alternatives);
                return Optional.of(new ConstraintApplicationResult<>(retainOriginalPlan, alternatives));
            }
        }
        log.debug("Could not create multiple alternatives for table %s, constraint %s", handle, constraint);
        return delegateResult;
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> delegateResult = delegate.applyProjection(session, getDelegate(handle), projections, assignments);
        return delegateResult.map(result -> new ProjectionApplicationResult<>(
                withDelegate(handle, result.getHandle()),
                result.getProjections(),
                result.getAssignments(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        Optional<SampleApplicationResult<ConnectorTableHandle>> delegateResult = delegate.applySample(session, getDelegate(handle), sampleType, sampleRatio);
        return delegateResult.map(result -> new SampleApplicationResult<>(
                withDelegate(handle, result.getHandle()),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> delegateResult = delegate.applyAggregation(session, getDelegate(handle), aggregates, assignments, groupingSets);
        return delegateResult.map(result -> new AggregationApplicationResult<>(
                withDelegate(handle, result.getHandle()),
                result.getProjections(),
                result.getAssignments(),
                result.getGroupingColumnMapping(),
                result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        return delegate.applyJoin(session, joinType, getDelegate(left), getDelegate(right), joinCondition, leftAssignments, rightAssignments, statistics);
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return delegate.applyTopN(session, getDelegate(handle), topNCount, sortItems, assignments);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        return delegate.applyTableFunction(session, handle);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        delegate.validateScan(session, getDelegate(handle));
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        delegate.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return delegate.getMaterializedViews(session, schemaName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return delegate.getMaterializedViewProperties(session, viewName, materializedViewDefinition);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return delegate.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return delegate.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        delegate.renameMaterializedView(session, source, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        delegate.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.applyTableScanRedirect(session, getDelegate(tableHandle));
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.redirectTable(session, tableName);
    }

    @Override
    public OptionalInt getMaxWriterTasks(ConnectorSession session)
    {
        return delegate.getMaxWriterTasks(session);
    }

    @Override
    public boolean isColumnarTableScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.isColumnarTableScan(session, getDelegate(tableHandle));
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return delegate.getNewTableWriterScalingOptions(session, tableName, tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.getInsertWriterScalingOptions(session, getDelegate(tableHandle));
    }

    private ConnectorTableHandle getDelegate(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof MockPlanAlternativeTableHandle handle ? handle.delegate() : tableHandle;
    }

    private ConnectorTableHandle withDelegate(ConnectorTableHandle tableHandle, ConnectorTableHandle delegate)
    {
        if (tableHandle instanceof MockPlanAlternativeTableHandle handle) {
            return new MockPlanAlternativeTableHandle(delegate, handle.filterColumn(), handle.filterDefinition());
        }
        return delegate;
    }
}
