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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
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
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
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
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.metadata.FunctionId.toFunctionId;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public abstract class AbstractMockMetadata
        implements Metadata
{
    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

    private final ResolvedFunctionDecoder functionDecoder = new ResolvedFunctionDecoder(TESTING_TYPE_MANAGER::getType);

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedureName, Map<String, Object> executeProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle tableExecuteHandle, TableHandle updatedSourceTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupQuery(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Catalog> getCatalogs(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        return Optional.empty();
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        return Optional.empty();
    }

    @Override
    public Optional<AggregationApplicationResult<TableHandle>> applyAggregation(
            Session session,
            TableHandle table,
            List<AggregateFunction> aggregations,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        return Optional.empty();
    }

    @Override
    public Optional<JoinApplicationResult<TableHandle>> applyJoin(
            Session session,
            JoinType joinType,
            TableHandle left,
            TableHandle right,
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        return Optional.empty();
    }

    //
    // Roles and Grants
    //

    @Override
    public boolean isCatalogManagedSecurity(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean roleExists(Session session, String role, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(Session session, String role, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles(Session session, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(Session session, Optional<String> catalog, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    //
    // Functions
    //

    @Override
    public Collection<FunctionMetadata> listFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction decodeFunction(QualifiedName name)
    {
        return functionDecoder.fromQualifiedName(name)
                .orElseThrow(() -> new IllegalArgumentException("Function is not resolved: " + name));
    }

    @Override
    public ResolvedFunction resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        String nameSuffix = name.getSuffix();
        if (nameSuffix.equals("rand") && parameterTypes.isEmpty()) {
            BoundSignature boundSignature = new BoundSignature(nameSuffix, DOUBLE, ImmutableList.of());
            return new ResolvedFunction(
                    boundSignature,
                    toFunctionId(boundSignature.toSignature()),
                    SCALAR,
                    true,
                    new FunctionNullability(false, ImmutableList.of()),
                    ImmutableMap.of(),
                    ImmutableSet.of());
        }
        throw new TrinoException(FUNCTION_NOT_FOUND, name + "(" + Joiner.on(", ").join(parameterTypes) + ")");
    }

    @Override
    public ResolvedFunction resolveOperator(Session session, OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction getCoercion(Session session, QualifiedName name, Type fromType, Type toType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        BoundSignature signature = resolvedFunction.getSignature();
        if (signature.getName().equals("rand") && signature.getArgumentTypes().isEmpty()) {
            return new FunctionMetadata(signature.toSignature(), new FunctionNullability(false, ImmutableList.of()), false, false, "", SCALAR);
        }
        throw new TrinoException(FUNCTION_NOT_FOUND, signature.toString());
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TopNApplicationResult<TableHandle>> applyTopN(Session session, TableHandle handle, long topNFunctions, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName)
    {
        return noRedirection(getTableHandle(session, tableName));
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValidTableVersion(Session session, QualifiedObjectName tableName, TableVersion version)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        throw new UnsupportedOperationException();
    }
}
