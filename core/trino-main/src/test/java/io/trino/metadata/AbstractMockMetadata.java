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
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
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
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.function.FunctionDependencyDeclaration.NO_DEPENDENCIES;
import static io.trino.spi.function.FunctionId.toFunctionId;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.type.DoubleType.DOUBLE;

public abstract class AbstractMockMetadata
        implements Metadata
{
    private static final CatalogSchemaFunctionName RAND_NAME = builtinFunctionName("rand");

    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
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
    public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
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
    public Map<SchemaTableName, RelationType> getRelationTypes(Session session, QualifiedTablePrefix prefix)
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
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RelationCommentMetadata> listRelationComments(Session session, String catalogName, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema, boolean cascade)
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
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName)
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
    public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
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
    public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace)
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
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties)
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
    public void beginQuery(Session session) {}

    @Override
    public void cleanupQuery(Session session) {}

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
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
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
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles, RefreshType refreshType)
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
            List<TableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> applyUpdate(Session session, TableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong executeUpdate(Session session, TableHandle tableHandle)
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
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle, Multimap<Integer, ColumnHandle> updateCaseColumnHandles)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishMerge(Session session, MergeHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogInfo> listCatalogs(Session session)
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
    public boolean isView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getViewProperties(Session session, QualifiedObjectName viewName)
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
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
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
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
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
    public Collection<FunctionMetadata> listGlobalFunctions(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        if (name.equals("rand") && parameterTypes.isEmpty()) {
            BoundSignature boundSignature = new BoundSignature(builtinFunctionName(name), DOUBLE, ImmutableList.of());
            return new ResolvedFunction(
                    boundSignature,
                    GlobalSystemConnector.CATALOG_HANDLE,
                    toFunctionId(name, boundSignature.toSignature()),
                    SCALAR,
                    true,
                    new FunctionNullability(false, ImmutableList.of()),
                    ImmutableMap.of(),
                    ImmutableSet.of());
        }
        throw new TrinoException(FUNCTION_NOT_FOUND, name + "(" + Joiner.on(", ").join(parameterTypes) + ")");
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName catalogSchemaFunctionName)
    {
        if (!catalogSchemaFunctionName.equals(RAND_NAME)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(new CatalogFunctionMetadata(
                GlobalSystemConnector.CATALOG_HANDLE,
                BUILTIN_SCHEMA,
                FunctionMetadata.scalarBuilder("random")
                        .signature(Signature.builder().returnType(DOUBLE).build())
                        .alias(RAND_NAME.getFunctionName())
                        .nondeterministic()
                        .noDescription()
                        .build()));
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature)
    {
        return NO_DEPENDENCIES;
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(Session session, QualifiedObjectName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createLanguageFunction(Session session, QualifiedObjectName name, LanguageFunction function, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken)
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
    public void createMaterializedView(
            Session session,
            QualifiedObjectName viewName,
            MaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
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
    public Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName viewName, MaterializedViewDefinition materializedViewDefinition)
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
    public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
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
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalInt getMaxWriterTasks(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }
}
