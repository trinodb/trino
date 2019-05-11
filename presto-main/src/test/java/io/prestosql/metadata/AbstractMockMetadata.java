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
package io.prestosql.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.prestosql.metadata.FunctionId.toFunctionId;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

public abstract class AbstractMockMetadata
        implements Metadata
{
    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

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
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableLayoutResult> getLayout(Session session, TableHandle tableHandle, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
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
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint)
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
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
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
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
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
    public void dropTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
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
    public void beginQuery(Session session, Multimap<CatalogName, ConnectorTableHandle> connectors)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupQuery(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
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
    public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, CatalogName> getCatalogNames(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ConnectorViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace)
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
    public boolean usesLegacyTableLayouts(Session session, TableHandle table)
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
    public Optional<TableHandle> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        return Optional.empty();
    }

    //
    // Roles and Grants
    //

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    //
    // Types
    //

    @Override
    public Type getType(TypeSignature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type fromSqlType(String sqlType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(TypeId id)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Type> getTypes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        throw new UnsupportedOperationException();
    }

    //
    // Functions
    //

    @Override
    public void addFunctions(List<? extends SqlFunction> functions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<FunctionMetadata> listFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedFunction resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        String nameSuffix = name.getSuffix();
        if (nameSuffix.equals("rand") && parameterTypes.isEmpty()) {
            Signature boundSignature = new Signature(nameSuffix, FunctionKind.SCALAR, DOUBLE.getTypeSignature(), ImmutableList.of());
            return new ResolvedFunction(boundSignature, toFunctionId(boundSignature));
        }
        throw new PrestoException(FUNCTION_NOT_FOUND, name + "(" + Joiner.on(", ").join(parameterTypes) + ")");
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
    public ResolvedFunction getCoercion(QualifiedName name, Type fromType, Type toType)
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
        Signature signature = resolvedFunction.getSignature();
        if (signature.getName().equals("rand") && signature.getArgumentTypes().isEmpty()) {
            return new FunctionMetadata(signature, false, false, "");
        }
        throw new PrestoException(FUNCTION_NOT_FOUND, signature.toString());
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionImplementation(ResolvedFunction resolvedFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregationFunction getAggregateFunctionImplementation(ResolvedFunction resolvedFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction resolvedFunction)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        throw new UnsupportedOperationException();
    }

    //
    // Blocks
    //

    @Override
    public BlockEncoding getBlockEncoding(String encodingName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        throw new UnsupportedOperationException();
    }

    //
    // Properties
    //

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }
}
