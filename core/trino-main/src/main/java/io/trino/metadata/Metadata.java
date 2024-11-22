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

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
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
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
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
import io.trino.spi.function.LanguageFunction;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.trino.spi.function.OperatorType.CAST;

public interface Metadata
{
    Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle);

    boolean catalogExists(Session session, String catalogName);

    boolean schemaExists(Session session, CatalogSchemaName schema);

    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

    Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName);

    Optional<TableExecuteHandle> getTableHandleForExecute(
            Session session,
            TableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties);

    Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle);

    BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle handle, TableHandle updatedSourceTableHandle);

    void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState);

    void executeTableExecute(Session session, TableExecuteHandle handle);

    TableProperties getTableProperties(Session session, TableHandle handle);

    /**
     * Return a table handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table handle.
     * The provided table handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table handle,
     * as promised by {@link #getCommonPartitioning}.
     */
    TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle);

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right);

    Optional<Object> getInfo(Session session, TableHandle handle);

    CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle);

    /**
     * Return table schema definition for the specified table handle.
     * Table schema definition is a set of information
     * required by semantic analyzer to analyze the query.
     *
     * @throws RuntimeException if table handle is no longer valid
     * @see #getTableMetadata(Session, TableHandle)
     */
    TableSchema getTableSchema(Session session, TableHandle tableHandle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     * @see #getTableSchema(Session, TableHandle) a different method which is less expensive.
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Return statistics for specified table.
     */
    TableStatistics getTableStatistics(Session session, TableHandle tableHandle);

    /**
     * Get the relation names that match the specified table prefix (never null).
     * This includes all relations (e.g. tables, views, materialized views).
     */
    List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the relation names that match the specified table prefix (never null).
     * This includes all relations (e.g. tables, views, materialized views).
     */
    Map<SchemaTableName, RelationType> getRelationTypes(Session session, QualifiedTablePrefix prefix);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns cannot be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the columns metadata for all tables that match the specified prefix.
     * TODO: consider returning a stream for more efficient processing
     */
    List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter);

    /**
     * Gets the comments metadata for all relations (tables, views, materialized views) that match the specified prefix.
     * TODO: consider returning a stream for more efficient processing
     */
    List<RelationCommentMetadata> listRelationComments(Session session, String catalogName, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter);

    /**
     * Creates a schema.
     *
     * @param principal TODO
     */
    void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal);

    /**
     * Drops the specified schema.
     */
    void dropSchema(Session session, CatalogSchemaName schema, boolean cascade);

    /**
     * Renames the specified schema.
     */
    void renameSchema(Session session, CatalogSchemaName source, String target);

    /**
     * Set the specified schema's user/role.
     */
    void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal);

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws TrinoException with {@code ALREADY_EXISTS} if the table already exists and {@code saveMode} is set to FAIL.
     */
    void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode);

    /**
     * Rename the specified table.
     */
    void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName);

    /**
     * Set properties to the specified table.
     */
    void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties);

    /**
     * Comments to the specified table.
     */
    void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment);

    /**
     * Comments to the specified view.
     */
    void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment);

    /**
     * Comments to the specified view column.
     */
    void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment);

    /**
     * Comments to the specified column.
     */
    void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment);

    /**
     * Rename the specified column.
     */
    void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target);

    /**
     * Rename the specified field.
     */
    void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target);

    /**
     * Add the specified column to the table.
     */
    void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column);

    /**
     * Add the specified field to the column.
     */
    void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting);

    /**
     * Set the specified type to the column.
     */
    void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type);

    /**
     * Set the specified type to the field.
     */
    void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type);

    /**
     * Drop a not null constraint on the specified column.
     */
    void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle column);

    /**
     * Set the authorization (owner) of specified table's user/role
     */
    void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal);

    /**
     * Drop the specified column.
     */
    void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column);

    /**
     * Drop the specified field from the column.
     */
    void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table cannot be dropped or table handle is no longer valid
     */
    void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName);

    /**
     * Truncates the specified table
     */
    void truncateTable(Session session, TableHandle tableHandle);

    Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Return the effective {@link io.trino.spi.type.Type} that is supported by the connector for the given type, if {@link Optional#empty()} is returned, the type will be used as is during table creation which may or may not be supported by the connector.
     */
    Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace);

    /**
     * Finish a table creation with data after the data is written.
     */
    Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    Optional<TableLayout> getInsertLayout(Session session, TableHandle target);

    /**
     * Describes statistics that must be collected during a write.
     */
    TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata);

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties);

    /**
     * Begin statistics collection
     */
    AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle);

    /**
     * Finish statistics collection
     */
    void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics);

    /**
     * Initialize before query begins
     */
    void beginQuery(Session session);

    /**
     * Cleanup after a query. This is the very last notification after the query finishes, regardless if it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    void cleanupQuery(Session session);

    /**
     * Begin insert query
     */
    InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns);

    /**
     * @return whether connector handles missing columns during insert
     */
    boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle);

    /**
     * Finish insert query
     */
    Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Returns true if materialized view refresh should be delegated to connector
     */
    boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName);

    /**
     * Refresh materialized view
     */
    ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName);

    /**
     * Begin refresh materialized view query
     */
    InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles, RefreshType refreshType);

    /**
     * Finish refresh materialized view query
     */
    Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertTableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles,
            List<String> sourceTableFunctions);

    /**
     * Push update into connector
     */
    Optional<TableHandle> applyUpdate(Session session, TableHandle tableHandle, Map<ColumnHandle, Constant> assignments);

    /**
     * Execute update in connector
     */
    OptionalLong executeUpdate(Session session, TableHandle tableHandle);

    /**
     * Push delete into connector
     */
    Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle);

    /**
     * Execute delete in connector
     */
    OptionalLong executeDelete(Session session, TableHandle tableHandle);

    /**
     * Return the row update paradigm supported by the connector on the table or throw
     * an exception if row change is not supported.
     */
    RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle);

    /**
     * Get the column handle that will generate row IDs for the merge operation.
     * These IDs will be passed to the {@code storeMergedRows()} method of the
     * {@link io.trino.spi.connector.ConnectorMergeSink} that created them.
     */
    ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle);

    /**
     * Get the physical layout for updated or deleted rows of a MERGE operation.
     */
    Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle);

    /**
     * Begin merge query
     *
     * @param updateCaseColumnHandles The merge update case number to the assignment target columns mapping
     */
    MergeHandle beginMerge(Session session, TableHandle tableHandle, Multimap<Integer, ColumnHandle> updateCaseColumnHandles);

    /**
     * Finish merge query
     */
    void finishMerge(Session session, MergeHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Returns a catalog handle for the specified catalog name.
     */
    Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName);

    /**
     * Gets all the loaded catalogs
     */
    List<CatalogInfo> listCatalogs(Session session);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix);

    boolean isView(Session session, QualifiedObjectName viewName);

    /**
     * Returns the view definition for the specified view name.
     */
    Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName);

    /**
     * Returns the view definition for the specified view name.
     */
    Map<String, Object> getViewProperties(Session session, QualifiedObjectName viewName);

    /**
     * Gets the schema properties for the specified schema.
     */
    Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName);

    /**
     * Gets the schema owner for the specified schema.
     */
    Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName);

    /**
     * Creates the specified view with the specified view definition.
     */
    void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, Map<String, Object> properties, boolean replace);

    /**
     * Rename the specified view.
     */
    void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName);

    /**
     * Set the authorization (owner) of specified view's user/role
     */
    void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal);

    /**
     * Drops the specified view.
     */
    void dropView(Session session, QualifiedObjectName viewName);

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain);

    Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit);

    Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint);

    Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments);

    Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio);

    Optional<AggregationApplicationResult<TableHandle>> applyAggregation(
            Session session,
            TableHandle table,
            List<AggregateFunction> aggregations,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets);

    Optional<JoinApplicationResult<TableHandle>> applyJoin(
            Session session,
            JoinType joinType,
            TableHandle left,
            TableHandle right,
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics);

    Optional<TopNApplicationResult<TableHandle>> applyTopN(
            Session session,
            TableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments);

    Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle);

    default void validateScan(Session session, TableHandle table) {}

    //
    // Roles and Grants
    //

    /**
     * Does the specified catalog manage security directly, or does it use system security management?
     */
    boolean isCatalogManagedSecurity(Session session, String catalog);

    /**
     * Does the specified role exist.
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    boolean roleExists(Session session, String role, Optional<String> catalog);

    /**
     * Creates the specified role in the specified catalog.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog);

    /**
     * Drops the specified role in the specified catalog.
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    void dropRole(Session session, String role, Optional<String> catalog);

    /**
     * List available roles in specified catalog.
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    Set<String> listRoles(Session session, Optional<String> catalog);

    /**
     * List roles grants in the specified catalog for a given principal, not recursively.
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal);

    /**
     * Grants the specified roles to the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog);

    /**
     * Revokes the specified roles from the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog);

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog);

    /**
     * List applicable system roles, including the transitive grants, for the given identity.
     */
    Set<String> listEnabledRoles(Identity identity);

    /**
     * List applicable roles, including the transitive grants, in given catalog
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    Set<String> listEnabledRoles(Session session, String catalog);

    /**
     * Grants the specified privilege to the specified user on the specified schema.
     */
    void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Deny the specified privilege to the specified principal on the specified schema.
     */
    void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee);

    /**
     * Revokes the specified privilege on the specified schema from the specified user.
     */
    void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Deny the specified privilege to the specified principal on the specified table
     */
    void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee);

    /**
     * Revokes the specified privilege on the specified table from the specified user.
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Gets the privileges for the specified table available to the given grantee considering the selected session role
     */
    List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix);

    /**
     * Gets all the EntityPrivileges associated with an entityKind.  Defines ALL PRIVILEGES
     * for the entityKind
     */
    default Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Grants the specified privileges to the specified user on the specified grantee.
     * If the set of privileges is empty, it is interpreted as all privileges for the entityKind.
     */
    default void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Deny the specified privileges to the specified principal on the specified entity.
     * If the set of privileges is empty, it is interpreted as all privileges for the entityKind.
     */
    default void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Revokes the specified privilege on the specified entity from the specified grantee.
     * If the set of privileges is empty, it is interpreted as all privileges for the entityKind.
     */
    default void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    //
    // Functions
    //

    Collection<FunctionMetadata> listGlobalFunctions(Session session);

    Collection<FunctionMetadata> listFunctions(Session session, CatalogSchemaName schema);

    Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName catalogSchemaFunctionName);

    ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes);

    ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException;

    default ResolvedFunction getCoercion(Type fromType, Type toType)
    {
        return getCoercion(CAST, fromType, toType);
    }

    ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType);

    ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType);

    AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction);

    FunctionDependencyDeclaration getFunctionDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature);

    Collection<LanguageFunction> getLanguageFunctions(Session session, QualifiedObjectName name);

    boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken);

    void createLanguageFunction(Session session, QualifiedObjectName name, LanguageFunction function, boolean replace);

    void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken);

    /**
     * Creates the specified materialized view with the specified view definition.
     */
    void createMaterializedView(
            Session session,
            QualifiedObjectName viewName,
            MaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting);

    /**
     * Drops the specified materialized view.
     */
    void dropMaterializedView(Session session, QualifiedObjectName viewName);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the materialized view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Is the specified table a materialized view.
     */
    default boolean isMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return getMaterializedView(session, viewName).isPresent();
    }

    /**
     * Returns the materialized view definition for the specified view name.
     */
    Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName);

    Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName objectName, MaterializedViewDefinition materializedViewDefinition);

    /**
     * Method to get difference between the states of table at two different points in time/or as of given token-ids.
     * The method is used by the engine to determine if a materialized view is current with respect to the tables it depends on.
     */
    MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name);

    /**
     * Rename the specified materialized view.
     */
    void renameMaterializedView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName);

    /**
     * Sets the properties of the specified materialized view.
     */
    void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties);

    /**
     * Comments to the specified materialized view column.
     */
    void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment);

    /**
     * Returns the result of redirecting the table scan on a given table to a different table.
     * This method is used by the engine during the plan optimization phase to allow a connector to offload table scans to any other connector.
     * This method is called after security checks against the original table.
     */
    Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle);

    /**
     * Get the target table handle after performing redirection.
     */
    RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName);

    /**
     * Get the target table handle after performing redirection with a table version.
     */
    RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion);

    /**
     * Returns a table handle for the specified table name with a specified version
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion);

    /**
     * Returns maximum number of tasks that can be created while writing data to specific connector.
     * Note: It is ignored when retry policy is set to TASK
     */
    OptionalInt getMaxWriterTasks(Session session, String catalogName);

    /**
     * Workaround to lack of statistics about IO and CPU operations performed by the connector.
     * In the long term, this should be replaced by improvements in the cost model.
     *
     * @return true if the cumulative cost of splitting a read of the specified tableHandle into multiple reads,
     * each of which projects a subset of the required columns, is not significantly more than the cost of reading the specified tableHandle
     */
    boolean allowSplittingReadIntoMultipleSubQueries(Session session, TableHandle tableHandle);

    /**
     * Returns writer scaling options for the specified table. This method is called when table handle is not available during CTAS.
     */
    WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties);

    /**
     * Returns writer scaling options for the specified table.
     */
    WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle);
}
