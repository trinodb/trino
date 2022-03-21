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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.connector.CatalogName;
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

import static io.trino.spi.function.OperatorType.CAST;

public interface Metadata
{
    Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName);

    boolean catalogExists(Session session, String catalogName);

    boolean schemaExists(Session session, CatalogSchemaName schema);

    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

    Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName);

    Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties);

    Optional<TableExecuteHandle> getTableHandleForExecute(
            Session session,
            TableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties);

    Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle);

    BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle handle, TableHandle updatedSourceTableHandle);

    void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState);

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

    /**
     * Return table schema definition for the specified table handle.
     * Table schema definition is a set of information
     * required by semantic analyzer to analyze the query.
     *
     * @throws RuntimeException if table handle is no longer valid
     * @see {@link #getTableMetadata(Session, TableHandle)}
     */
    TableSchema getTableSchema(Session session, TableHandle tableHandle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     * @see {@link #getTableSchema(Session, TableHandle)} which is less expensive.
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Return statistics for specified table for given filtering contraint.
     */
    TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint);

    /**
     * Get the relation names that match the specified table prefix (never null).
     * This includes all relations (e.g. tables, views, materialized views).
     */
    List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix);

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
    List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates a schema.
     *
     * @param principal TODO
     */
    void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal);

    /**
     * Drops the specified schema.
     */
    void dropSchema(Session session, CatalogSchemaName schema);

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
     * @throws TrinoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting);

    /**
     * Rename the specified table.
     */
    void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName);

    /**
     * Set properties to the specified table.
     */
    void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties);

    /**
     * Comments to the specified table.
     */
    void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment);

    /**
     * Comments to the specified column.
     */
    void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment);

    /**
     * Rename the specified column.
     */
    void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target);

    /**
     * Add the specified column to the table.
     */
    void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column);

    /**
     * Set the authorization (owner) of specified table's user/role
     */
    void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal);

    /**
     * Drop the specified column.
     */
    void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table cannot be dropped or table handle is no longer valid
     */
    void dropTable(Session session, TableHandle tableHandle);

    /**
     * Truncates the specified table
     */
    void truncateTable(Session session, TableHandle tableHandle);

    Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout);

    /**
     * Finish a table creation with data after the data is written.
     */
    Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    Optional<TableLayout> getInsertLayout(Session session, TableHandle target);

    /**
     * Describes statistics that must be collected during a write.
     */
    TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin statistics collection
     */
    AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle);

    /**
     * Finish statistics collection
     */
    void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics);

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
    Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

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
    InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles);

    /**
     * Finish refresh materialized view query
     */
    Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertTableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles);

    /**
     * Get the row ID column handle used with UpdatablePageSource#deleteRows.
     */
    ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle);

    /**
     * Get the row ID column handle used with UpdatablePageSource#updateRows.
     */
    ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns);

    /**
     * Push delete into connector
     */
    Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle);

    /**
     * Execute delete in connector
     */
    OptionalLong executeDelete(Session session, TableHandle tableHandle);

    /**
     * Begin delete query
     */
    TableHandle beginDelete(Session session, TableHandle tableHandle);

    /**
     * Finish delete query
     */
    void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Begin update query
     */
    TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns);

    /**
     * Finish update query
     */
    void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Returns a connector id for the specified catalog name.
     */
    Optional<CatalogName> getCatalogHandle(Session session, String catalogName);

    /**
     * Gets all the loaded catalogs
     *
     * @return Map of catalog name to connector
     */
    Map<String, Catalog> getCatalogs(Session session);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Is the specified table a view.
     */
    default boolean isView(Session session, QualifiedObjectName viewName)
    {
        return getView(session, viewName).isPresent();
    }

    /**
     * Returns the view definition for the specified view name.
     */
    Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName);

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
    void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace);

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

    Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint, Set<ColumnHandle> remainingPredicateColumns);

    Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments);

    Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio);

    /**
     * If the query has a filter, push the final (after prune) projected columns into connector
     */
    Optional<TableHandle> applyProjectedColumns(Session session, TableHandle table, Set<ColumnHandle> columns);

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
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics);

    Optional<TopNApplicationResult<TableHandle>> applyTopN(
            Session session,
            TableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments);

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
     * List all role grants in the specified catalog,
     * optionally filtered by passed role, grantee, and limit predicates.
     *
     * @param catalog if present, the role catalog; otherwise the role is a system role
     */
    Set<RoleGrant> listAllRoleGrants(Session session, Optional<String> catalog, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit);

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
     * Revokes the specified privilege on the specified table from the specified user
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Gets the privileges for the specified table available to the given grantee considering the selected session role
     */
    List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix);

    //
    // Functions
    //

    Collection<FunctionMetadata> listFunctions();

    ResolvedFunction decodeFunction(QualifiedName name);

    ResolvedFunction resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes);

    ResolvedFunction resolveOperator(Session session, OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException;

    default ResolvedFunction getCoercion(Session session, Type fromType, Type toType)
    {
        return getCoercion(session, CAST, fromType, toType);
    }

    ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType);

    ResolvedFunction getCoercion(Session session, QualifiedName name, Type fromType, Type toType);

    /**
     * Is the named function an aggregation function?  This does not need type parameters
     * because overloads between aggregation and other function types are not allowed.
     */
    boolean isAggregationFunction(QualifiedName name);

    FunctionMetadata getFunctionMetadata(ResolvedFunction resolvedFunction);

    AggregationFunctionMetadata getAggregationFunctionMetadata(ResolvedFunction resolvedFunction);

    /**
     * Creates the specified materialized view with the specified view definition.
     */
    void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting);

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
     * Verifies that a version is valid for a given table
     */
    boolean isValidTableVersion(Session session, QualifiedObjectName tableName, TableVersion version);

    /**
     * Returns a table handle for the specified table name with a specified version
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion);
}
