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
package io.prestosql.spi.connector;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public interface ConnectorMetadata
{
    /**
     * Checks if a schema exists. The connector may have schemas that exist
     * but are not enumerable via {@link #listSchemaNames}.
     */
    default boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

    /**
     * Returns the schemas provided by this connector.
     */
    List<String> listSchemaNames(ConnectorSession session);

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    @Nullable
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     * The returned table handle can contain information in analyzeProperties.
     */
    @Nullable
    default ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support analyze");
    }

    /**
     * Returns the system table for the specified table name, if one exists.
     * The system tables handled via {@link #getSystemTable} differ form those returned by {@link Connector#getSystemTables()}.
     * The former mechanism allows dynamic resolution of system tables, while the latter is
     * based on static list of system tables built during startup.
     */
    default Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     */
    @Deprecated
    default List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (usesLegacyTableLayouts()) {
            throw new IllegalStateException("Connector uses legacy Table Layout but doesn't implement getTableLayouts()");
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Deprecated
    default ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        if (usesLegacyTableLayouts()) {
            throw new IllegalStateException("Connector uses legacy Table Layout but doesn't implement getTableLayout()");
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return a table layout handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table layout handle.
     * The provided table layout handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table layout handle,
     * as promised by {@link #getCommonPartitioningHandle}.
     */
    default ConnectorTableLayoutHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getCommonPartitioningHandle() is implemented without makeCompatiblePartitioning()");
    }

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    default Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        if (left.equals(right)) {
            return Optional.of(left);
        }
        return Optional.empty();
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);

    /**
     * Return the connector-specific metadata for the specified table layout. This is the object that is passed to the event listener framework.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    @Deprecated
    default Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        return Optional.empty();
    }

    default Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    /**
     * List table and view names, possibly filtered by schema. An empty list is returned if none match.
     */
    default List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return emptyList();
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix);

    /**
     * Get statistics for table for given filtering constraint.
     */
    default TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        return TableStatistics.empty();
    }

    /**
     * Creates a schema.
     */
    default void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    /**
     * Drops the specified schema.
     *
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    default void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    /**
     * Renames the specified schema.
     */
    default void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws PrestoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    default void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    default void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    /**
     * Rename the specified table
     */
    default void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    /**
     * Comments to the specified table
     */
    default void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setting table comments");
    }

    /**
     * Add the specified column
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    /**
     * Rename the specified column
     */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    /**
     * Drop the specified column
     */
    default void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    /**
     * Get the physical layout for a new table.
     */
    default Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    default Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableProperties properties = getTableProperties(session, tableHandle);
        return properties.getTablePartitioning()
                .map(partitioning -> {
                    Map<ColumnHandle, String> columnNamesByHandle = getColumnHandles(session, tableHandle).entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                    List<String> partitionColumns = partitioning.getPartitioningColumns().stream()
                            .map(columnNamesByHandle::get)
                            .collect(toList());

                    return new ConnectorNewTableLayout(partitioning.getPartitioningHandle(), partitionColumns);
                });
    }

    /**
     * Describes statistics that must be collected during a write.
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return TableStatisticsMetadata.empty();
    }

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandleForStatisticsCollection() is implemented without getStatisticsCollectionMetadata()");
    }

    /**
     * Begin statistics collection
     */
    default ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getStatisticsCollectionMetadata() is implemented without beginStatisticsCollection()");
    }

    /**
     * Finish statistics collection
     */
    default void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginStatisticsCollection() is implemented without finishStatisticsCollection()");
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    default ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    default Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginCreateTable() is implemented without finishCreateTable()");
    }

    /**
     * Start a SELECT/UPDATE/INSERT/DELETE query. This notification is triggered after the planning phase completes.
     */
    default void beginQuery(ConnectorSession session) {}

    /**
     * Cleanup after a SELECT/UPDATE/INSERT/DELETE query. This is the very last notification after the query finishes, whether it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    default void cleanupQuery(ConnectorSession session) {}

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    /**
     * Finish insert query
     */
    default Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginInsert() is implemented without finishInsert()");
    }

    /**
     * Get the column handle that will generate row IDs for the delete operation.
     * These IDs will be passed to the {@code deleteRows()} method of the
     * {@link io.prestosql.spi.connector.UpdatablePageSource} that created them.
     */
    default ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates or deletes");
    }

    /**
     * Begin delete query
     */
    default ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Finish delete query
     *
     * @param fragments all fragments returned by {@link io.prestosql.spi.connector.UpdatablePageSource#finish()}
     */
    default void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Create the specified view. The data for the view is opaque to the connector.
     */
    default void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    /**
     * Drop the specified view.
     */
    default void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }

    default List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return emptyList();
    }

    /**
     * Gets the view data for views that match the specified table prefix.
     */
    default Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    /**
     * @return whether delete without table scan is supported
     */
    default boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Delete the provided table layout
     *
     * @return number of rows deleted, or null for unknown
     */
    default OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    default Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return Optional.empty();
    }

    /**
     * Creates the specified role.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    default void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support create role");
    }

    /**
     * Drops the specified role.
     */
    default void dropRole(ConnectorSession session, String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support drop role");
    }

    /**
     * List available roles.
     */
    default Set<String> listRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List role grants for a given principal, not recursively.
     */
    default Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified roles to the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Revokes the specified roles from the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    default Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    default Set<String> listEnabledRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    default void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support grants");
    }

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    default void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support revokes");
    }

    /**
     * List the table privileges granted to the specified grantee for the tables that have the specified prefix considering the selected session role
     */
    default List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyList();
    }

    /**
     * Whether the connector uses the legacy Table Layout feature. If this method returns false,
     * connectors are required to implement the following methods:
     * <ul>
     * <li>{@link #getTableProperties(ConnectorSession session, ConnectorTableHandle table)}</li>
     * <li>{@link #getInfo(ConnectorTableHandle table)} </li>
     * <li>{@link ConnectorSplitManager#getSplits(ConnectorTransactionHandle, ConnectorSession, ConnectorTableHandle, ConnectorSplitManager.SplitSchedulingStrategy)}</li>
     * </ul>
     */
    default boolean usesLegacyTableLayouts()
    {
        return true;
    }

    default ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        if (!usesLegacyTableLayouts()) {
            throw new IllegalStateException("getTableProperties() must be implemented if usesLegacyTableLayouts is false");
        }

        List<ConnectorTableLayoutResult> layouts = getTableLayouts(session, table, Constraint.alwaysTrue(), Optional.empty());

        if (layouts.size() != 1) {
            throw new PrestoException(NOT_SUPPORTED, format("Connector must return a single layout for table %s, but got %s", table, layouts.size()));
        }

        return new ConnectorTableProperties(layouts.get(0).getTableLayout());
    }

    /**
     * Attempt to push down the provided limit into the table.
     * <p>
     * Connectors can indicate whether they don't support limit pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports limit pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     * <p>
     * If the connector could benefit from the information but can't guarantee that it will be able to produce
     * fewer rows than the provided limit, it should return a non-empty result containing a new handle for the
     * derived table and the "limit guaranteed" flag set to false.
     * <p>
     * If the connector can guarantee it will produce fewer rows than the provided limit, it should return a
     * non-empty result with the "limit guaranteed" flag set to true.
     */
    default Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        return Optional.empty();
    }

    /**
     * Attempt to push down the provided constraint into the table. This method is provided as replacement to
     * {@link ConnectorMetadata#getTableLayouts(ConnectorSession, ConnectorTableHandle, Constraint, Optional)} to ease
     * migration for the legacy API.
     * <p>
     * Connectors can indicate whether they don't support predicate pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     */
    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        return Optional.empty();
    }
}
