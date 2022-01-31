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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;

public interface AccessControl
{
    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated replaced with user mapping during authentication and {@link #checkCanImpersonateUser}
     */
    @Deprecated
    void checkCanSetUser(Optional<Principal> principal, String userName);

    /**
     * Check if the identity is allowed impersonate the specified user.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanImpersonateUser(Identity identity, String userName);

    /**
     * Check if identity is allowed to read system information such as statistics,
     * service registry, thread stacks, etc.  This is typically allowed for administrators
     * and management tools.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanReadSystemInformation(Identity identity);

    /**
     * Check if identity is allowed to write system information such as marking nodes
     * offline, or changing runtime flags.  This is typically allowed for administrators.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanWriteSystemInformation(Identity identity);

    /**
     * Checks if identity can execute a query.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanExecuteQuery(Identity identity);

    /**
     * Checks if identity can view a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner);

    /**
     * Filter the list of users to those the identity view query owned by the user.  The method
     * will not be called with the current user in the set.
     */
    Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners);

    /**
     * Checks if identity can kill a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner);

    /**
     * Filter the list of catalogs to those visible to the identity.
     */
    Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs);

    /**
     * Check if identity is allowed to create the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to drop the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to rename the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName);

    /**
     * Check if identity is allowed to change the specified schema's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetSchemaAuthorization(SecurityContext context, CatalogSchemaName schemaName, TrinoPrincipal principal);

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowSchemas(SecurityContext context, String catalogName);

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames);

    /**
     * Check if identity is allowed to execute SHOW CREATE SCHEMA.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to execute SHOW CREATE TABLE, SHOW CREATE VIEW or SHOW CREATE MATERIALIZED VIEW
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified table.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated use {@link #checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map properties)}
     */
    @Deprecated
    void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified table with properties.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties);

    /**
     * Check if identity is allowed to drop the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName);

    /**
     * Check if identity is allowed to set properties to the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Optional<Object>> properties);

    /**
     * Check if identity is allowed to comment the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to comment the specified column.
     *
     * @throws io.trino.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to show tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog schema.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowTables(SecurityContext context, CatalogSchemaName schema);

    /**
     * Filter the list of tables, materialized views and views to those visible to the identity.
     */
    Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames);

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table);

    /**
     * Filter the list of columns to those visible to the identity.
     */
    Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, Set<String> columns);

    /**
     * Check if identity is allowed to add columns to the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop columns from the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to change the specified table's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetTableAuthorization(SecurityContext context, QualifiedObjectName tableName, TrinoPrincipal principal);

    /**
     * Check if identity is allowed to rename a column in the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to insert into the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to delete from the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to truncate the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to update the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames);

    /**
     * Check if identity is allowed to create the specified view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to rename the specified view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName);

    /**
     * Check if identity is allowed to change the specified view's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetViewAuthorization(SecurityContext context, QualifiedObjectName view, TrinoPrincipal principal)
    {
        denySetViewAuthorization(view.toString(), principal);
    }

    /**
     * Check if identity is allowed to drop the specified view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropView(SecurityContext context, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to create a view that selects from the specified columns.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to create the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated use {@link #checkCanCreateMaterializedView(SecurityContext, QualifiedObjectName, Map<String, Object>) instead}
     */
    @Deprecated
    void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName);

    /**
     * Check if identity is allowed to create the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties);

    /**
     * Check if identity is allowed to refresh the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName);

    /**
     * Check if identity is allowed to drop the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName);

    /**
     * Check if identity is allowed to rename the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName);

    /**
     * Check if identity is allowed to set the properties of the specified materialized view.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties);

    /**
     * Check if identity is allowed to create a view that executes the function.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption);

    /**
     * Check if identity is allowed to grant a privilege to the grantee on the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Check if identity is allowed to deny a privilege to the grantee on the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDenySchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee);

    /**
     * Check if identity is allowed to revoke a privilege from the revokee on the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption);

    /**
     * Check if identity is allowed to grant a privilege to the grantee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Check if identity is allowed to deny a privilege to the grantee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDenyTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee);

    /**
     * Check if identity is allowed to revoke a privilege from the revokee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption);

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, String propertyName);

    /**
     * Check if identity is allowed to set the specified catalog property.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName);

    /**
     * Check if identity is allowed to select from the specified columns.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to create the specified role.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalogName);

    /**
     * Check if identity is allowed to drop the specified role.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanDropRole(SecurityContext context, String role, Optional<String> catalogName);

    /**
     * Check if identity is allowed to grant the specified roles to the specified principals.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName);

    /**
     * Check if identity is allowed to revoke the specified roles from the specified principals.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanRevokeRoles(SecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor,
            Optional<String> catalogName);

    /**
     * Check if identity is allowed to set role for specified catalog.
     *
     * @param catalogName the role catalog
     * @throws AccessDeniedException if not allowed
     */
    void checkCanSetCatalogRole(SecurityContext context, String role, String catalogName);

    /**
     * Check if identity is allowed to show role authorization descriptors (i.e. RoleGrants).
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowRoleAuthorizationDescriptors(SecurityContext context, Optional<String> catalogName);

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowRoles(SecurityContext context, Optional<String> catalogName);

    /**
     * Check if identity is allowed to show current roles on the specified catalog.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowCurrentRoles(SecurityContext context, Optional<String> catalogName);

    /**
     * Check if identity is allowed to show its own role grants on the specified catalog.
     *
     * @param catalogName if present, the role catalog; otherwise the role is a system role
     * @throws AccessDeniedException if not allowed
     */
    void checkCanShowRoleGrants(SecurityContext context, Optional<String> catalogName);

    /**
     * Check if identity is allowed to execute procedure
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName);

    /**
     * Check if identity is allowed to execute function
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanExecuteFunction(SecurityContext context, String functionName);

    /**
     * Check if identity is allowed to execute given table procedure on given table
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanExecuteTableProcedure(SecurityContext context, QualifiedObjectName tableName, String procedureName);

    default List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        return ImmutableList.of();
    }

    default List<ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        return ImmutableList.of();
    }
}
