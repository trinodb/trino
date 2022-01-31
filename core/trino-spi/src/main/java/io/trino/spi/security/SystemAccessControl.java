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
package io.trino.spi.security;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantExecuteFunctionPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyKillQuery;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetUser;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowCurrentRoles;
import static io.trino.spi.security.AccessDeniedException.denyShowRoleAuthorizationDescriptors;
import static io.trino.spi.security.AccessDeniedException.denyShowRoleGrants;
import static io.trino.spi.security.AccessDeniedException.denyShowRoles;
import static io.trino.spi.security.AccessDeniedException.denyShowSchemas;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyViewQuery;
import static java.lang.String.format;
import static java.util.Collections.emptySet;

public interface SystemAccessControl
{
    /**
     * Check if the identity is allowed impersonate the specified user.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        denyImpersonateUser(context.getIdentity().getUser(), userName);
    }

    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated use user mapping and {@link #checkCanImpersonateUser} instead
     */
    @Deprecated
    default void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName);
    }

    /**
     * Checks if identity can execute a query.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanExecuteQuery(SystemSecurityContext context)
    {
        denyExecuteQuery();
    }

    /**
     * Checks if identity can view a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanViewQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        checkCanViewQueryOwnedBy(context, queryOwner.getUser());
    }

    /**
     * Checks if identity can view a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated Implement {@link #checkCanViewQueryOwnedBy(SystemSecurityContext, Identity)} instead.
     */
    @Deprecated
    default void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        denyViewQuery();
    }

    /**
     * Filter the list of users to those the identity view query owned by the user.  The method
     * will not be called with the current user in the set.
     */
    default Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        Set<String> ownerUsers = queryOwners.stream()
                .map(Identity::getUser)
                .collect(Collectors.toSet());
        Set<String> allowedUsers = filterViewQueryOwnedBy(context, ownerUsers);
        return queryOwners.stream()
                .filter(owner -> allowedUsers.contains(owner.getUser()))
                .collect(Collectors.toList());
    }

    /**
     * Filter the list of users to those the identity view query owned by the user.  The method
     * will not be called with the current user in the set.
     *
     * @deprecated Implement {@link #filterViewQueryOwnedBy(SystemSecurityContext, Collection)} instead.
     */
    @Deprecated
    default Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners)
    {
        return emptySet();
    }

    /**
     * Checks if identity can kill a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanKillQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        checkCanKillQueryOwnedBy(context, queryOwner.getUser());
    }

    /**
     * Checks if identity can kill a query owned by the specified user.  The method
     * will not be called when the current user is the query owner.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated Implement {@link #checkCanKillQueryOwnedBy(SystemSecurityContext, Identity)} instead.
     */
    @Deprecated
    default void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner)
    {
        denyKillQuery();
    }

    /**
     * Check if identity is allowed to read system information such as statistics,
     * service registry, thread stacks, etc.  This is typically allowed for administrators
     * and management tools.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        AccessDeniedException.denyReadSystemInformationAccess();
    }

    /**
     * Check if identity is allowed to write system information such as marking nodes
     * offline, or changing runtime flags.  This is typically allowed for administrators.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanWriteSystemInformation(SystemSecurityContext context)
    {
        AccessDeniedException.denyReadSystemInformationAccess();
    }

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        denySetSystemSessionProperty(propertyName);
    }

    /**
     * Check if identity is allowed to access the specified catalog
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        denyCatalogAccess(catalogName);
    }

    /**
     * Filter the list of catalogs to those visible to the identity.
     */
    default Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to create the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        denyCreateSchema(schema.toString());
    }

    /**
     * Check if identity is allowed to drop the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        denyDropSchema(schema.toString());
    }

    /**
     * Check if identity is allowed to rename the specified schema in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        denyRenameSchema(schema.toString(), newSchemaName);
    }

    /**
     * Check if identity is allowed to change the specified schema's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        denySetSchemaAuthorization(schema.toString(), principal);
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        denyShowSchemas();
    }

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    default Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to execute SHOW CREATE SCHEMA.
     *
     * @throws io.trino.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        denyShowCreateSchema(schemaName.toString());
    }

    /**
     * Check if identity is allowed to execute SHOW CREATE TABLE, SHOW CREATE VIEW or SHOW CREATE MATERIALIZED VIEW
     *
     * @throws io.trino.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyShowCreateTable(table.toString());
    }

    /**
     * Check if identity is allowed to create the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated use {@link #checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map properties)} instead
     */
    @Deprecated
    default void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyCreateTable(table.toString());
    }

    /**
     * Check if identity is allowed to create the specified table with properties in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        denyCreateTable(table.toString());
    }

    /**
     * Check if identity is allowed to drop the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyDropTable(table.toString());
    }

    /**
     * Check if identity is allowed to rename the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        denyRenameTable(table.toString(), newTable.toString());
    }

    /**
     * Check if identity is allowed to alter properties to the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        denySetTableProperties(table.toString());
    }

    /**
     * Check if identity is allowed to comment the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyCommentTable(table.toString());
    }

    /**
     * Check if identity is allowed to set comment to column in the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyCommentColumn(table.toString());
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        denyShowTables(schema.toString());
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    default Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws io.trino.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyShowColumns(table.toString());
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    default Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to add columns to the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyAddColumn(table.toString());
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyDropColumn(table.toString());
    }

    /**
     * Check if identity is allowed to change the specified table's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        denySetTableAuthorization(table.toString(), principal);
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyRenameColumn(table.toString());
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        denySelectColumns(table.toString(), columns);
    }

    /**
     * Check if identity is allowed to insert into the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyInsertTable(table.toString());
    }

    /**
     * Check if identity is allowed to delete from the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyDeleteTable(table.toString());
    }

    /**
     * Check if identity is allowed to truncate the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        denyTruncateTable(table.toString());
    }

    /**
     * Check if identity is allowed to update the supplied columns in the specified table in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        denyUpdateTableColumns(table.toString(), updatedColumnNames);
    }

    /**
     * Check if identity is allowed to create the specified view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        denyCreateView(view.toString());
    }

    /**
     * Check if identity is allowed to rename the specified view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        denyRenameTable(view.toString(), newView.toString());
    }

    /**
     * Check if identity is allowed to change the specified view's user/role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        denySetViewAuthorization(view.toString(), principal);
    }

    /**
     * Check if identity is allowed to drop the specified view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        denyDropView(view.toString());
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        denyCreateViewWithSelect(table.toString(), context.getIdentity());
    }

    /**
     * Check if identity is allowed to create the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     * @deprecated use {@link #checkCanCreateMaterializedView(SystemSecurityContext, CatalogSchemaTableName, Map<String, Object>)} instead
     */
    @Deprecated
    default void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        denyCreateMaterializedView(materializedView.toString());
    }

    /**
     * Check if identity is allowed to create the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        denyCreateMaterializedView(materializedView.toString());
    }

    /**
     * Check if identity is allowed to refresh the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        denyRefreshMaterializedView(materializedView.toString());
    }

    /**
     * Check if identity is allowed to set the properties of the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        denySetMaterializedViewProperties(materializedView.toString());
    }

    /**
     * Check if identity is allowed to drop the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        denyDropMaterializedView(materializedView.toString());
    }

    /**
     * Check if identity is allowed to rename the specified materialized view in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        denyRenameMaterializedView(view.toString(), newView.toString());
    }

    /**
     * Check if identity is allowed to grant an access to the function execution to grantee.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        String granteeAsString = format("%s '%s'", grantee.getType().name().toLowerCase(Locale.ENGLISH), grantee.getName());
        denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity(), granteeAsString);
    }

    /**
     * Check if identity is allowed to set the specified property in a catalog.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(propertyName);
    }

    /**
     * Check if identity is allowed to grant the specified privilege to the grantee on the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
    }

    /**
     * Check if identity is allowed to deny the specified privilege to the grantee on the specified schema.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        denyDenySchemaPrivilege(privilege.toString(), schema.toString());
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified schema from the revokee.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
    }

    /**
     * Check if identity is allowed to grant the specified privilege to the grantee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        denyGrantTablePrivilege(privilege.toString(), table.toString());
    }

    /**
     * Check if identity is allowed to deny the specified privilege to the grantee on the specified table.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        denyDenyTablePrivilege(privilege.toString(), table.toString());
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table from the revokee.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        denyRevokeTablePrivilege(privilege.toString(), table.toString());
    }

    /**
     * Check if identity is allowed to show roles.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowRoles(SystemSecurityContext context)
    {
        denyShowRoles();
    }

    /**
     * Check if identity is allowed to create the specified role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        denyCreateRole(role);
    }

    /**
     * Check if identity is allowed to drop the specified role.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanDropRole(SystemSecurityContext context, String role)
    {
        denyDropRole(role);
    }

    /**
     * Check if identity is allowed to grant the specified roles to the specified principals.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        denyGrantRoles(roles, grantees);
    }

    /**
     * Check if identity is allowed to revoke the specified roles from the specified principals.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        denyRevokeRoles(roles, grantees);
    }

    /**
     * Check if identity is allowed to show role authorization descriptors (i.e. RoleGrants).
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowRoleAuthorizationDescriptors(SystemSecurityContext context)
    {
        denyShowRoleAuthorizationDescriptors();
    }

    /**
     * Check if identity is allowed to show current roles.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        denyShowCurrentRoles();
    }

    /**
     * Check if identity is allowed to show its own role grants.
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        denyShowRoleGrants();
    }

    /**
     * Check if identity is allowed to execute the specified procedure
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        denyExecuteProcedure(procedure.toString());
    }

    /**
     * Check if identity is allowed to execute the specified function
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
        denyExecuteFunction(functionName);
    }

    /**
     * Check if identity is allowed to execute the specified table procedure on specified table
     *
     * @throws AccessDeniedException if not allowed
     */
    default void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        denyExecuteTableProcedure(table.toString(), procedure);
    }

    /**
     * Get a row filter associated with the given table and identity.
     * <p>
     * The filter must be a scalar SQL expression of boolean type over the columns in the table.
     *
     * @return the filter, or {@link Optional#empty()} if not applicable
     */
    default Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        return Optional.empty();
    }

    /**
     * Get a column mask associated with the given table, column and identity.
     * <p>
     * The mask must be a scalar SQL expression of a type coercible to the type of the column being masked. The expression
     * must be written in terms of columns in the table.
     *
     * @return the mask, or {@link Optional#empty()} if not applicable
     */
    default Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    /**
     * @return the event listeners provided by this system access control
     */
    default Iterable<EventListener> getEventListeners()
    {
        return emptySet();
    }
}
