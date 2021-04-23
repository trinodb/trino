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

import io.trino.spi.TrinoException;

import java.security.Principal;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;

public class AccessDeniedException
        extends TrinoException
{
    public static final String PREFIX = "Access Denied: ";

    public AccessDeniedException(String message)
    {
        super(PERMISSION_DENIED, PREFIX + message);
    }

    public AccessDeniedException(String message, AccessDeniedException e)
    {
        super(PERMISSION_DENIED, PREFIX + message, e);
    }

    public static void denyImpersonateUser(String originalUser, String newUser)
    {
        denyImpersonateUser(originalUser, newUser, null);
    }

    public static void denyImpersonateUser(String originalUser, String newUser, String extraInfo)
    {
        throw new AccessDeniedException(format("User %s cannot impersonate user %s%s", originalUser, newUser, formatExtraInfo(extraInfo)));
    }

    public static void denySetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName, null);
    }

    public static void denySetUser(Optional<Principal> principal, String userName, String extraInfo)
    {
        throw new AccessDeniedException(format("Principal %s cannot become user %s%s", principal.orElse(null), userName, formatExtraInfo(extraInfo)));
    }

    public static void denyReadSystemInformationAccess()
    {
        denyReadSystemInformationAccess(null);
    }

    public static void denyReadSystemInformationAccess(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot read system information%s", formatExtraInfo(extraInfo)));
    }

    public static void denyWriteSystemInformationAccess()
    {
        denyWriteSystemInformationAccess(null);
    }

    public static void denyWriteSystemInformationAccess(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot write system information%s", formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteQuery()
    {
        denyExecuteQuery(null);
    }

    public static void denyExecuteQuery(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot execute query%s", formatExtraInfo(extraInfo)));
    }

    public static void denyViewQuery()
    {
        denyViewQuery(null);
    }

    public static void denyViewQuery(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot view query%s", formatExtraInfo(extraInfo)));
    }

    public static void denyKillQuery()
    {
        denyKillQuery(null);
    }

    public static void denyKillQuery(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot kill query%s", formatExtraInfo(extraInfo)));
    }

    public static void denyCatalogAccess(String catalogName)
    {
        denyCatalogAccess(catalogName, null);
    }

    public static void denyCatalogAccess(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot access catalog %s%s", catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateSchema(String schemaName)
    {
        denyCreateSchema(schemaName, null);
    }

    public static void denyCreateSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropSchema(String schemaName)
    {
        denyDropSchema(schemaName, null);
    }

    public static void denyDropSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName, newSchemaName, null);
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename schema from %s to %s%s", schemaName, newSchemaName, formatExtraInfo(extraInfo)));
    }

    public static void denySetSchemaAuthorization(String schemaName, TrinoPrincipal principal)
    {
        throw new AccessDeniedException(format("Cannot set authorization for schema %s to %s", schemaName, principal));
    }

    public static void denyShowSchemas()
    {
        denyShowSchemas(null);
    }

    public static void denyShowSchemas(String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show schemas%s", formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateSchema(String schemaName)
    {
        throw new AccessDeniedException(format("Cannot show create schema for %s", schemaName));
    }

    public static void denyShowCreateTable(String tableName)
    {
        denyShowCreateTable(tableName, null);
    }

    public static void denyShowCreateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show create table for %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateTable(String tableName)
    {
        denyCreateTable(tableName, null);
    }

    public static void denyCreateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropTable(String tableName)
    {
        denyDropTable(tableName, null);
    }

    public static void denyDropTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameTable(String tableName, String newTableName)
    {
        denyRenameTable(tableName, newTableName, null);
    }

    public static void denyRenameTable(String tableName, String newTableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename table from %s to %s%s", tableName, newTableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentTable(String tableName)
    {
        denyCommentTable(tableName, null);
    }

    public static void denyCommentTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot comment table to %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentColumn(String tableName)
    {
        denyCommentColumn(tableName, null);
    }

    public static void denyCommentColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot comment column to %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowTables(String schemaName)
    {
        denyShowTables(schemaName, null);
    }

    public static void denyShowTables(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show tables of schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowColumns(String tableName)
    {
        throw new AccessDeniedException(format("Cannot show columns of table %s", tableName));
    }

    public static void denyAddColumn(String tableName)
    {
        denyAddColumn(tableName, null);
    }

    public static void denyAddColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot add a column to table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropColumn(String tableName)
    {
        denyDropColumn(tableName, null);
    }

    public static void denyDropColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop a column from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySetTableAuthorization(String tableName, TrinoPrincipal principal)
    {
        throw new AccessDeniedException(format("Cannot set authorization for table %s to %s", tableName, principal));
    }

    public static void denyRenameColumn(String tableName)
    {
        denyRenameColumn(tableName, null);
    }

    public static void denyRenameColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename a column in table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectTable(String tableName)
    {
        denySelectTable(tableName, null);
    }

    public static void denySelectTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot select from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyInsertTable(String tableName)
    {
        denyInsertTable(tableName, null);
    }

    public static void denyInsertTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot insert into table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDeleteTable(String tableName)
    {
        denyDeleteTable(tableName, null);
    }

    public static void denyDeleteTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot delete from table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyUpdateTableColumns(String tableName, Set<String> updatedColumnNames)
    {
        denyUpdateTableColumns(tableName, updatedColumnNames, null);
    }

    public static void denyUpdateTableColumns(String tableName, Set<String> updatedColumnNames, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot update columns %s in table %s%s", updatedColumnNames, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateView(String viewName)
    {
        denyCreateView(viewName, null);
    }

    public static void denyCreateView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateViewWithSelect(String sourceName, Identity identity)
    {
        denyCreateViewWithSelect(sourceName, identity.toConnectorIdentity());
    }

    public static void denyCreateViewWithSelect(String sourceName, ConnectorIdentity identity)
    {
        denyCreateViewWithSelect(sourceName, identity, null);
    }

    public static void denyCreateViewWithSelect(String sourceName, ConnectorIdentity identity, String extraInfo)
    {
        throw new AccessDeniedException(format("View owner '%s' cannot create view that selects from %s%s", identity.getUser(), sourceName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantExecuteFunctionPrivilege(String functionName, Identity identity, Identity grantee)
    {
        denyGrantExecuteFunctionPrivilege(functionName, identity, format("user '%s'", grantee.getUser()));
    }

    public static void denyGrantExecuteFunctionPrivilege(String functionName, Identity identity, String grantee)
    {
        throw new AccessDeniedException(format("'%s' cannot grant '%s' execution to %s", identity.getUser(), functionName, grantee));
    }

    public static void denyRenameView(String viewName, String newViewName)
    {
        denyRenameView(viewName, newViewName, null);
    }

    public static void denyRenameView(String viewName, String newViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename view from %s to %s%s", viewName, newViewName, formatExtraInfo(extraInfo)));
    }

    public static void denySetViewAuthorization(String tableName, TrinoPrincipal principal)
    {
        throw new AccessDeniedException(format("Cannot set authorization for view %s to %s", tableName, principal));
    }

    public static void denyDropView(String viewName)
    {
        denyDropView(viewName, null);
    }

    public static void denyDropView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectView(String viewName)
    {
        denySelectView(viewName, null);
    }

    public static void denySelectView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot select from view %s%s", viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateMaterializedView(String materializedViewName)
    {
        denyCreateMaterializedView(materializedViewName, null);
    }

    public static void denyCreateMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create materialized view %s%s", materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyRefreshMaterializedView(String materializedViewName)
    {
        denyRefreshMaterializedView(materializedViewName, null);
    }

    public static void denyRefreshMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot refresh materialized view %s%s", materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropMaterializedView(String materializedViewName)
    {
        denyDropMaterializedView(materializedViewName, null);
    }

    public static void denyDropMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop materialized view %s%s", materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName)
    {
        denyGrantSchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot grant privilege %s on schema %s%s", privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeSchemaPrivilege(String privilege, String schemaName)
    {
        denyRevokeSchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyRevokeSchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot revoke privilege %s on schema %s%s", privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName)
    {
        denyGrantTablePrivilege(privilege, tableName, null);
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot grant privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName)
    {
        denyRevokeTablePrivilege(privilege, tableName, null);
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot revoke privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowRoles(String catalogName)
    {
        throw new AccessDeniedException(format("Cannot show roles from catalog %s", catalogName));
    }

    public static void denyShowRoleAuthorizationDescriptors(String catalogName)
    {
        throw new AccessDeniedException(format("Cannot show role authorizatin descriptors from catalog %s", catalogName));
    }

    public static void denyShowCurrentRoles(String catalogName)
    {
        throw new AccessDeniedException(format("Cannot show current roles from catalog %s", catalogName));
    }

    public static void denyShowRoleGrants(String catalogName)
    {
        throw new AccessDeniedException(format("Cannot show role grants from catalog %s", catalogName));
    }

    public static void denySetSystemSessionProperty(String propertyName)
    {
        denySetSystemSessionProperty(propertyName, null);
    }

    public static void denySetSystemSessionProperty(String propertyName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set system session property %s%s", propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName, null);
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set catalog session property %s.%s%s", catalogName, propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String propertyName)
    {
        throw new AccessDeniedException(format("Cannot set catalog session property %s", propertyName));
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames)
    {
        denySelectColumns(tableName, columnNames, null);
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot select from columns %s in table or view %s%s", columnNames, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateRole(String roleName)
    {
        throw new AccessDeniedException(format("Cannot create role %s", roleName));
    }

    public static void denyDropRole(String roleName)
    {
        throw new AccessDeniedException(format("Cannot drop role %s", roleName));
    }

    public static void denyGrantRoles(Set<String> roles, Set<TrinoPrincipal> grantees)
    {
        throw new AccessDeniedException(format("Cannot grant roles %s to %s ", roles, grantees));
    }

    public static void denyRevokeRoles(Set<String> roles, Set<TrinoPrincipal> grantees)
    {
        throw new AccessDeniedException(format("Cannot revoke roles %s from %s ", roles, grantees));
    }

    public static void denySetRole(String role)
    {
        throw new AccessDeniedException(format("Cannot set role %s", role));
    }

    public static void denyExecuteProcedure(String procedureName)
    {
        throw new AccessDeniedException(format("Cannot execute procedure %s", procedureName));
    }

    public static void denyExecuteFunction(String functionName)
    {
        throw new AccessDeniedException(format("Cannot execute function %s", functionName));
    }

    private static Object formatExtraInfo(String extraInfo)
    {
        if (extraInfo == null || extraInfo.isEmpty()) {
            return "";
        }
        return ": " + extraInfo;
    }
}
