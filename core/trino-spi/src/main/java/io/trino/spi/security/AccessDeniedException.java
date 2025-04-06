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
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.function.FunctionKind;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

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

    public static void denyCreateCatalog(String catalogName)
    {
        denyCreateCatalog(catalogName, null);
    }

    public static void denyCreateCatalog(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create catalog %s%s", catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropCatalog(String catalogName)
    {
        denyDropCatalog(catalogName, null);
    }

    public static void denyDropCatalog(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop catalog %s%s", catalogName, formatExtraInfo(extraInfo)));
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

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetSchemaAuthorization(String schemaName, TrinoPrincipal principal)
    {
        denySetSchemaAuthorization(schemaName, principal, null);
    }

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal, String)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetSchemaAuthorization(String schemaName, TrinoPrincipal principal, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set authorization for schema %s to %s%s", schemaName, principal, formatExtraInfo(extraInfo)));
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
        denyShowCreateSchema(schemaName, null);
    }

    public static void denyShowCreateSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show create schema for %s%s", schemaName, formatExtraInfo(extraInfo)));
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

    public static void denySetTableProperties(String tableName)
    {
        denySetTableProperties(tableName, null);
    }

    public static void denySetTableProperties(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set table properties to %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentTable(String tableName)
    {
        denyCommentTable(tableName, null);
    }

    public static void denyCommentTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot comment table to %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentView(String viewName)
    {
        denyCommentView(viewName, null);
    }

    public static void denyCommentView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot comment view to %s%s", viewName, formatExtraInfo(extraInfo)));
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

    public static void denyShowColumns(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show columns of table %s%s", tableName, formatExtraInfo(extraInfo)));
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

    public static void denyAlterColumn(String tableName)
    {
        denyAlterColumn(tableName, null);
    }

    public static void denyAlterColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot alter a column for table %s%s", tableName, formatExtraInfo(extraInfo)));
    }

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetTableAuthorization(String tableName, TrinoPrincipal principal)
    {
        denySetTableAuthorization(tableName, principal, null);
    }

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal, String)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetTableAuthorization(String tableName, TrinoPrincipal principal, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set authorization for table %s to %s%s", tableName, principal, formatExtraInfo(extraInfo)));
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

    public static void denyTruncateTable(String tableName)
    {
        denyTruncateTable(tableName, null);
    }

    public static void denyTruncateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot truncate table %s%s", tableName, formatExtraInfo(extraInfo)));
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

    public static void denyRenameView(String viewName, String newViewName)
    {
        denyRenameView(viewName, newViewName, null);
    }

    public static void denyRenameView(String viewName, String newViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename view from %s to %s%s", viewName, newViewName, formatExtraInfo(extraInfo)));
    }

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetViewAuthorization(String viewName, TrinoPrincipal principal)
    {
        denySetViewAuthorization(viewName, principal, null);
    }

    /**
     * @deprecated Use {@link #denySetEntityAuthorization(EntityKindAndName, TrinoPrincipal, String)}
     */
    @Deprecated(forRemoval = true)
    public static void denySetViewAuthorization(String viewName, TrinoPrincipal principal, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set authorization for view %s to %s%s", viewName, principal, formatExtraInfo(extraInfo)));
    }

    public static void denyDropView(String viewName)
    {
        denyDropView(viewName, null);
    }

    public static void denyDropView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop view %s%s", viewName, formatExtraInfo(extraInfo)));
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

    public static void denySetMaterializedViewProperties(String materializedViewName)
    {
        denySetMaterializedViewProperties(materializedViewName, null);
    }

    public static void denySetMaterializedViewProperties(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set properties of materialized view %s%s", materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropMaterializedView(String materializedViewName)
    {
        denyDropMaterializedView(materializedViewName, null);
    }

    public static void denyDropMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop materialized view %s%s", materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameMaterializedView(String materializedViewName, String newMaterializedViewName)
    {
        denyRenameMaterializedView(materializedViewName, newMaterializedViewName, null);
    }

    public static void denyRenameMaterializedView(String materializedViewName, String newMaterializedViewName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot rename materialized view from %s to %s%s", materializedViewName, newMaterializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName)
    {
        denyGrantSchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot grant privilege %s on schema %s%s", privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDenySchemaPrivilege(String privilege, String schemaName)
    {
        denyDenySchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyDenySchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot deny privilege %s on schema %s%s", privilege, schemaName, formatExtraInfo(extraInfo)));
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

    public static void denyDenyTablePrivilege(String privilege, String tableName)
    {
        denyDenyTablePrivilege(privilege, tableName, null);
    }

    public static void denyDenyTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot deny privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName)
    {
        denyRevokeTablePrivilege(privilege, tableName, null);
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot revoke privilege %s on table %s%s", privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantEntityPrivilege(String privilege, EntityKindAndName entity)
    {
        denyGrantEntityPrivilege(privilege, entity, null);
    }

    public static void denyGrantEntityPrivilege(String privilege, EntityKindAndName entity, String extraInfo)
    {
        entityPrivilegeException("grant", privilege, entity, extraInfo);
    }

    public static void denyDenyEntityPrivilege(String privilege, EntityKindAndName entity)
    {
        denyDenyEntityPrivilege(privilege, entity, null);
    }

    public static void denyDenyEntityPrivilege(String privilege, EntityKindAndName entity, String extraInfo)
    {
        entityPrivilegeException("deny", privilege, entity, extraInfo);
    }

    public static void denyRevokeEntityPrivilege(String privilege, EntityKindAndName entity)
    {
        denyRevokeEntityPrivilege(privilege, entity, null);
    }

    public static void denyRevokeEntityPrivilege(String privilege, EntityKindAndName entity, String extraInfo)
    {
        entityPrivilegeException("revoke", privilege, entity, extraInfo);
    }

    private static void entityPrivilegeException(String operation, String privilege, EntityKindAndName entity, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot %s privilege %s on %s %s%s",
                operation,
                privilege,
                entity.entityKind().toLowerCase(Locale.ROOT),
                entity.name(),
                formatExtraInfo(extraInfo)));
    }

    public static void denyShowRoles()
    {
        throw new AccessDeniedException("Cannot show roles");
    }

    public static void denyShowCurrentRoles()
    {
        throw new AccessDeniedException("Cannot show current roles");
    }

    public static void denyShowRoleGrants()
    {
        throw new AccessDeniedException("Cannot show role grants");
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
        throw new AccessDeniedException(format("Cannot grant roles %s to %s", roles, grantees));
    }

    public static void denyRevokeRoles(Set<String> roles, Set<TrinoPrincipal> grantees)
    {
        throw new AccessDeniedException(format("Cannot revoke roles %s from %s", roles, grantees));
    }

    public static void denySetRole(String role)
    {
        throw new AccessDeniedException(format("Cannot set role %s", role));
    }

    public static void denyExecuteProcedure(String procedureName)
    {
        denyExecuteProcedure(procedureName, null);
    }

    public static void denyExecuteProcedure(String procedureName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot execute procedure %s%s", procedureName, formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteFunction(String functionName)
    {
        throw new AccessDeniedException(format("Cannot execute function %s", functionName));
    }

    public static void denyExecuteFunction(String functionName, FunctionKind functionKind, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot execute %s function %s%s", functionKind.name().toLowerCase(Locale.ROOT), functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteTableProcedure(String tableName, String procedureName)
    {
        throw new AccessDeniedException(format("Cannot execute table procedure %s on %s", procedureName, tableName));
    }

    public static void denyShowFunctions(String schemaName)
    {
        denyShowFunctions(schemaName, null);
    }

    public static void denyShowFunctions(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show functions of schema %s%s", schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateFunction(String functionName)
    {
        denyCreateFunction(functionName, null);
    }

    public static void denyCreateFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot create function %s%s", functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropFunction(String functionName)
    {
        denyDropFunction(functionName, null);
    }

    public static void denyDropFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot drop function %s%s", functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateFunction(String functionName)
    {
        denyShowCreateFunction(functionName, null);
    }

    public static void denyShowCreateFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot show create function for %s%s", functionName, formatExtraInfo(extraInfo)));
    }

    public static void denySetEntityAuthorization(EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        denySetEntityAuthorization(entityKindAndName, principal, null);
    }

    public static void denySetEntityAuthorization(EntityKindAndName entityKindAndName, TrinoPrincipal principal, String extraInfo)
    {
        throw new AccessDeniedException(format("Cannot set authorization for %s %s to %s%s",
                entityKindAndName.entityKind().toLowerCase(ENGLISH), entityNameString(entityKindAndName.name()), principal, formatExtraInfo(extraInfo)));
    }

    private static String entityNameString(List<String> name)
    {
        return name.stream().collect(joining("."));
    }

    private static Object formatExtraInfo(String extraInfo)
    {
        if (extraInfo == null || extraInfo.isEmpty()) {
            return "";
        }
        return ": " + extraInfo;
    }
}
