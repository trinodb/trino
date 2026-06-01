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
        throw new AccessDeniedException("User %s cannot impersonate user %s%s".formatted(originalUser, newUser, formatExtraInfo(extraInfo)));
    }

    public static void denySetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName, null);
    }

    public static void denySetUser(Optional<Principal> principal, String userName, String extraInfo)
    {
        throw new AccessDeniedException("Principal %s cannot become user %s%s".formatted(principal.orElse(null), userName, formatExtraInfo(extraInfo)));
    }

    public static void denyReadSystemInformationAccess()
    {
        denyReadSystemInformationAccess(null);
    }

    public static void denyReadSystemInformationAccess(String extraInfo)
    {
        throw new AccessDeniedException("Cannot read system information%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyWriteSystemInformationAccess()
    {
        denyWriteSystemInformationAccess(null);
    }

    public static void denyWriteSystemInformationAccess(String extraInfo)
    {
        throw new AccessDeniedException("Cannot write system information%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteQuery()
    {
        denyExecuteQuery(null);
    }

    public static void denyExecuteQuery(String extraInfo)
    {
        throw new AccessDeniedException("Cannot execute query%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyViewQuery()
    {
        denyViewQuery(null);
    }

    public static void denyViewQuery(String extraInfo)
    {
        throw new AccessDeniedException("Cannot view query%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyKillQuery()
    {
        denyKillQuery(null);
    }

    public static void denyKillQuery(String extraInfo)
    {
        throw new AccessDeniedException("Cannot kill query%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyCatalogAccess(String catalogName)
    {
        denyCatalogAccess(catalogName, null);
    }

    public static void denyCatalogAccess(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot access catalog %s%s".formatted(catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateCatalog(String catalogName)
    {
        denyCreateCatalog(catalogName, null);
    }

    public static void denyCreateCatalog(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create catalog %s%s".formatted(catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropCatalog(String catalogName)
    {
        denyDropCatalog(catalogName, null);
    }

    public static void denyDropCatalog(String catalogName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop catalog %s%s".formatted(catalogName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateSchema(String schemaName)
    {
        denyCreateSchema(schemaName, null);
    }

    public static void denyCreateSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create schema %s%s".formatted(schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropSchema(String schemaName)
    {
        denyDropSchema(schemaName, null);
    }

    public static void denyDropSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop schema %s%s".formatted(schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName, newSchemaName, null);
    }

    public static void denyRenameSchema(String schemaName, String newSchemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot rename schema from %s to %s%s".formatted(schemaName, newSchemaName, formatExtraInfo(extraInfo)));
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
        throw new AccessDeniedException("Cannot set authorization for schema %s to %s%s".formatted(schemaName, principal, formatExtraInfo(extraInfo)));
    }

    public static void denyShowSchemas()
    {
        denyShowSchemas(null);
    }

    public static void denyShowSchemas(String extraInfo)
    {
        throw new AccessDeniedException("Cannot show schemas%s".formatted(formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateSchema(String schemaName)
    {
        denyShowCreateSchema(schemaName, null);
    }

    public static void denyShowCreateSchema(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show create schema for %s%s".formatted(schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateTable(String tableName)
    {
        denyShowCreateTable(tableName, null);
    }

    public static void denyShowCreateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show create table for %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateTable(String tableName)
    {
        denyCreateTable(tableName, null);
    }

    public static void denyCreateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropTable(String tableName)
    {
        denyDropTable(tableName, null);
    }

    public static void denyDropTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameTable(String tableName, String newTableName)
    {
        denyRenameTable(tableName, newTableName, null);
    }

    public static void denyRenameTable(String tableName, String newTableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot rename table from %s to %s%s".formatted(tableName, newTableName, formatExtraInfo(extraInfo)));
    }

    public static void denySetTableProperties(String tableName)
    {
        denySetTableProperties(tableName, null);
    }

    public static void denySetTableProperties(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot set table properties to %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentTable(String tableName)
    {
        denyCommentTable(tableName, null);
    }

    public static void denyCommentTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot comment table to %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentView(String viewName)
    {
        denyCommentView(viewName, null);
    }

    public static void denyCommentView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot comment view to %s%s".formatted(viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCommentColumn(String tableName)
    {
        denyCommentColumn(tableName, null);
    }

    public static void denyCommentColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot comment column to %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowTables(String schemaName)
    {
        denyShowTables(schemaName, null);
    }

    public static void denyShowTables(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show tables of schema %s%s".formatted(schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowColumns(String tableName)
    {
        throw new AccessDeniedException("Cannot show columns of table %s".formatted(tableName));
    }

    public static void denyShowColumns(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show columns of table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyAddColumn(String tableName)
    {
        denyAddColumn(tableName, null);
    }

    public static void denyAddColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot add a column to table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropColumn(String tableName)
    {
        denyDropColumn(tableName, null);
    }

    public static void denyDropColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop a column from table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyAlterColumn(String tableName)
    {
        denyAlterColumn(tableName, null);
    }

    public static void denyAlterColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot alter a column for table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
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
        throw new AccessDeniedException("Cannot set authorization for table %s to %s%s".formatted(tableName, principal, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameColumn(String tableName)
    {
        denyRenameColumn(tableName, null);
    }

    public static void denyRenameColumn(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot rename a column in table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySelectTable(String tableName)
    {
        denySelectTable(tableName, (String) null);
    }

    public static void denySelectTable(String tableName, String extraInfo)
    {
        denySelectTable(tableName, Optional.empty(), extraInfo);
    }

    public static void denySelectTable(String tableName, Optional<String> branchName)
    {
        denySelectTable(tableName, branchName, null);
    }

    public static void denySelectTable(String tableName, Optional<String> branchName, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "Cannot select from branch %s in table %s%s".formatted(branch, tableName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "Cannot select from table %s%s".formatted(tableName, formatExtraInfo(extraInfo))));
    }

    public static void denyInsertTable(String tableName)
    {
        denyInsertTable(tableName, (String) null);
    }

    public static void denyInsertTable(String tableName, String extraInfo)
    {
        denyInsertTable(tableName, Optional.empty(), extraInfo);
    }

    public static void denyInsertTable(String tableName, Optional<String> branchName)
    {
        denyInsertTable(tableName, branchName, null);
    }

    public static void denyInsertTable(String tableName, Optional<String> branchName, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "Cannot insert into branch %s in table %s%s".formatted(branch, tableName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "Cannot insert into table %s%s".formatted(tableName, formatExtraInfo(extraInfo))));
    }

    public static void denyDeleteTable(String tableName)
    {
        denyDeleteTable(tableName, (String) null);
    }

    public static void denyDeleteTable(String tableName, String extraInfo)
    {
        denyDeleteTable(tableName, Optional.empty(), extraInfo);
    }

    public static void denyDeleteTable(String tableName, Optional<String> branchName)
    {
        denyDeleteTable(tableName, branchName, null);
    }

    public static void denyDeleteTable(String tableName, Optional<String> branchName, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "Cannot delete from branch %s in table %s%s".formatted(branch, tableName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "Cannot delete from table %s%s".formatted(tableName, formatExtraInfo(extraInfo))));
    }

    public static void denyTruncateTable(String tableName)
    {
        denyTruncateTable(tableName, null);
    }

    public static void denyTruncateTable(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot truncate table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyUpdateTableColumns(String tableName, Set<String> updatedColumnNames)
    {
        denyUpdateTableColumns(tableName, updatedColumnNames, null);
    }

    public static void denyUpdateTableColumns(String tableName, Set<String> updatedColumnNames, String extraInfo)
    {
        denyUpdateTableColumns(tableName, Optional.empty(), updatedColumnNames, extraInfo);
    }

    public static void denyUpdateTableColumns(String tableName, Optional<String> branchName, Set<String> updatedColumnNames)
    {
        denyUpdateTableColumns(tableName, branchName, updatedColumnNames, null);
    }

    public static void denyUpdateTableColumns(String tableName, Optional<String> branchName, Set<String> updatedColumnNames, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "Cannot update columns %s in branch %s in table %s%s".formatted(updatedColumnNames, branch, tableName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "Cannot update columns %s in table %s%s".formatted(updatedColumnNames, tableName, formatExtraInfo(extraInfo))));
    }

    public static void denyCreateView(String viewName)
    {
        denyCreateView(viewName, null);
    }

    public static void denyCreateView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create view %s%s".formatted(viewName, formatExtraInfo(extraInfo)));
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
        denyCreateViewWithSelect(sourceName, Optional.empty(), identity, extraInfo);
    }

    public static void denyCreateViewWithSelect(String sourceName, Optional<String> branchName, Identity identity)
    {
        denyCreateViewWithSelect(sourceName, branchName, identity.toConnectorIdentity());
    }

    public static void denyCreateViewWithSelect(String sourceName, Optional<String> branchName, ConnectorIdentity identity)
    {
        denyCreateViewWithSelect(sourceName, branchName, identity, null);
    }

    public static void denyCreateViewWithSelect(String sourceName, Optional<String> branchName, ConnectorIdentity identity, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "View owner '%s' cannot create view that selects from branch %s in %s%s".formatted(identity.getUser(), branch, sourceName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "View owner '%s' cannot create view that selects from %s%s".formatted(identity.getUser(), sourceName, formatExtraInfo(extraInfo))));
    }

    public static void denyRenameView(String viewName, String newViewName)
    {
        denyRenameView(viewName, newViewName, null);
    }

    public static void denyRenameView(String viewName, String newViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot rename view from %s to %s%s".formatted(viewName, newViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyRefreshView(String viewName)
    {
        denyRefreshView(viewName, null);
    }

    public static void denyRefreshView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot refresh view %s%s".formatted(viewName, formatExtraInfo(extraInfo)));
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
        throw new AccessDeniedException("Cannot set authorization for view %s to %s%s".formatted(viewName, principal, formatExtraInfo(extraInfo)));
    }

    public static void denyDropView(String viewName)
    {
        denyDropView(viewName, null);
    }

    public static void denyDropView(String viewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop view %s%s".formatted(viewName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateMaterializedView(String materializedViewName)
    {
        denyCreateMaterializedView(materializedViewName, null);
    }

    public static void denyCreateMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create materialized view %s%s".formatted(materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyRefreshMaterializedView(String materializedViewName)
    {
        denyRefreshMaterializedView(materializedViewName, null);
    }

    public static void denyRefreshMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot refresh materialized view %s%s".formatted(materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denySetMaterializedViewProperties(String materializedViewName)
    {
        denySetMaterializedViewProperties(materializedViewName, null);
    }

    public static void denySetMaterializedViewProperties(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot set properties of materialized view %s%s".formatted(materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropMaterializedView(String materializedViewName)
    {
        denyDropMaterializedView(materializedViewName, null);
    }

    public static void denyDropMaterializedView(String materializedViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop materialized view %s%s".formatted(materializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyRenameMaterializedView(String materializedViewName, String newMaterializedViewName)
    {
        denyRenameMaterializedView(materializedViewName, newMaterializedViewName, null);
    }

    public static void denyRenameMaterializedView(String materializedViewName, String newMaterializedViewName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot rename materialized view from %s to %s%s".formatted(materializedViewName, newMaterializedViewName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName)
    {
        denyGrantSchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyGrantSchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot grant privilege %s on schema %s%s".formatted(privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyDenySchemaPrivilege(String privilege, String schemaName)
    {
        denyDenySchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyDenySchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot deny privilege %s on schema %s%s".formatted(privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeSchemaPrivilege(String privilege, String schemaName)
    {
        denyRevokeSchemaPrivilege(privilege, schemaName, null);
    }

    public static void denyRevokeSchemaPrivilege(String privilege, String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot revoke privilege %s on schema %s%s".formatted(privilege, schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName)
    {
        denyGrantTablePrivilege(privilege, tableName, null);
    }

    public static void denyGrantTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot grant privilege %s on table %s%s".formatted(privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDenyTablePrivilege(String privilege, String tableName)
    {
        denyDenyTablePrivilege(privilege, tableName, null);
    }

    public static void denyDenyTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot deny privilege %s on table %s%s".formatted(privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName)
    {
        denyRevokeTablePrivilege(privilege, tableName, null);
    }

    public static void denyRevokeTablePrivilege(String privilege, String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot revoke privilege %s on table %s%s".formatted(privilege, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyGrantTableBranchPrivilege(String privilege, String tableName, String branchName)
    {
        denyGrantTableBranchPrivilege(privilege, tableName, branchName, null);
    }

    public static void denyGrantTableBranchPrivilege(String privilege, String tableName, String branchName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot grant privilege %s on branch %s in table %s%s".formatted(privilege, branchName, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDenyTableBranchPrivilege(String privilege, String tableName, String branchName)
    {
        denyDenyTableBranchPrivilege(privilege, tableName, branchName, null);
    }

    public static void denyDenyTableBranchPrivilege(String privilege, String tableName, String branchName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot deny privilege %s on branch %s in table %s%s".formatted(privilege, branchName, tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyRevokeTableBranchPrivilege(String privilege, String tableName, String branchName)
    {
        denyRevokeTableBranchPrivilege(privilege, tableName, branchName, null);
    }

    public static void denyRevokeTableBranchPrivilege(String privilege, String tableName, String branchName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot revoke privilege %s on branch %s in table %s%s".formatted(privilege, branchName, tableName, formatExtraInfo(extraInfo)));
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
        throw new AccessDeniedException("Cannot %s privilege %s on %s %s%s".formatted(
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
        throw new AccessDeniedException("Cannot set system session property %s%s".formatted(propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName, null);
    }

    public static void denySetCatalogSessionProperty(String catalogName, String propertyName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot set catalog session property %s.%s%s".formatted(catalogName, propertyName, formatExtraInfo(extraInfo)));
    }

    public static void denySetCatalogSessionProperty(String propertyName)
    {
        throw new AccessDeniedException("Cannot set catalog session property %s".formatted(propertyName));
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames)
    {
        denySelectColumns(tableName, columnNames, null);
    }

    public static void denySelectColumns(String tableName, Collection<String> columnNames, String extraInfo)
    {
        denySelectColumns(tableName, Optional.empty(), columnNames, extraInfo);
    }

    public static void denySelectColumns(String tableName, Optional<String> branchName, Collection<String> columnNames)
    {
        denySelectColumns(tableName, branchName, columnNames, null);
    }

    public static void denySelectColumns(String tableName, Optional<String> branchName, Collection<String> columnNames, String extraInfo)
    {
        throw new AccessDeniedException(branchName
                .map(branch -> "Cannot select from columns %s in branch %s in table %s%s".formatted(columnNames, branch, tableName, formatExtraInfo(extraInfo)))
                .orElseGet(() -> "Cannot select from columns %s in table or view %s%s".formatted(columnNames, tableName, formatExtraInfo(extraInfo))));
    }

    public static void denyCreateRole(String roleName)
    {
        throw new AccessDeniedException("Cannot create role %s".formatted(roleName));
    }

    public static void denyDropRole(String roleName)
    {
        throw new AccessDeniedException("Cannot drop role %s".formatted(roleName));
    }

    public static void denyGrantRoles(Set<String> roles, Set<TrinoPrincipal> grantees)
    {
        throw new AccessDeniedException("Cannot grant roles %s to %s".formatted(roles, grantees));
    }

    public static void denyRevokeRoles(Set<String> roles, Set<TrinoPrincipal> grantees)
    {
        throw new AccessDeniedException("Cannot revoke roles %s from %s".formatted(roles, grantees));
    }

    public static void denySetRole(String role)
    {
        throw new AccessDeniedException("Cannot set role %s".formatted(role));
    }

    public static void denyExecuteProcedure(String procedureName)
    {
        denyExecuteProcedure(procedureName, null);
    }

    public static void denyExecuteProcedure(String procedureName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot execute procedure %s%s".formatted(procedureName, formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteFunction(String functionName)
    {
        throw new AccessDeniedException("Cannot execute function %s".formatted(functionName));
    }

    public static void denyExecuteFunction(String functionName, FunctionKind functionKind, String extraInfo)
    {
        throw new AccessDeniedException("Cannot execute %s function %s%s".formatted(functionKind.name().toLowerCase(Locale.ROOT), functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyExecuteTableProcedure(String tableName, String procedureName)
    {
        throw new AccessDeniedException("Cannot execute table procedure %s on %s".formatted(procedureName, tableName));
    }

    public static void denyShowFunctions(String schemaName)
    {
        denyShowFunctions(schemaName, null);
    }

    public static void denyShowFunctions(String schemaName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show functions of schema %s%s".formatted(schemaName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateFunction(String functionName)
    {
        denyCreateFunction(functionName, null);
    }

    public static void denyCreateFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create function %s%s".formatted(functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropFunction(String functionName)
    {
        denyDropFunction(functionName, null);
    }

    public static void denyDropFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop function %s%s".formatted(functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowCreateFunction(String functionName)
    {
        denyShowCreateFunction(functionName, null);
    }

    public static void denyShowCreateFunction(String functionName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show create function for %s%s".formatted(functionName, formatExtraInfo(extraInfo)));
    }

    public static void denyShowBranches(String tableName)
    {
        denyShowBranches(tableName, null);
    }

    public static void denyShowBranches(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot show branches of table %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyCreateBranch(String tableName)
    {
        denyCreateBranch(tableName, null);
    }

    public static void denyCreateBranch(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot create a branch in %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyDropBranch(String tableName)
    {
        denyDropBranch(tableName, null);
    }

    public static void denyDropBranch(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot drop a branch from %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denyFastForwardBranch(String tableName)
    {
        denyFastForwardBranch(tableName, null);
    }

    public static void denyFastForwardBranch(String tableName, String extraInfo)
    {
        throw new AccessDeniedException("Cannot fast-forward a branch in %s%s".formatted(tableName, formatExtraInfo(extraInfo)));
    }

    public static void denySetEntityAuthorization(EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        denySetEntityAuthorization(entityKindAndName, principal, null);
    }

    public static void denySetEntityAuthorization(EntityKindAndName entityKindAndName, TrinoPrincipal principal, String extraInfo)
    {
        throw new AccessDeniedException("Cannot set authorization for %s %s to %s%s".formatted(
                entityKindAndName.entityKind().toLowerCase(ENGLISH),
                entityNameString(entityKindAndName.name()),
                principal,
                formatExtraInfo(extraInfo)));
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
