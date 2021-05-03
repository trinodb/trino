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

import com.google.common.collect.ImmutableSet;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyGrantExecuteFunctionPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyKillQuery;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
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
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyViewQuery;
import static io.trino.spi.security.AccessDeniedException.denyWriteSystemInformationAccess;

public class DenyAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        denyImpersonateUser(identity.getUser(), userName);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        denySetUser(principal, userName);
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        denyReadSystemInformationAccess();
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        denyWriteSystemInformationAccess();
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        denyExecuteQuery();
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, String queryOwner)
    {
        denyViewQuery();
    }

    @Override
    public Set<String> filterQueriesOwnedBy(Identity identity, Set<String> queryOwners)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, String queryOwner)
    {
        denyKillQuery();
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        denyCreateSchema(schemaName.toString());
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        denyDropSchema(schemaName.toString());
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName.toString(), newSchemaName);
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        denyShowCreateSchema(schemaName.toString());
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        denyShowCreateTable(tableName.toString());
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext context, CatalogSchemaName schemaName, TrinoPrincipal principal)
    {
        denySetSchemaAuthorization(schemaName.toString(), principal);
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        denyCommentTable(tableName.toString());
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        denyCommentColumn(tableName.toString());
    }

    @Override
    public void checkCanShowTables(SecurityContext context, CatalogSchemaName schema)
    {
        denyShowTables(schema.toString());
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table)
    {
        denyShowColumns(table.toString());
    }

    @Override
    public Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        denyShowSchemas();
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        denyDropColumn(tableName.toString());
    }

    @Override
    public void checkCanSetTableAuthorization(SecurityContext context, QualifiedObjectName tableName, TrinoPrincipal principal)
    {
        denySetTableAuthorization(tableName.toString(), principal);
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        denyUpdateTableColumns(tableName.toString(), updatedColumnNames);
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        denyRenameView(viewName.toString(), newViewName.toString());
    }

    @Override
    public void checkCanSetViewAuthorization(SecurityContext context, QualifiedObjectName view, TrinoPrincipal principal)
    {
        denySetViewAuthorization(view.toString(), principal);
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        denyCreateViewWithSelect(tableName.toString(), context.getIdentity());
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        denyCreateMaterializedView(materializedViewName.toString());
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        denyRefreshMaterializedView(materializedViewName.toString());
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        denyDropMaterializedView(materializedViewName.toString());
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
        denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity(), grantee);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        denyGrantSchemaPrivilege(privilege.name(), schemaName.toString());
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        denyRevokeSchemaPrivilege(privilege.name(), schemaName.toString());
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        denySetCatalogSessionProperty(catalogName, propertyName);
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        denySelectColumns(tableName.toString(), columnNames);
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, String catalogName)
    {
        denyCreateRole(role);
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, String catalogName)
    {
        denyDropRole(role);
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
        denyGrantRoles(roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
        denyRevokeRoles(roles, grantees);
    }

    @Override
    public void checkCanSetRole(SecurityContext context, String role, String catalog)
    {
        denySetRole(role);
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SecurityContext context, String catalogName)
    {
        denyShowRoleAuthorizationDescriptors(catalogName);
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, String catalogName)
    {
        denyShowRoles(catalogName);
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, String catalogName)
    {
        denyShowCurrentRoles(catalogName);
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, String catalogName)
    {
        denyShowRoleGrants(catalogName);
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName)
    {
        denyExecuteProcedure(procedureName.toString());
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        denyExecuteFunction(functionName);
    }
}
