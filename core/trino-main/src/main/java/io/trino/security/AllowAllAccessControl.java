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

public class AllowAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, String queryOwner)
    {
    }

    @Override
    public Set<String> filterQueriesOwnedBy(Identity identity, Set<String> queryOwners)
    {
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, String queryOwner)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext context, CatalogSchemaName schemaName, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanShowTables(SecurityContext context, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName tableName)
    {
    }

    @Override
    public Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanSetTableAuthorization(SecurityContext context, QualifiedObjectName tableName, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
    }

    @Override
    public void checkCanSetViewAuthorization(SecurityContext context, QualifiedObjectName view, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, String catalogName)
    {
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanSetRole(SecurityContext context, String role, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, String catalogName)
    {
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName)
    {
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
    }
}
