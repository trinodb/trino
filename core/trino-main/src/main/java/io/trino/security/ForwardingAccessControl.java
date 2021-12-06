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
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingAccessControl
        implements AccessControl
{
    public static ForwardingAccessControl of(Supplier<AccessControl> accessControlSupplier)
    {
        requireNonNull(accessControlSupplier, "accessControlSupplier is null");
        return new ForwardingAccessControl()
        {
            @Override
            protected AccessControl delegate()
            {
                return requireNonNull(accessControlSupplier.get(), "accessControlSupplier.get() is null");
            }
        };
    }

    protected abstract AccessControl delegate();

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        delegate().checkCanImpersonateUser(identity, userName);
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        delegate().checkCanReadSystemInformation(identity);
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        delegate().checkCanWriteSystemInformation(identity);
    }

    @Override
    @Deprecated
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        delegate().checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        delegate().checkCanExecuteQuery(identity);
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        delegate().checkCanViewQueryOwnedBy(identity, queryOwner);
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return delegate().filterQueriesOwnedBy(identity, queryOwners);
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        delegate().checkCanKillQueryOwnedBy(identity, queryOwner);
    }

    @Override
    public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
    {
        return delegate().filterCatalogs(context, catalogs);
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        delegate().checkCanCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        delegate().checkCanDropSchema(context, schemaName);
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        delegate().checkCanRenameSchema(context, schemaName, newSchemaName);
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext context, CatalogSchemaName schemaName, TrinoPrincipal principal)
    {
        delegate().checkCanSetSchemaAuthorization(context, schemaName, principal);
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        delegate().checkCanShowSchemas(context, catalogName);
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return delegate().filterSchemas(context, catalogName, schemaNames);
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        delegate().checkCanShowCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanShowCreateTable(context, tableName);
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanCreateTable(context, tableName);
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        delegate().checkCanCreateTable(context, tableName, properties);
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanDropTable(context, tableName);
    }

    @Override
    public void checkCanTruncateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanTruncateTable(context, tableName);
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        delegate().checkCanRenameTable(context, tableName, newTableName);
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext context, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        delegate().checkCanSetTableProperties(context, tableName, properties);
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanSetTableComment(context, tableName);
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanSetColumnComment(context, tableName);
    }

    @Override
    public void checkCanShowTables(SecurityContext context, CatalogSchemaName schema)
    {
        delegate().checkCanShowTables(context, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(context, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumns(SecurityContext context, CatalogSchemaTableName table)
    {
        delegate().checkCanShowColumns(context, table);
    }

    @Override
    public Set<String> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, Set<String> columns)
    {
        return delegate().filterColumns(context, tableName, columns);
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanAddColumns(context, tableName);
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanDropColumn(context, tableName);
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanRenameColumn(context, tableName);
    }

    @Override
    public void checkCanSetTableAuthorization(SecurityContext context, QualifiedObjectName tableName, TrinoPrincipal principal)
    {
        delegate().checkCanSetTableAuthorization(context, tableName, principal);
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanInsertIntoTable(context, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        delegate().checkCanDeleteFromTable(context, tableName);
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        delegate().checkCanUpdateTableColumns(context, tableName, updatedColumnNames);
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        delegate().checkCanCreateView(context, viewName);
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        delegate().checkCanRenameView(context, viewName, newViewName);
    }

    @Override
    public void checkCanSetViewAuthorization(SecurityContext context, QualifiedObjectName view, TrinoPrincipal principal)
    {
        delegate().checkCanSetViewAuthorization(context, view, principal);
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        delegate().checkCanDropView(context, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        delegate().checkCanCreateMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        delegate().checkCanCreateMaterializedView(context, materializedViewName, properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        delegate().checkCanRefreshMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext context, QualifiedObjectName materializedViewName)
    {
        delegate().checkCanDropMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        delegate().checkCanRenameMaterializedView(context, viewName, newViewName);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext context, QualifiedObjectName materializedViewName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
        delegate().checkCanSetMaterializedViewProperties(context, materializedViewName, nonNullProperties, nullPropertyNames);
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
        delegate().checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().checkCanGrantSchemaPrivilege(context, privilege, schemaName, grantee, grantOption);
    }

    @Override
    public void checkCanDenySchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee)
    {
        delegate().checkCanDenySchemaPrivilege(context, privilege, schemaName, grantee);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext context, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        delegate().checkCanRevokeSchemaPrivilege(context, privilege, schemaName, revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().checkCanGrantTablePrivilege(context, privilege, tableName, grantee, grantOption);
    }

    @Override
    public void checkCanDenyTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee)
    {
        delegate().checkCanDenyTablePrivilege(context, privilege, tableName, grantee);
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        delegate().checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOption);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        delegate().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate().checkCanSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        delegate().checkCanCreateRole(context, role, grantor, catalogName);
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, Optional<String> catalogName)
    {
        delegate().checkCanDropRole(context, role, catalogName);
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        delegate().checkCanGrantRoles(context, roles, grantees, adminOption, grantor, catalogName);
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        delegate().checkCanRevokeRoles(context, roles, grantees, adminOption, grantor, catalogName);
    }

    @Override
    public void checkCanSetCatalogRole(SecurityContext context, String role, String catalogName)
    {
        delegate().checkCanSetCatalogRole(context, role, catalogName);
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SecurityContext context, Optional<String> catalogName)
    {
        delegate().checkCanShowRoleAuthorizationDescriptors(context, catalogName);
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, Optional<String> catalogName)
    {
        delegate().checkCanShowRoles(context, catalogName);
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, Optional<String> catalogName)
    {
        delegate().checkCanShowCurrentRoles(context, catalogName);
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, Optional<String> catalogName)
    {
        delegate().checkCanShowRoleGrants(context, catalogName);
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext context, QualifiedObjectName procedureName)
    {
        delegate().checkCanExecuteProcedure(context, procedureName);
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        delegate().checkCanExecuteFunction(context, functionName);
    }

    @Override
    public void checkCanExecuteTableProcedure(SecurityContext context, QualifiedObjectName tableName, String procedureName)
    {
        delegate().checkCanExecuteTableProcedure(context, tableName, procedureName);
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        return delegate().getRowFilters(context, tableName);
    }

    @Override
    public List<ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        return delegate().getColumnMasks(context, tableName, columnName, type);
    }
}
