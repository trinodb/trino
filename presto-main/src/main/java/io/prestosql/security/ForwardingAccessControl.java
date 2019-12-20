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
package io.prestosql.security;

import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.security.Principal;
import java.util.List;
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
            protected AccessControl getDelegate()
            {
                return requireNonNull(accessControlSupplier.get(), "accessControlSupplier.get() is null");
            }
        };
    }

    protected abstract AccessControl getDelegate();

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        getDelegate().checkCanSetUser(principal, userName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return getDelegate().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        getDelegate().checkCanCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        getDelegate().checkCanDropSchema(context, schemaName);
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        getDelegate().checkCanRenameSchema(context, schemaName, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        getDelegate().checkCanShowSchemas(context, catalogName);
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return getDelegate().filterSchemas(context, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanCreateTable(context, tableName);
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanDropTable(context, tableName);
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        getDelegate().checkCanRenameTable(context, tableName, newTableName);
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanSetTableComment(context, tableName);
    }

    @Override
    public void checkCanShowTablesMetadata(SecurityContext context, CatalogSchemaName schema)
    {
        getDelegate().checkCanShowTablesMetadata(context, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return getDelegate().filterTables(context, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumnsMetadata(SecurityContext context, CatalogSchemaTableName table)
    {
        getDelegate().checkCanShowColumnsMetadata(context, table);
    }

    @Override
    public List<ColumnMetadata> filterColumns(SecurityContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return getDelegate().filterColumns(context, tableName, columns);
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanAddColumns(context, tableName);
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanDropColumn(context, tableName);
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanRenameColumn(context, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanInsertIntoTable(context, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        getDelegate().checkCanDeleteFromTable(context, tableName);
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        getDelegate().checkCanCreateView(context, viewName);
    }

    @Override
    public void checkCanRenameView(SecurityContext context, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        getDelegate().checkCanRenameView(context, viewName, newViewName);
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        getDelegate().checkCanDropView(context, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        getDelegate().checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        getDelegate().checkCanGrantTablePrivilege(context, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        getDelegate().checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOptionFor);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        getDelegate().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext context, String catalogName, String propertyName)
    {
        getDelegate().checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        getDelegate().checkCanSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        getDelegate().checkCanCreateRole(context, role, grantor, catalogName);
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, String catalogName)
    {
        getDelegate().checkCanDropRole(context, role, catalogName);
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        getDelegate().checkCanGrantRoles(context, roles, grantees, withAdminOption, grantor, catalogName);
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        getDelegate().checkCanRevokeRoles(context, roles, grantees, adminOptionFor, grantor, catalogName);
    }

    @Override
    public void checkCanSetRole(SecurityContext context, String role, String catalogName)
    {
        getDelegate().checkCanSetRole(context, role, catalogName);
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, String catalogName)
    {
        getDelegate().checkCanShowRoles(context, catalogName);
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, String catalogName)
    {
        getDelegate().checkCanShowCurrentRoles(context, catalogName);
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, String catalogName)
    {
        getDelegate().checkCanShowRoleGrants(context, catalogName);
    }
}
