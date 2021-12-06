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
package io.trino.plugin.base.security;

import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingConnectorAccessControl
        implements ConnectorAccessControl
{
    public static ConnectorAccessControl of(Supplier<ConnectorAccessControl> connectorAccessControlSupplier)
    {
        requireNonNull(connectorAccessControlSupplier, "connectorAccessControlSupplier is null");
        return new ForwardingConnectorAccessControl()
        {
            @Override
            protected ConnectorAccessControl delegate()
            {
                return connectorAccessControlSupplier.get();
            }
        };
    }

    protected abstract ConnectorAccessControl delegate();

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        delegate().checkCanCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        delegate().checkCanDropSchema(context, schemaName);
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        delegate().checkCanRenameSchema(context, schemaName, newSchemaName);
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
        delegate().checkCanSetSchemaAuthorization(context, schemaName, principal);
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
        delegate().checkCanShowSchemas(context);
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return delegate().filterSchemas(context, schemaNames);
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        delegate().checkCanShowCreateSchema(context, schemaName);
    }

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanShowCreateTable(context, tableName);
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanCreateTable(context, tableName);
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
    {
        delegate().checkCanCreateTable(context, tableName, properties);
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanDropTable(context, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        delegate().checkCanRenameTable(context, tableName, newTableName);
    }

    @Override
    public void checkCanSetTableProperties(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
    {
        delegate().checkCanSetTableProperties(context, tableName, properties);
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanSetTableComment(context, tableName);
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanSetColumnComment(context, tableName);
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
        delegate().checkCanShowTables(context, schemaName);
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(context, tableNames);
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanShowColumns(context, tableName);
    }

    @Override
    public Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        return delegate().filterColumns(context, tableName, columns);
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanAddColumn(context, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanDropColumn(context, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanRenameColumn(context, tableName);
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
        delegate().checkCanSetTableAuthorization(context, tableName, principal);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanInsertIntoTable(context, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanDeleteFromTable(context, tableName);
    }

    @Override
    public void checkCanTruncateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        delegate().checkCanTruncateTable(context, tableName);
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumns)
    {
        delegate().checkCanUpdateTableColumns(context, tableName, updatedColumns);
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        delegate().checkCanCreateView(context, viewName);
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        delegate().checkCanRenameView(context, viewName, newViewName);
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        delegate().checkCanSetViewAuthorization(context, viewName, principal);
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        delegate().checkCanDropView(context, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        delegate().checkCanCreateMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> properties)
    {
        delegate().checkCanCreateMaterializedView(context, materializedViewName, properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        delegate().checkCanRefreshMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        delegate().checkCanDropMaterializedView(context, materializedViewName);
    }

    @Override
    public void checkCanRenameMaterializedView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        delegate().checkCanRenameMaterializedView(context, viewName, newViewName);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
        delegate().checkCanSetMaterializedViewProperties(context, materializedViewName, nonNullProperties, nullPropertyNames);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(context, propertyName);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().checkCanGrantSchemaPrivilege(context, privilege, schemaName, grantee, grantOption);
    }

    @Override
    public void checkCanDenySchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee)
    {
        delegate().checkCanDenySchemaPrivilege(context, privilege, schemaName, grantee);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        delegate().checkCanRevokeSchemaPrivilege(context, privilege, schemaName, revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        delegate().checkCanGrantTablePrivilege(context, privilege, tableName, grantee, grantOption);
    }

    @Override
    public void checkCanDenyTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee)
    {
        delegate().checkCanDenyTablePrivilege(context, privilege, tableName, grantee);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        delegate().checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOption);
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        delegate().checkCanCreateRole(context, role, grantor);
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        delegate().checkCanDropRole(context, role);
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        delegate().checkCanGrantRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        delegate().checkCanRevokeRoles(context, roles, grantees, adminOption, grantor);
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role)
    {
        delegate().checkCanSetRole(context, role);
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context)
    {
        delegate().checkCanShowRoleAuthorizationDescriptors(context);
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context)
    {
        delegate().checkCanShowRoles(context);
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context)
    {
        delegate().checkCanShowCurrentRoles(context);
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context)
    {
        delegate().checkCanShowRoleGrants(context);
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
        delegate().checkCanExecuteProcedure(context, procedure);
    }

    @Override
    public void checkCanExecuteTableProcedure(ConnectorSecurityContext context, SchemaTableName tableName, String procedure)
    {
        delegate().checkCanExecuteTableProcedure(context, tableName, procedure);
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return delegate().getRowFilter(context, tableName);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        return delegate().getColumnMask(context, tableName, columnName, type);
    }
}
