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

public class AllowAllAccessControl
        implements ConnectorAccessControl
{
    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanSetTableProperties(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName table)
    {
    }

    @Override
    public Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanTruncateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumnNames)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> properties)
    {
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
    }

    @Override
    public void checkCanRenameMaterializedView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
    }

    @Override
    public void checkCanSetMaterializedViewProperties(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> nonNullProperties, Set<String> nullPropertyNames)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanDenySchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee)
    {
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
    }

    @Override
    public void checkCanDenyTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role)
    {
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context)
    {
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context)
    {
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context)
    {
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
    }

    @Override
    public void checkCanExecuteTableProcedure(ConnectorSecurityContext context, SchemaTableName tableName, String procedure)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }
}
