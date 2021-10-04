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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class InjectedConnectorAccessControl
        implements ConnectorAccessControl
{
    private final AccessControl accessControl;
    private final SecurityContext securityContext;
    private final String catalogName;

    public InjectedConnectorAccessControl(AccessControl accessControl, SecurityContext securityContext, String catalogName)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.securityContext = requireNonNull(securityContext, "securityContext is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateSchema(securityContext, getCatalogSchemaName(schemaName));
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropSchema(securityContext, getCatalogSchemaName(schemaName));
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRenameSchema(securityContext, getCatalogSchemaName(schemaName), newSchemaName);
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetSchemaAuthorization(securityContext, getCatalogSchemaName(schemaName), principal);
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowSchemas(securityContext, catalogName);
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        checkArgument(context == null, "context must be null");
        return accessControl.filterSchemas(securityContext, catalogName, schemaNames);
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowCreateSchema(securityContext, getCatalogSchemaName(schemaName));
    }

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowCreateTable(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateTable(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropTable(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRenameTable(securityContext, getQualifiedObjectName(tableName), getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetTableComment(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetColumnComment(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowTables(securityContext, getCatalogSchemaName(schemaName));
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        checkArgument(context == null, "context must be null");
        return accessControl.filterTables(securityContext, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowColumns(securityContext, new CatalogSchemaTableName(catalogName, tableName));
    }

    @Override
    public Set<String> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columns)
    {
        checkArgument(context == null, "context must be null");
        return accessControl.filterColumns(securityContext, new CatalogSchemaTableName(catalogName, tableName), columns);
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanAddColumns(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropColumn(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetTableAuthorization(securityContext, getQualifiedObjectName(tableName), principal);
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRenameColumn(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSelectFromColumns(securityContext, getQualifiedObjectName(tableName), columnNames);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanInsertIntoTable(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDeleteFromTable(securityContext, getQualifiedObjectName(tableName));
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumns)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanUpdateTableColumns(securityContext, getQualifiedObjectName(tableName), updatedColumns);
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateView(securityContext, getQualifiedObjectName(viewName));
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetViewAuthorization(securityContext, getQualifiedObjectName(viewName), principal);
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRenameView(securityContext, getQualifiedObjectName(viewName), getQualifiedObjectName(viewName));
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropView(securityContext, getQualifiedObjectName(viewName));
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateViewWithSelectFromColumns(securityContext, getQualifiedObjectName(tableName), columnNames);
    }

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateMaterializedView(securityContext, getQualifiedObjectName(materializedViewName));
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRefreshMaterializedView(securityContext, getQualifiedObjectName(materializedViewName));
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropMaterializedView(securityContext, getQualifiedObjectName(materializedViewName));
    }

    @Override
    public void checkCanRenameMaterializedView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRenameMaterializedView(securityContext, getQualifiedObjectName(viewName), getQualifiedObjectName(newViewName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetCatalogSessionProperty(securityContext, propertyName, catalogName);
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanGrantSchemaPrivilege(securityContext, privilege, getCatalogSchemaName(schemaName), grantee, grantOption);
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRevokeSchemaPrivilege(securityContext, privilege, getCatalogSchemaName(schemaName), revokee, grantOption);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanGrantTablePrivilege(securityContext, privilege, getQualifiedObjectName(tableName), grantee, grantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRevokeTablePrivilege(securityContext, privilege, getQualifiedObjectName(tableName), revokee, grantOption);
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanCreateRole(securityContext, role, grantor, Optional.of(catalogName));
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanDropRole(securityContext, role, Optional.of(catalogName));
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanGrantRoles(securityContext, roles, grantees, adminOption, grantor, Optional.of(catalogName));
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context,
            Set<String> roles,
            Set<TrinoPrincipal> grantees,
            boolean adminOption,
            Optional<TrinoPrincipal> grantor)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanRevokeRoles(securityContext, roles, grantees, adminOption, grantor, Optional.of(catalogName));
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanSetCatalogRole(securityContext, role, catalogName);
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowRoleAuthorizationDescriptors(securityContext, Optional.of(catalogName));
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowRoles(securityContext, Optional.of(catalogName));
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowCurrentRoles(securityContext, Optional.of(catalogName));
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanShowRoleGrants(securityContext, Optional.of(catalogName));
    }

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
        checkArgument(context == null, "context must be null");
        accessControl.checkCanExecuteProcedure(securityContext, new QualifiedObjectName(catalogName, procedure.getSchemaName(), procedure.getRoutineName()));
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkArgument(context == null, "context must be null");
        if (accessControl.getRowFilters(securityContext, new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName())).isEmpty()) {
            return Optional.empty();
        }
        throw new TrinoException(NOT_SUPPORTED, "Row filtering not supported");
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        checkArgument(context == null, "context must be null");
        if (accessControl.getColumnMasks(securityContext, new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName()), columnName, type).isEmpty()) {
            return Optional.empty();
        }
        throw new TrinoException(NOT_SUPPORTED, "Column masking not supported");
    }

    private QualifiedObjectName getQualifiedObjectName(SchemaTableName schemaTableName)
    {
        return new QualifiedObjectName(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    private CatalogSchemaName getCatalogSchemaName(String schemaName)
    {
        return new CatalogSchemaName(catalogName, schemaName);
    }
}
