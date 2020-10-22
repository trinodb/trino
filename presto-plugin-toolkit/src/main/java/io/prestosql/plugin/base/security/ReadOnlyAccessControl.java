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
package io.prestosql.plugin.base.security;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.List;
import java.util.Set;

import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameView;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;

public class ReadOnlyAccessControl
        implements ConnectorAccessControl
{
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
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyDropColumn(tableName.toString());
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
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyCommentTable(tableName.toString());
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName)
    {
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyCommentColumn(tableName.toString());
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        // allow
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        // allow
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        denyRenameView(viewName.toString(), newViewName.toString());
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        // allow
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        // allow
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean grantOption)
    {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOption)
    {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(ConnectorSecurityContext context, String catalogName)
    {
        // allow
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context, String catalogName)
    {
        // allow
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context, String catalogName)
    {
        // allow
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context, String catalogName)
    {
        // allow
    }
}
