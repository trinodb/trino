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
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.List;
import java.util.Set;

import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
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
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;

public class ReadOnlyAccessControl
        implements ConnectorAccessControl
{
    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDropColumn(tableName.toString());
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanSetTableComment(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyCommentTable(tableName.toString());
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        // allow
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        // allow
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
        denyCreateView(viewName.toString());
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        // allow
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String propertyName)
    {
        // allow
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanShowRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        // allow
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        // allow
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        // allow
    }
}
