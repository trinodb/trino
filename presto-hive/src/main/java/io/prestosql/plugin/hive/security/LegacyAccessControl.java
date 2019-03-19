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
package io.prestosql.plugin.hive.security;

import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static java.util.Objects.requireNonNull;

public class LegacyAccessControl
        implements ConnectorAccessControl
{
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowCommentTable;
    private final boolean allowAddColumn;
    private final boolean allowDropColumn;
    private final boolean allowRenameColumn;

    @Inject
    public LegacyAccessControl(
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            LegacySecurityConfig securityConfig)
    {
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");

        requireNonNull(securityConfig, "securityConfig is null");
        allowDropTable = securityConfig.getAllowDropTable();
        allowRenameTable = securityConfig.getAllowRenameTable();
        allowCommentTable = securityConfig.getAllowCommentTable();
        allowAddColumn = securityConfig.getAllowAddColumn();
        allowDropColumn = securityConfig.getAllowDropColumn();
        allowRenameColumn = securityConfig.getAllowRenameColumn();
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName, String newSchemaName)
    {
    }

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
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowDropTable) {
            denyDropTable(tableName.toString());
        }

        Optional<Table> target = metastoreProvider.apply(((HiveTransactionHandle) transaction)).getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }

        if (!identity.getUser().equals(target.get().getOwner())) {
            denyDropTable(tableName.toString(), "Owner of the table is different from session user");
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowCommentTable) {
            denyCommentTable(tableName.toString());
        }
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
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowAddColumn) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowDropColumn) {
            denyDropColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
        if (!allowRenameColumn) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanCreateRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, Optional<PrestoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanDropRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role)
    {
    }

    @Override
    public void checkCanGrantRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanSetRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }
}
