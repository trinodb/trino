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
package io.prestosql.spi.connector;

import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.security.AccessDeniedException.denyAddColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyCommentTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateRole;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateTable;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateView;
import static io.prestosql.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.prestosql.spi.security.AccessDeniedException.denyDeleteTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyDropRole;
import static io.prestosql.spi.security.AccessDeniedException.denyDropSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyDropTable;
import static io.prestosql.spi.security.AccessDeniedException.denyDropView;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyInsertTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameColumn;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameSchema;
import static io.prestosql.spi.security.AccessDeniedException.denyRenameTable;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static io.prestosql.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.prestosql.spi.security.AccessDeniedException.denySetRole;
import static io.prestosql.spi.security.AccessDeniedException.denyShowColumnsMetadata;
import static io.prestosql.spi.security.AccessDeniedException.denyShowCurrentRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyShowRoleGrants;
import static io.prestosql.spi.security.AccessDeniedException.denyShowRoles;
import static io.prestosql.spi.security.AccessDeniedException.denyShowSchemas;
import static io.prestosql.spi.security.AccessDeniedException.denyShowTablesMetadata;
import static java.util.Collections.emptySet;

public interface ConnectorAccessControl
{
    /**
     * Check if identity is allowed to create the specified schema in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
        denyCreateSchema(schemaName);
    }

    /**
     * Check if identity is allowed to drop the specified schema in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
        denyDropSchema(schemaName);
    }

    /**
     * Check if identity is allowed to rename the specified schema in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName, String newSchemaName)
    {
        denyRenameSchema(schemaName, newSchemaName);
    }

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must handle filter all results for unauthorized users,
     * since there are multiple way to list schemas.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
        denyShowSchemas();
    }

    /**
     * Filter the list of schemas to those visible to the identity.
     */
    default Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> schemaNames)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to create the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyCreateTable(tableName.toString());
    }

    /**
     * Check if identity is allowed to drop the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDropTable(tableName.toString());
    }

    /**
     * Check if identity is allowed to rename the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    /**
     * Check if identity is allowed to comment the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSetTableComment(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyCommentTable(tableName.toString());
    }

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
        denyShowTablesMetadata(schemaName);
    }

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    default Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<SchemaTableName> tableNames)
    {
        return emptySet();
    }

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowColumnsMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyShowColumnsMetadata(tableName.getTableName());
    }

    /**
     * Filter the list of columns to those visible to the identity.
     */
    default List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return Collections.emptyList();
    }

    /**
     * Check if identity is allowed to add columns to the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyAddColumn(tableName.toString());
    }

    /**
     * Check if identity is allowed to drop columns from the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDropColumn(tableName.toString());
    }

    /**
     * Check if identity is allowed to rename a column in the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyRenameColumn(tableName.toString());
    }

    /**
     * Check if identity is allowed to select from the specified columns in a relation.  The column set can be empty.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        denySelectColumns(tableName.toString(), columnNames);
    }

    /**
     * Check if identity is allowed to insert into the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyInsertTable(tableName.toString());
    }

    /**
     * Check if identity is allowed to delete from the specified table in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
        denyDeleteTable(tableName.toString());
    }

    /**
     * Check if identity is allowed to create the specified view in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName viewName)
    {
        denyCreateView(viewName.toString());
    }

    /**
     * Check if identity is allowed to drop the specified view in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName viewName)
    {
        denyDropView(viewName.toString());
    }

    /**
     * Check if identity is allowed to create a view that selects from the specified columns in a relation.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        denyCreateViewWithSelect(tableName.toString(), identity);
    }

    /**
     * Check if identity is allowed to set the specified property in this catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String propertyName)
    {
        denySetCatalogSessionProperty(propertyName);
    }

    /**
     * Check if identity is allowed to grant to any other user the specified privilege on the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(privilege.toString(), tableName.toString());
    }

    /**
     * Check if identity is allowed to revoke the specified privilege on the specified table from any user.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(privilege.toString(), tableName.toString());
    }

    default void checkCanCreateRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, Optional<PrestoPrincipal> grantor)
    {
        denyCreateRole(role);
    }

    default void checkCanDropRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role)
    {
        denyDropRole(role);
    }

    default void checkCanGrantRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        denyGrantRoles(roles, grantees);
    }

    default void checkCanRevokeRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        denyRevokeRoles(roles, grantees);
    }

    default void checkCanSetRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, String catalogName)
    {
        denySetRole(role);
    }

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        denyShowRoles(catalogName);
    }

    /**
     * Check if identity is allowed to show current roles on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowCurrentRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        denyShowCurrentRoles(catalogName);
    }

    /**
     * Check if identity is allowed to show its own role grants on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    default void checkCanShowRoleGrants(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
        denyShowRoleGrants(catalogName);
    }
}
