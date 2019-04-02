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
import io.prestosql.spi.Name;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.transaction.TransactionId;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AllowAllAccessControl
        implements AccessControl
{
    @Override
    public void checkCanSetUser(Optional<Principal> principal, Name userName)
    {
    }

    @Override
    public Set<Name> filterCatalogs(Identity identity, Set<Name> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, Name catalogName)
    {
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, Name newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, Name catalogName)
    {
    }

    @Override
    public Set<Name> filterSchemas(TransactionId transactionId, Identity identity, Name catalogName, Set<Name> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
    }

    @Override
    public void checkCanSetTableComment(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, Name catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(TransactionId transactionId, Identity identity, CatalogSchemaTableName tableName)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(TransactionId transactionId, Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<Name> columnNames)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, Name catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<Name> columnNames)
    {
    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, Name role, Optional<PrestoPrincipal> grantor, Name catalogName)
    {
    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, Name role, Name catalogName)
    {
    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, Set<Name> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, Name catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, Set<Name> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, Name catalogName)
    {
    }

    @Override
    public void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, Name role, Name catalog)
    {
    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, Name catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, Name catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, Name catalogName)
    {
    }
}
