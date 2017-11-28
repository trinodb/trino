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

import io.prestosql.plugin.base.security.ForwardingConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.hive.HiveMetadata.getSourceTableNameForPartitionsTable;
import static io.prestosql.plugin.hive.HiveMetadata.isPartitionsSystemTable;
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static java.util.Objects.requireNonNull;

public class PartitionsAwareAccessControl
        extends ForwardingConnectorAccessControl
{
    private final ConnectorAccessControl delegate;

    public PartitionsAwareAccessControl(ConnectorAccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected ConnectorAccessControl delegate()
    {
        return delegate;
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        if (isPartitionsSystemTable(tableName)) {
            try {
                checkCanSelectFromColumns(transactionHandle, identity, getSourceTableNameForPartitionsTable(tableName), columnNames);
                return;
            }
            catch (AccessDeniedException e) {
                denySelectTable(tableName.toString());
            }
        }

        delegate.checkCanSelectFromColumns(transactionHandle, identity, tableName, columnNames);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate.checkCanInsertIntoTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        delegate.checkCanDeleteFromTable(transactionHandle, identity, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        delegate.checkCanCreateView(transactionHandle, identity, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        delegate.checkCanDropView(transactionHandle, identity, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        delegate.checkCanCreateViewWithSelectFromColumns(transactionHandle, identity, tableName, columnNames);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, Identity identity, String propertyName)
    {
        delegate.checkCanSetCatalogSessionProperty(transactionHandle, identity, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        delegate.checkCanGrantTablePrivilege(transactionHandle, identity, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
        delegate.checkCanRevokeTablePrivilege(transactionHandle, identity, privilege, tableName, revokee, grantOptionFor);
    }

    @Override
    public void checkCanCreateRole(ConnectorTransactionHandle transactionHandle, Identity identity, String role, Optional<PrestoPrincipal> grantor)
    {
        delegate.checkCanCreateRole(transactionHandle, identity, role, grantor);
    }

    @Override
    public void checkCanDropRole(ConnectorTransactionHandle transactionHandle, Identity identity, String role)
    {
        delegate.checkCanDropRole(transactionHandle, identity, role);
    }
}
