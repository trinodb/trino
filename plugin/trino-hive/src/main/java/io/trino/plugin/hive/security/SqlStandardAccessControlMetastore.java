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
package io.trino.plugin.hive.security;

import com.google.inject.Inject;
import io.trino.metastore.Database;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.security.RoleGrant;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SqlStandardAccessControlMetastore
{
    private final HiveTransactionManager transactionManager;

    @Inject
    public SqlStandardAccessControlMetastore(HiveTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    public Set<RoleGrant> listRoleGrants(ConnectorSecurityContext context, HivePrincipal principal)
    {
        return metastore(context).listRoleGrants(principal);
    }

    public Set<HivePrivilegeInfo> listTablePrivileges(ConnectorSecurityContext context, String databaseName, String tableName, Optional<HivePrincipal> principal)
    {
        return metastore(context).listTablePrivileges(databaseName, tableName, principal);
    }

    public Optional<Database> getDatabase(ConnectorSecurityContext context, String databaseName)
    {
        return metastore(context).getDatabase(databaseName);
    }

    private SemiTransactionalHiveMetastore metastore(ConnectorSecurityContext context)
    {
        return transactionManager.get(context.getTransactionHandle(), context.getIdentity()).getMetastore();
    }
}
