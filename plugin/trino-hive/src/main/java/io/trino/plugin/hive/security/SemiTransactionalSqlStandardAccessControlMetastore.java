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

import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.security.RoleGrant;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SemiTransactionalSqlStandardAccessControlMetastore
        implements SqlStandardAccessControlMetastore
{
    private final HiveTransactionManager transactionManager;

    @Inject
    public SemiTransactionalSqlStandardAccessControlMetastore(HiveTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSecurityContext context, HivePrincipal principal)
    {
        SemiTransactionalHiveMetastore metastore = transactionManager.get(context.getTransactionHandle()).getMetastore();
        return metastore.listRoleGrants(principal);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(ConnectorSecurityContext context, HiveIdentity identity, String databaseName, String tableName, Optional<HivePrincipal> principal)
    {
        SemiTransactionalHiveMetastore metastore = transactionManager.get(context.getTransactionHandle()).getMetastore();
        return metastore.listTablePrivileges(identity, databaseName, tableName, principal);
    }

    @Override
    public Optional<Database> getDatabase(ConnectorSecurityContext context, String databaseName)
    {
        SemiTransactionalHiveMetastore metastore = transactionManager.get(context.getTransactionHandle()).getMetastore();
        return metastore.getDatabase(databaseName);
    }
}
