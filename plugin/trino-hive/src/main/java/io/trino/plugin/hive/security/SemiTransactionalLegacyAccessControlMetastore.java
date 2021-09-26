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
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSecurityContext;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SemiTransactionalLegacyAccessControlMetastore
        implements LegacyAccessControlMetastore
{
    private final HiveTransactionManager transactionManager;

    @Inject
    public SemiTransactionalLegacyAccessControlMetastore(HiveTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public Optional<Table> getTable(ConnectorSecurityContext context, HiveIdentity identity, String databaseName, String tableName)
    {
        SemiTransactionalHiveMetastore metastore = transactionManager.get(context.getTransactionHandle()).getMetastore();
        return metastore.getTable(new HiveIdentity(context.getIdentity()), databaseName, tableName);
    }
}
