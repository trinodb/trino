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
package io.trino.plugin.hive.metastore;

import com.linkedin.coral.hive.hive2rel.HiveMetastoreClient;
import io.trino.plugin.hive.CoralTableRedirectionResolver;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static java.util.Objects.requireNonNull;

/**
 * Class to adapt Trino's {@link SemiTransactionalHiveMetastore} to Coral's
 * {@link HiveMetastoreClient}. This allows reuse of the hive metastore instantiated by
 * Trino, based on configuration, inside Coral.
 */
public class CoralSemiTransactionalHiveMSCAdapter
        implements HiveMetastoreClient
{
    private final SemiTransactionalHiveMetastore delegate;
    private final CoralTableRedirectionResolver tableRedirection;

    public CoralSemiTransactionalHiveMSCAdapter(
            SemiTransactionalHiveMetastore coralHiveMetastoreClient,
            CoralTableRedirectionResolver tableRedirection)
    {
        this.delegate = requireNonNull(coralHiveMetastoreClient, "coralHiveMetastoreClient is null");
        this.tableRedirection = requireNonNull(tableRedirection, "tableRedirection is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    // returning null for missing entry is as per Coral's requirements
    @Override
    public Database getDatabase(String dbName)
    {
        return delegate.getDatabase(dbName).map(ThriftMetastoreUtil::toMetastoreApiDatabase).orElse(null);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        return delegate.getAllTables(dbName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
    {
        if (!dbName.isEmpty() && !tableName.isEmpty()) {
            Optional<Table> redirected = tableRedirection.redirect(new SchemaTableName(dbName, tableName));
            if (redirected.isPresent()) {
                return redirected.get();
            }
        }

        return delegate.getTable(dbName, tableName)
                .map(value -> ThriftMetastoreUtil.toMetastoreApiTable(value, NO_PRIVILEGES))
                .orElse(null);
    }
}
