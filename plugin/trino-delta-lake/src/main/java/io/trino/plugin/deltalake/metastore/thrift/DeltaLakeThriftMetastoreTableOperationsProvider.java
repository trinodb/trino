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
package io.trino.plugin.deltalake.metastore.thrift;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakeThriftMetastoreTableOperationsProvider
        implements DeltaLakeTableOperationsProvider
{
    private final HiveMetastoreFactory hiveMetastoreFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;

    @Inject
    public DeltaLakeThriftMetastoreTableOperationsProvider(HiveMetastoreFactory hiveMetastoreFactory, ThriftMetastoreFactory thriftMetastoreFactory)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
    }

    @Override
    public DeltaLakeTableOperations createTableOperations(ConnectorSession session)
    {
        Optional<ConnectorIdentity> identity = Optional.of(session.getIdentity());
        return new DeltaLakeThriftMetastoreTableOperations(session, hiveMetastoreFactory.createMetastore(identity), thriftMetastoreFactory.createMetastore(identity));
    }
}
