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
package io.trino.plugin.hive.metastore.thrift;

import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BridgingHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final ThriftMetastore thriftMetastore;

    @Inject
    public BridgingHiveMetastoreFactory(ThriftMetastore thriftMetastore)
    {
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return thriftMetastore.isImpersonationEnabled();
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new BridgingHiveMetastore(thriftMetastore, identity.map(HiveIdentity::new).orElse(HiveIdentity.none()));
    }
}
