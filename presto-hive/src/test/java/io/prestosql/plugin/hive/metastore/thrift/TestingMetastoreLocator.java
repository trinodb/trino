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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.authentication.NoHiveMetastoreAuthentication;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

public class TestingMetastoreLocator
        implements MetastoreLocator
{
    private final HiveConfig config;
    private final HostAndPort address;

    public TestingMetastoreLocator(HiveConfig config, String host, int port)
    {
        this.config = requireNonNull(config, "config is null");
        this.address = HostAndPort.fromParts(requireNonNull(host, "host is null"), port);
    }

    @Override
    public ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        return new HiveMetastoreClientFactory(config, new NoHiveMetastoreAuthentication()).create(address);
    }
}
