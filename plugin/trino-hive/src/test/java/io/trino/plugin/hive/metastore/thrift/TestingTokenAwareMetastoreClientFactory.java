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

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.thrift.TException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestingTokenAwareMetastoreClientFactory
        implements TokenAwareMetastoreClientFactory
{
    private static final HiveMetastoreAuthentication AUTHENTICATION = new NoHiveMetastoreAuthentication();
    public static final Duration TIMEOUT = new Duration(20, SECONDS);

    private final DefaultThriftMetastoreClientFactory factory;
    private final HostAndPort address;

    public TestingTokenAwareMetastoreClientFactory(Optional<HostAndPort> socksProxy, HostAndPort address)
    {
        this(socksProxy, address, TIMEOUT);
    }

    public TestingTokenAwareMetastoreClientFactory(Optional<HostAndPort> socksProxy, HostAndPort address, Duration timeout)
    {
        this.factory = new DefaultThriftMetastoreClientFactory(Optional.empty(), socksProxy, timeout, AUTHENTICATION, "localhost");
        this.address = requireNonNull(address, "address is null");
    }

    @Override
    public ThriftMetastoreClient createMetastoreClient(Optional<String> delegationToken)
            throws TException
    {
        return factory.create(address, delegationToken);
    }
}
